use std::collections::HashMap;

use bytes::Bytes;

use edgedb_protocol::common::CompilationFlags;
use edgedb_protocol::features::ProtocolVersion;
use edgedb_protocol::client_message::{ClientMessage, Prepare, Execute};
use edgedb_protocol::client_message::{DescribeStatement, DescribeAspect};
use edgedb_protocol::server_message::{ServerMessage, Data};
use edgedb_protocol::server_message::{PrepareComplete, CommandDataDescription};

use crate::errors::{Error, ClientError, ErrorKind};
use crate::errors::{ProtocolOutOfOrderError};
use crate::raw::{ConnInner, Connection};

impl ConnInner {
    async fn expect_ready(&mut self) -> Result<(), Error> {
        loop {
            let msg = self.message().await?;
            match msg {
                ServerMessage::ReadyForCommand(_) => {
                    // TODO(tailhook) update transaction state
                    return Ok(())
                }
                // TODO(tailhook) should we react on messages somehow?
                //                At list parse LogMessage's?
                _ => {},
            }
        }
    }
    pub async fn prepare(&mut self, flags: &CompilationFlags, query: &str)
        -> Result<PrepareComplete, Error>
    {
        // TODO(tailhook) modify state
        self.send_messages(&[
            ClientMessage::Prepare(Prepare::new(flags, query)),
            ClientMessage::Sync,
        ]).await?;

        loop {
            let msg = self.message().await?;
            match msg {
                ServerMessage::PrepareComplete(data) => {
                    self.expect_ready().await?;
                    return Ok(data);
                }
                ServerMessage::ErrorResponse(err) => {
                    self.expect_ready().await
                        .map_err(|e| log::warn!(
                            "Error waiting for Ready after error: {e:#}"))
                        .ok();
                    return Err(err.into());
                }
                _ => {
                    return Err(ProtocolOutOfOrderError::with_message(format!(
                        "Unsolicited message {:?}", msg)));
                }
            }
        }
    }
    pub async fn describe_data(&mut self)
        -> Result<CommandDataDescription, Error>
    {
        // TODO(tailhook) modify state
        self.send_messages(&[
            ClientMessage::DescribeStatement(DescribeStatement {
                headers: HashMap::new(),
                aspect: DescribeAspect::DataDescription,
                statement_name: Bytes::from(""),
            }),
            ClientMessage::Sync,
        ]).await?;

        loop {
            let msg = self.message().await?;
            match msg {
                ServerMessage::CommandDataDescription(data_desc) => {
                    self.expect_ready().await?;
                    return Ok(data_desc);
                }
                ServerMessage::ErrorResponse(err) => {
                    self.expect_ready().await
                        .map_err(|e| log::warn!(
                            "Error waiting for Ready after error: {e:#}"))
                        .ok();
                    return Err(err.into());
                }
                _ => {
                    return Err(ProtocolOutOfOrderError::with_message(format!(
                        "Unsolicited message {:?}", msg)));
                }
            }
        }
    }
    pub async fn execute(&mut self, arguments: &Bytes)
        -> Result<Vec<Data>, Error>
    {
        // TODO(tailhook) modify state
        self.send_messages(&[
            ClientMessage::Execute(Execute {
                headers: HashMap::new(),
                statement_name: Bytes::from(""),
                arguments: arguments.clone(),
            }),
            ClientMessage::Sync,
        ]).await?;

        let mut result = Vec::new();
        loop {
            let msg = self.message().await?;
            match msg {
                ServerMessage::Data(data) => {
                    result.push(data);
                }
                ServerMessage::CommandComplete(_) => {
                    self.expect_ready().await?;
                    return Ok(result);
                }
                ServerMessage::ErrorResponse(err) => {
                    self.expect_ready().await
                        .map_err(|e| log::warn!(
                            "Error waiting for Ready after error: {e:#}"))
                        .ok();
                    return Err(err.into());
                }
                _ => {
                    return Err(ProtocolOutOfOrderError::with_message(format!(
                        "Unsolicited message {:?}", msg)));
                }
            }
        }
    }
}

impl Connection {
    pub async fn prepare(&mut self, flags: &CompilationFlags, query: &str)
        -> Result<PrepareComplete, Error>
    {
        self.inner.as_mut().expect("connection is not dropped")
            .prepare(flags, query).await
    }

    pub async fn describe_data(&mut self)
        -> Result<CommandDataDescription, Error>
    {
        self.inner.as_mut().expect("connection is not dropped")
            .describe_data().await
    }

    pub async fn execute(&mut self, arguments: &Bytes)
        -> Result<Vec<Data>, Error>
    {
        self.inner.as_mut().expect("connection is not dropped")
            .execute(arguments).await
    }
    pub fn proto(&self) -> &ProtocolVersion {
        &self.inner.as_ref().expect("connection is not dropped").proto
    }
}
