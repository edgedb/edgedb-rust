use std::collections::HashMap;
use std::time::Instant;

use bytes::Bytes;
use edgedb_protocol::model::Uuid;

use edgedb_protocol::client_message::{ClientMessage, Parse, Prepare};
use edgedb_protocol::client_message::{Execute0, Execute1};
use edgedb_protocol::client_message::{DescribeStatement, DescribeAspect};
use edgedb_protocol::client_message::{OptimisticExecute};
use edgedb_protocol::common::{CompilationOptions, CompilationFlags};
use edgedb_protocol::common::{IoFormat, Cardinality, Capabilities};
use edgedb_protocol::features::ProtocolVersion;
use edgedb_protocol::server_message::{PrepareComplete, CommandDataDescription1};
use edgedb_protocol::server_message::{ServerMessage, Data};

use crate::errors::{Error, ErrorKind};
use crate::errors::{ProtocolOutOfOrderError, ClientInconsistentError};
use crate::raw::{ConnInner, Connection};
use crate::raw::connection::Mode;

pub(crate) struct Guard;

impl ConnInner {
    fn begin_request(&mut self) -> Result<Guard, Error> {
        match self.mode {
            Mode::Normal { .. } => {
                self.mode = Mode::Dirty;
                Ok(Guard)
            }
            Mode::Transaction { dirty: ref mut dirty@false } => {
                *dirty = true;
                Ok(Guard)
            }
            Mode::Transaction { dirty: true }
            | Mode::Dirty => Err(ClientInconsistentError::build()),
            // TODO(tailhook) technically we could just wait ping here
            Mode::AwaitingPing => Err(ClientInconsistentError
                                       ::with_message("interrupted ping")),
        }
    }
    async fn expect_ready(&mut self, guard: Guard) -> Result<(), Error> {
        use edgedb_protocol::server_message::TransactionState::*;
        loop {
            let msg = self.message().await?;
            match msg {
                ServerMessage::ReadyForCommand(ready) => {
                    drop(guard);
                    match ready.transaction_state {
                        NotInTransaction => {
                            self.mode = Mode::Normal {
                                idle_since: Instant::now()
                            };
                        },
                        InTransaction | InFailedTransaction => {
                            self.mode = Mode::Transaction { dirty: false };
                        }
                    }
                    // TODO(tailhook) update transaction state
                    return Ok(())
                }
                // TODO(tailhook) should we react on messages somehow?
                //                At list parse LogMessage's?
                _ => {},
            }
        }
    }
    pub async fn parse(&mut self, flags: &CompilationOptions, query: &str)
        -> Result<CommandDataDescription1, Error>
    {
        if self.proto.is_1() {
            self._parse1(flags, query).await
        } else {
            let pre = self._prepare0(flags, query).await?;
            self._describe0(pre).await
        }
    }
    async fn _parse1(&mut self, flags: &CompilationOptions, query: &str)
        -> Result<CommandDataDescription1, Error>
    {
        let guard = self.begin_request()?;
        self.send_messages(&[
            ClientMessage::Parse(Parse::new(flags, query)),
            ClientMessage::Sync,
        ]).await?;

        loop {
            let msg = self.message().await?;
            match msg {
                ServerMessage::StateDataDescription(..) => {}
                ServerMessage::CommandDataDescription1(data_desc) => {
                    self.expect_ready(guard).await?;
                    return Ok(data_desc);
                }
                ServerMessage::ErrorResponse(err) => {
                    self.expect_ready(guard).await
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
    async fn _prepare0(&mut self, flags: &CompilationOptions, query: &str)
        -> Result<PrepareComplete, Error>
    {
        let guard = self.begin_request()?;
        self.send_messages(&[
            ClientMessage::Prepare(Prepare::new(flags, query)),
            ClientMessage::Sync,
        ]).await?;

        loop {
            let msg = self.message().await?;
            match msg {
                ServerMessage::PrepareComplete(data) => {
                    self.expect_ready(guard).await?;
                    return Ok(data);
                }
                ServerMessage::ErrorResponse(err) => {
                    self.expect_ready(guard).await
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
    async fn _describe0(&mut self, prepare: PrepareComplete)
        -> Result<CommandDataDescription1, Error>
    {
        let guard = self.begin_request()?;
        self.send_messages(&[
            ClientMessage::DescribeStatement(DescribeStatement {
                headers: HashMap::new(),
                aspect: DescribeAspect::DataDescription,
                statement_name: Bytes::from(""),
            }),
            ClientMessage::Sync,
        ]).await?;

        let desc = loop {
            let msg = self.message().await?;
            match msg {
                ServerMessage::CommandDataDescription0(data_desc) => {
                    self.expect_ready(guard).await?;
                    break data_desc;
                }
                ServerMessage::ErrorResponse(err) => {
                    self.expect_ready(guard).await
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
        };
        Ok(CommandDataDescription1 {
            proto: desc.proto,
            annotations: HashMap::new(),
            capabilities: prepare.get_capabilities()
                .unwrap_or(Capabilities::ALL),
            result_cardinality: prepare.cardinality,
            input_typedesc_id: desc.input_typedesc_id,
            input_typedesc: desc.input_typedesc,
            output_typedesc_id: desc.output_typedesc_id,
            output_typedesc: desc.output_typedesc,
        })
    }
    pub async fn execute(&mut self, opts: &CompilationOptions, query: &str,
                         desc: &CommandDataDescription1, arguments: &Bytes)
        -> Result<Vec<Data>, Error>
    {
        if self.proto.is_1() {
            self._execute1(opts, query, desc, arguments).await
        } else {
            self._execute0(arguments).await
        }
    }

    async fn _execute1(&mut self, opts: &CompilationOptions, query: &str,
                       desc: &CommandDataDescription1, arguments: &Bytes)
        -> Result<Vec<Data>, Error>
    {
        let guard = self.begin_request()?;
        let mut cflags = CompilationFlags::empty();
        if opts.implicit_typenames {
            cflags |= CompilationFlags::INJECT_OUTPUT_TYPE_NAMES;
        }
        if opts.implicit_typeids {
            cflags |= CompilationFlags::INJECT_OUTPUT_TYPE_IDS;
        }
        self.send_messages(&[
            ClientMessage::Execute1(Execute1 {
                annotations: HashMap::new(),
                allowed_capabilities: opts.allow_capabilities,
                compilation_flags: cflags,
                implicit_limit: opts.implicit_limit,
                output_format: opts.io_format,
                expected_cardinality: opts.expected_cardinality,
                command_text: query.into(),
                state_typedesc_id: Uuid::from_u128(0),  // TODO(tailhook)
                state_data: Bytes::new(),
                input_typedesc_id: desc.input_typedesc_id,
                output_typedesc_id: desc.output_typedesc_id,
                arguments: arguments.clone(),
            }),
            ClientMessage::Sync,
        ]).await?;

        let mut result = Vec::new();
        loop {
            let msg = self.message().await?;
            match msg {
                ServerMessage::StateDataDescription(..) => {}
                ServerMessage::Data(data) => {
                    result.push(data);
                }
                ServerMessage::CommandComplete1(_) => {
                    self.expect_ready(guard).await?;
                    return Ok(result);
                }
                ServerMessage::ErrorResponse(err) => {
                    self.expect_ready(guard).await
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

    async fn _execute0(&mut self, arguments: &Bytes)
        -> Result<Vec<Data>, Error>
    {
        let guard = self.begin_request()?;
        self.send_messages(&[
            ClientMessage::Execute0(Execute0 {
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
                ServerMessage::CommandComplete0(_) => {
                    self.expect_ready(guard).await?;
                    return Ok(result);
                }
                ServerMessage::ErrorResponse(err) => {
                    self.expect_ready(guard).await
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
    pub async fn statement(&mut self, flags: &CompilationOptions, query: &str)
        -> Result<(), Error>
    {
        if self.proto.is_1() {
            self._statement1(flags, query).await
        } else {
            self._statement0(flags, query).await
        }
    }

    async fn _statement1(&mut self, opts: &CompilationOptions, query: &str)
        -> Result<(), Error>
    {
        let guard = self.begin_request()?;
        let mut cflags = CompilationFlags::empty();
        if opts.implicit_typenames {
            cflags |= CompilationFlags::INJECT_OUTPUT_TYPE_NAMES;
        }
        if opts.implicit_typeids {
            cflags |= CompilationFlags::INJECT_OUTPUT_TYPE_IDS;
        }
        self.send_messages(&[
            ClientMessage::Execute1(Execute1 {
                annotations: HashMap::new(),
                allowed_capabilities: opts.allow_capabilities,
                compilation_flags: cflags,
                implicit_limit: opts.implicit_limit,
                output_format: opts.io_format,
                expected_cardinality: opts.expected_cardinality,
                command_text: query.into(),
                state_typedesc_id: Uuid::from_u128(0),  // TODO(tailhook)
                state_data: Bytes::new(),
                input_typedesc_id: Uuid::from_u128(0),
                output_typedesc_id: Uuid::from_u128(0),
                arguments: Bytes::new(),
            }),
            ClientMessage::Sync,
        ]).await?;

        let mut result = Vec::new();
        loop {
            let msg = self.message().await?;
            match msg {
                ServerMessage::StateDataDescription(..) => {}
                ServerMessage::Data(data) => {
                    result.push(data);
                }
                ServerMessage::CommandComplete1(..) => {
                    self.expect_ready(guard).await?;
                    return Ok(());
                }
                ServerMessage::ErrorResponse(err) => {
                    self.expect_ready(guard).await
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
    async fn _statement0(&mut self, flags: &CompilationOptions, query: &str)
        -> Result<(), Error>
    {
        let guard = self.begin_request()?;
        self.send_messages(&[
            ClientMessage::OptimisticExecute(OptimisticExecute::new(
                flags,
                query, Bytes::new(),
                Uuid::from_u128(0x0), Uuid::from_u128(0x0),
            )),
            ClientMessage::Sync,
        ]).await?;

        let mut result = Vec::new();
        loop {
            let msg = self.message().await?;
            match msg {
                ServerMessage::Data(data) => {
                    result.push(data);
                }
                ServerMessage::CommandComplete0(_) => {
                    self.expect_ready(guard).await?;
                    return Ok(());
                }
                ServerMessage::ErrorResponse(err) => {
                    self.expect_ready(guard).await
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
    pub async fn parse(&mut self, flags: &CompilationOptions, query: &str)
        -> Result<CommandDataDescription1, Error>
    {
        self.inner.as_mut().expect("connection is not dropped")
            .parse(flags, query).await
    }
    pub async fn execute(&mut self, opts: &CompilationOptions, query: &str,
                         desc: &CommandDataDescription1, arguments: &Bytes)
        -> Result<Vec<Data>, Error>
    {
        self.inner.as_mut().expect("connection is not dropped")
            .execute(opts, query, desc, arguments).await
    }
    pub async fn statement(&mut self, query: &str) -> Result<(), Error> {
        let flags = CompilationOptions {
            implicit_limit: None,
            implicit_typenames: false,
            implicit_typeids: false,
            explicit_objectids: false,
            allow_capabilities: Capabilities::ALL,
            io_format: IoFormat::Binary,
            expected_cardinality: Cardinality::Many, // no result is unsupported
        };
        self.inner.as_mut().expect("connection is not dropped")
            .statement(&flags, query).await
    }
    pub fn proto(&self) -> &ProtocolVersion {
        &self.inner.as_ref().expect("connection is not dropped").proto
    }
}
