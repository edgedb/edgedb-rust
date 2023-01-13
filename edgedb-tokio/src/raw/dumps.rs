use std::collections::HashMap;
use std::time::{Instant, Duration};

use bytes::Bytes;
use tokio::time::sleep;
use tokio_stream::{Stream, StreamExt};

use edgedb_errors::{Error, ErrorKind};
use edgedb_errors::{ProtocolOutOfOrderError};
use edgedb_protocol::server_message::ServerMessage;
use edgedb_protocol::client_message::{ClientMessage, Restore, RestoreBlock};

use crate::raw::{Connection, Response};
use crate::raw::connection::{send_messages, wait_message};


impl Connection {
    pub async fn restore(&mut self, header: Bytes,
        mut stream: impl Stream<Item=Result<Bytes, Error>> + Unpin)
        -> Result<Response<()>, Error>
    {
        let guard = self.begin_request()?;
        let start_headers = Instant::now();
        self.send_messages(&[
            ClientMessage::Restore(Restore {
                headers: HashMap::new(),
                jobs: 1,
                data: header,
            }),
        ]).await?;
        loop {
            let msg = self.message().await?;
            match msg {
                ServerMessage::RestoreReady(_) => {
                    log::info!("Schema applied in {:?}",
                               start_headers.elapsed());
                    break;
                }
                ServerMessage::ErrorResponse(err) => {
                    self.send_messages(&[ClientMessage::Sync]).await?;
                    self.expect_ready_or_eos(guard).await
                        .map_err(|e| log::warn!(
                            "Error waiting for Ready after error: {e:#}"))
                        .ok();
                    return Err(Into::<Error>::into(err)
                        .context("error initiating restore protocol")
                        .into());
                }
                _ => {
                    return Err(ProtocolOutOfOrderError::with_message(format!(
                        "unsolicited message {:?}", msg)))?;
                }
            }
        }

        let start_blocks = Instant::now();
        while let Some(data) = stream.next().await.transpose()? {
            let (mut rd, mut wr) = tokio::io::split(&mut self.stream);
            let block = [ClientMessage::RestoreBlock(RestoreBlock { data })];
            tokio::select! {
                msg = wait_message(&mut rd, &mut self.in_buf, &self.proto)
                    => match msg? {
                        ServerMessage::ErrorResponse(err) => {
                            self.send_messages(&[ClientMessage::Sync]).await?;
                            self.expect_ready_or_eos(guard).await
                                .map_err(|e| log::warn!(
                                    "Error waiting for Ready \
                                     after error: {e:#}"))
                                .ok();
                            return Err(Into::<Error>::into(err))?;
                        }
                        msg => {
                            return Err(ProtocolOutOfOrderError::with_message(
                                format!("unsolicited message {:?}", msg)))?;
                        }
                    },
                res = send_messages(&mut wr, &mut self.out_buf,
                                  &self.proto, &block)
                    => res?,
            }
        }
        self.send_messages(&[ClientMessage::RestoreEof]).await?;
        log::info!(target: "edgedb::restore",
            "Blocks sent in {:?}", start_blocks.elapsed());

        let wait = wait_print_loop();
        tokio::pin!(wait);
        loop {
            let msg = tokio::select! {
                _ = &mut wait => unreachable!(),
                msg = self.message() => msg?,
            };
            match msg {
                ServerMessage::StateDataDescription(d) => {
                    self.state_desc = d.typedesc;
                }
                ServerMessage::CommandComplete0(complete) => {
                    log::info!("Complete in {:?}", start_headers.elapsed());
                    self.end_request(guard);
                    return Ok(Response {
                        status_data: complete.status_data,
                        new_state: None,
                        data: (),
                    });
                }
                ServerMessage::CommandComplete1(complete) => {
                    log::info!("Complete in {:?}", start_headers.elapsed());
                    self.end_request(guard);
                    return Ok(Response {
                        status_data: complete.status_data,
                        new_state: Some(complete.state),
                        data: (),
                    });
                }
                ServerMessage::ErrorResponse(err) => {
                    self.send_messages(&[ClientMessage::Sync]).await?;
                    self.expect_ready_or_eos(guard).await
                        .map_err(|e| log::warn!(
                            "Error waiting for Ready \
                             after error: {e:#}"))
                        .ok();
                    return Err(Into::<Error>::into(err))?;
                }
                _ => {
                    return Err(ProtocolOutOfOrderError::with_message(format!(
                        "unsolicited message {:?}", msg)))?;
                }
            }
        }
    }
}

async fn wait_print_loop() {
    // This future should be canceled restore loop finishes
    let start_waiting = Instant::now();
    loop {
        sleep(Duration::from_secs(60)).await;
        log::info!(target: "edgedb::restore",
            "Waiting for complete {:?}", start_waiting.elapsed());
    }
}
