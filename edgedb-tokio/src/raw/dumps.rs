use std::collections::HashMap;
use std::time::{Instant, Duration};
use std::mem;

use bytes::Bytes;
use tokio::time::sleep;
use tokio_stream::{Stream, StreamExt};

use edgedb_errors::{Error, ErrorKind};
use edgedb_errors::ProtocolOutOfOrderError;
use edgedb_protocol::server_message::{ServerMessage, RawPacket};
use edgedb_protocol::client_message::{ClientMessage, Restore, RestoreBlock};
use edgedb_protocol::client_message::Dump;

use crate::raw::{Connection, Response};
use crate::raw::connection::{send_messages, wait_message};
use crate::raw::queries::Guard;

enum DumpState {
    Header(RawPacket),
    Blocks,
    Complete(Response<()>),
    Error(Error),
    Reset,
}

pub struct DumpStream<'a> {
    conn: &'a mut Connection,
    state: DumpState,
    guard: Option<Guard>,
}


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
        let mut num_blocks = 0;
        let mut total_len = 0;
        while let Some(data) = stream.next().await.transpose()? {
            num_blocks += 1;
            total_len += data.len();
            log::info!(target: "edgedb::restore", "Block {num_blocks} processed: {:.02} MB restored", total_len as f64 / 1048576.0);
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
            "Database restored in {:?}", start_blocks.elapsed());

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
                        new_state: complete.state,
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
    pub async fn dump(&mut self) -> Result<DumpStream<'_>, Error> {
        self.dump_with_secrets(false).await
    }
    pub async fn dump_with_secrets(&mut self, with_secrets: bool)
                       -> Result<DumpStream<'_>, Error> {
        let guard = self.begin_request()?;

        let mut headers = HashMap::new();
        if with_secrets {
            headers.insert(0xFF10, Bytes::from(vec!(with_secrets as u8)));
        }

        self.send_messages(&[
            ClientMessage::Dump(Dump {
                headers: headers,
            }),
            ClientMessage::Sync,
        ]).await?;
        let msg = self.message().await?;
        let header = match msg {
            ServerMessage::DumpHeader(packet) => packet,
            ServerMessage::ErrorResponse(err) => {
                self.expect_ready_or_eos(guard).await
                    .map_err(|e| log::warn!(
                        "Error waiting for Ready after error: {e:#}"))
                    .ok();
                return Err(Into::<Error>::into(err)
                    .context("error receiving dump header")
                    .into());
            }
            _ => {
                return Err(ProtocolOutOfOrderError::with_message(format!(
                    "unsolicited message {:?}", msg)))?;
            }
        };
        Ok(DumpStream {
            conn: self,
            state: DumpState::Header(header),
            guard: Some(guard),
        })
    }
}

impl DumpStream<'_> {
    pub async fn complete(mut self) -> Result<Response<()>, Error> {
        self.process_complete().await
    }
    pub fn take_header(&mut self) -> Option<RawPacket> {
        match mem::replace(&mut self.state, DumpState::Reset) {
            DumpState::Header(header) => {
                self.state = DumpState::Blocks;
                Some(header)
            }
            state => {
                self.state = state;
                None
            }
        }
    }
    pub async fn next_block(&mut self) -> Option<RawPacket> {
        match &self.state {
            DumpState::Header(_) |
            DumpState::Blocks => {
                match self.conn.message().await {
                    Ok(ServerMessage::DumpBlock(packet)) => {
                        Some(packet)
                    }
                    Ok(ServerMessage::CommandComplete0(complete))
                    if self.guard.is_some() && !self.conn.proto.is_1() => {
                        let guard = self.guard.take().unwrap();
                        if let Err(e) = self.conn.expect_ready(guard).await {
                            self.state = DumpState::Error(e)
                        } else {
                            self.state = DumpState::Complete(Response {
                                status_data: complete.status_data,
                                new_state: None,
                                data: (),
                            });
                        }
                        None
                    }
                    Ok(ServerMessage::CommandComplete1(complete))
                    if self.guard.is_some() && self.conn.proto.is_1() => {
                        let guard = self.guard.take().unwrap();
                        if let Err(e) = self.conn.expect_ready(guard).await {
                            self.state = DumpState::Error(e)
                        } else {
                            self.state = DumpState::Complete(Response {
                                status_data: complete.status_data,
                                new_state: complete.state,
                                data: (),
                            });
                        }
                        None
                    }
                    Ok(ServerMessage::ErrorResponse(err)) => {
                        let guard = self.guard.take().unwrap();
                        self.conn.expect_ready_or_eos(guard).await
                            .map_err(|e| log::warn!(
                                "Error waiting for Ready after error: {e:#}"))
                            .ok();
                        self.state = DumpState::Error(err.into());
                        None
                    }
                    Ok(msg) => {
                        self.state = DumpState::Error(
                            ProtocolOutOfOrderError::with_message(format!(
                            "unsolicited message {:?}", msg))
                        );
                        None
                    }
                    Err(e) => {
                        self.state = DumpState::Error(e);
                        None
                    }
                }
            }
            _ => None,
        }
    }
    pub async fn process_complete(&mut self) -> Result<Response<()>, Error> {
        use DumpState::*;

        match mem::replace(&mut self.state, Reset) {
            Header(..) | Blocks
                => panic!("process_complete() called too early"),
            Complete(c) => Ok(c),
            Error(e) => Err(e),

            Reset => panic!("process_complete() called twice"),
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
