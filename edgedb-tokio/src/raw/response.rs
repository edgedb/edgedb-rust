use std::collections::VecDeque;
use std::mem;

use bytes::Bytes;
use edgedb_errors::ProtocolEncodingError;
use edgedb_errors::{Error, ErrorKind};
use edgedb_errors::{ParameterTypeMismatchError, ProtocolOutOfOrderError};
use edgedb_protocol::QueryResult;
use edgedb_protocol::common::State;
use edgedb_protocol::descriptors::Typedesc;
use edgedb_protocol::server_message::{CommandDataDescription1};
use edgedb_protocol::server_message::{ServerMessage, ErrorResponse};

use crate::raw::{Connection, Response, Description};
use crate::raw::queries::Guard;


enum Buffer {
    Reading(VecDeque<Bytes>),
    Complete { status_data: Bytes, new_state: Option<State> },
    ErrorResponse(ErrorResponse),
    Error(Error),
    Reset,
}

pub struct ResponseStream<'a, T: QueryResult>
    where T::State: Unpin,
{
    connection: &'a mut Connection,
    buffer: Buffer,
    state: Option<T::State>,
    guard: Option<Guard>,
    description: Option<CommandDataDescription1>,
}

impl<'a, T: QueryResult> ResponseStream<'a, T>
    where T::State: Unpin,
{
    pub(crate) async fn new(connection: &'a mut Connection,
               out_desc: &Typedesc,
               guard: Guard)
        -> Result<ResponseStream<'a, T>, Error>
    {
        use Buffer::*;

        let buffer;
        let mut description = None;
        let mut guard = Some(guard);
        loop {
            match connection.message().await? {
                ServerMessage::StateDataDescription(d) => {
                    connection.state_desc = d.typedesc;
                }
                ServerMessage::Data(datum) => {
                    buffer = Reading(datum.data.into());
                    break;
                }
                ServerMessage::CommandComplete1(complete)
                if connection.proto.is_1() => {
                    let guard = guard.take().unwrap();
                    connection.expect_ready(guard).await?;
                    buffer = Complete {
                        status_data: complete.status_data,
                        new_state: complete.state,
                    };
                    break;
                }
                ServerMessage::CommandComplete0(complete)
                if !connection.proto.is_1() => {
                    let guard = guard.take().unwrap();
                    connection.expect_ready(guard).await?;
                    buffer = Complete {
                        status_data: complete.status_data,
                        new_state: None,
                    };
                    break;
                }
                ServerMessage::CommandDataDescription1(desc)
                if connection.proto.is_1() => {
                    description = Some(desc);
                }
                ServerMessage::CommandDataDescription0(desc)
                if !connection.proto.is_1() => {
                    let guard = guard.take().unwrap();
                    connection.expect_ready(guard).await?;
                    let err = ParameterTypeMismatchError::build()
                        .set::<Description>(CommandDataDescription1::from(desc));
                    return Err(err);
                }
                ServerMessage::ErrorResponse(err) => {
                    let guard = guard.take().unwrap();
                    connection.expect_ready_or_eos(guard).await
                        .map_err(|e| log::warn!(
                            "Error waiting for Ready after error: {e:#}"))
                        .ok();
                    let mut err: edgedb_errors::Error = err.into();
                    if let Some(desc) = description.take() {
                        err = err.set::<Description>(desc);
                    }
                    return Err(err);
                }
                msg => {
                    return Err(ProtocolOutOfOrderError::with_message(format!(
                        "Unsolicited message {:?}", msg)))?;
                }
            }
        }
        let computed_desc = description.as_ref().map(|d| d.output())
            .transpose().map_err(ProtocolEncodingError::with_source)?;
        let computed_desc = computed_desc.as_ref().unwrap_or(out_desc);
        if let Some(type_pos) = computed_desc.root_pos() {
            let ctx = computed_desc.as_queryable_context();
            let state = T::prepare(&ctx, type_pos)?;
            Ok(ResponseStream {
                connection,
                buffer,
                state: Some(state),
                guard,
                description,
            })
        } else {
            Ok(ResponseStream {
                connection,
                buffer,
                state: None,
                guard,
                description,
            })
        }
    }
    pub fn can_contain_data(&self) -> bool {
        self.state.is_some()
    }
    async fn expect_ready(&mut self) {
        let guard = self.guard.take().expect("guard is checked before");
        if let Err(e) = self.connection.expect_ready(guard).await {
            self.buffer = Buffer::Error(e);
        }
    }
    async fn ignore_data(&mut self) {
        use Buffer::*;

        loop {
            match self.connection.message().await {
                Ok(ServerMessage::StateDataDescription(d)) => {
                    self.connection.state_desc = d.typedesc;
                }
                Ok(ServerMessage::Data(_)) if self.state.is_some() => {}
                Ok(ServerMessage::CommandComplete1(complete))
                if self.guard.is_some() && self.connection.proto.is_1() => {
                    self.buffer = Complete {
                        status_data: complete.status_data,
                        new_state: complete.state,
                    };
                    self.expect_ready().await;
                    return;
                }
                Ok(ServerMessage::CommandComplete0(complete))
                if self.guard.is_some() && !self.connection.proto.is_1() => {
                    self.buffer = Complete {
                        status_data: complete.status_data,
                        new_state: None,
                    };
                    self.expect_ready().await;
                    return;
                }
                Ok(ServerMessage::ErrorResponse(err))
                if self.guard.is_some() => {
                    let guard = self.guard.take().unwrap();
                    self.connection.expect_ready_or_eos(guard).await
                        .map_err(|e| log::warn!(
                            "Error waiting for Ready after error: {e:#}"))
                        .ok();
                    self.buffer = ErrorResponse(err);
                    return;
                }
                Ok(msg) => {
                    self.buffer = Error(
                        ProtocolOutOfOrderError::with_message(format!(
                        "Unsolicited message {:?}", msg)));
                    return;
                }
                Err(e) => {
                    self.buffer = Error(e);
                    return;
                }
            }
        }
    }
    pub async fn next_element(&mut self) -> Option<T> {
        use Buffer::*;

        let Reading(ref mut buffer) = self.buffer else { return None };
        loop {
            if let Some(element) = buffer.pop_front() {
                let state = self.state.as_mut()
                    .expect("data packets are ignored if state is None");
                match T::decode(state, &element) {
                    Ok(value) => return Some(value),
                    Err(e) => {
                        self.ignore_data().await;
                        self.buffer = Error(e);
                        return None;
                    }
                }
            }
            match self.connection.message().await {
                Ok(ServerMessage::StateDataDescription(d)) => {
                    self.connection.state_desc = d.typedesc;
                }
                Ok(ServerMessage::Data(datum)) if self.state.is_some() => {
                    buffer.extend(datum.data);
                }
                Ok(ServerMessage::CommandComplete1(complete))
                if self.guard.is_some() && self.connection.proto.is_1() => {
                    self.expect_ready().await;
                    self.buffer = Complete {
                        status_data: complete.status_data,
                        new_state: complete.state,
                    };
                    return None;
                }
                Ok(ServerMessage::CommandComplete0(complete))
                if self.guard.is_some() && !self.connection.proto.is_1() => {
                    self.expect_ready().await;
                    self.buffer = Complete {
                        status_data: complete.status_data,
                        new_state: None,
                    };
                    return None;
                }
                Ok(ServerMessage::ErrorResponse(err))
                if self.guard.is_some() => {
                    let guard = self.guard.take().unwrap();
                    self.connection.expect_ready_or_eos(guard).await
                        .map_err(|e| log::warn!(
                            "Error waiting for Ready after error: {e:#}"))
                        .ok();
                    self.buffer = ErrorResponse(err.into());
                    return None;
                }
                Ok(msg) => {
                    self.buffer = Error(
                        ProtocolOutOfOrderError::with_message(format!(
                        "Unsolicited message {:?}", msg)));
                    return None;
                }
                Err(e) => {
                    self.buffer = Error(e);
                    return None;
                }
            }
        }
    }
    pub async fn complete(mut self) -> Result<Response<()>, Error> {
        self.process_complete().await
    }
    pub async fn process_complete(&mut self) -> Result<Response<()>, Error> {
        use Buffer::*;
        while matches!(self.buffer, Reading(_)) {
            self.ignore_data().await
        }

        match mem::replace(&mut self.buffer, Buffer::Reset) {
            Reading(_) => unreachable!(),
            Complete { status_data, new_state }
            => Ok(Response { status_data, new_state, data: () }),
            Error(e) => Err(e),
            ErrorResponse(e) => {
                let mut err: edgedb_errors::Error = e.into();
                if let Some(desc) = self.description.take() {
                    err = err.set::<Description>(desc);
                }
                Err(err.into())
            }
            Reset => panic!("process_complete() called twice"),
        }
    }
}
