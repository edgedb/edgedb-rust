#![cfg_attr(not(feature="unstable"), allow(dead_code, unused_imports))]

use core::future::Future;
use std::collections::HashMap;
use std::fmt;
use std::str;
use std::time::{Duration, Instant};

use async_std::future::{timeout, pending};
use async_std::io::prelude::WriteExt;
use async_std::prelude::FutureExt;
use async_std::prelude::StreamExt;
use bytes::{Bytes, BytesMut};
use futures_util::io::{ReadHalf, WriteHalf};
use tls_api::TlsStream;
use typemap::TypeMap;

use edgedb_protocol::QueryResult;
use edgedb_protocol::client_message::ClientMessage;
use edgedb_protocol::client_message::{Capabilities, CompilationFlags, State};
use edgedb_protocol::client_message::{DescribeStatement, DescribeAspect};
use edgedb_protocol::client_message::{Execute0, Execute1, ExecuteScript};
use edgedb_protocol::client_message::{Prepare, Parse, IoFormat, Cardinality};
use edgedb_protocol::server_message::{RawTypedesc};
use edgedb_protocol::descriptors::Typedesc;
use edgedb_protocol::encoding::{Input, Output};
use edgedb_protocol::features::ProtocolVersion;
use edgedb_protocol::model::Uuid;
use edgedb_protocol::query_arg::{QueryArgs, Encoder};
use edgedb_protocol::queryable::{Queryable};
use edgedb_protocol::server_message::{ServerMessage, TransactionState};
use edgedb_protocol::server_message::{StateDataDescription, CommandComplete1};
use edgedb_protocol::value::Value;

use crate::debug::PartialDebug;
use crate::errors::{ClientConnectionError, ProtocolError};
use crate::errors::{ClientConnectionTimeoutError, ClientConnectionEosError};
use crate::errors::{ClientInconsistentError, ClientEncodingError};
use crate::errors::{Error, ErrorKind, ResultExt};
use crate::errors::{NoResultExpected, NoDataError};
use crate::errors::{ProtocolOutOfOrderError, ProtocolEncodingError};
use crate::reader::{self, QueryResponse, Reader};
use crate::server_params::{ServerParam, SystemConfig};


#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub(crate) enum Mode {
    Normal {
        idle_since: Instant,
    },
    Dirty,
    AwaitingPing,
}


#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub(crate) enum PingInterval {
    Unknown,
    Disabled,
    Interval(Duration),
}

#[derive(Debug)]
/// A single connection to the EdgeDB server.
pub struct Connection {
    pub(crate) ping_interval: PingInterval,
    pub(crate) input: ReadHalf<TlsStream>,
    pub(crate) output: WriteHalf<TlsStream>,
    pub(crate) input_buf: BytesMut,
    pub(crate) output_buf: BytesMut,
    pub(crate) version: ProtocolVersion,
    pub(crate) params: TypeMap<dyn typemap::DebugAny + Send + Sync>,
    pub(crate) transaction_state: TransactionState,
    pub(crate) mode: Mode,
    pub(crate) state_desc: RawTypedesc,
    pub(crate) state: State,
}

pub(crate) struct PartialState<'a> {
    pub(crate) mode: &'a mut Mode,
    pub(crate) state_desc: &'a mut RawTypedesc,
    pub(crate) state: &'a mut State,
}

pub struct Sequence<'a> {
    pub writer: Writer<'a>,
    pub reader: Reader<'a>,
    pub(crate) active: bool,
    pub(crate) state: PartialState<'a>,
}

pub struct Writer<'a> {
    stream: &'a mut WriteHalf<TlsStream>,
    proto: &'a ProtocolVersion,
    outbuf: &'a mut BytesMut,
}

#[derive(Debug, Clone)]
pub struct StatementParams {
    pub io_format: IoFormat,
    pub cardinality: Cardinality,
}

impl StatementParams {
    pub fn new() -> StatementParams {
        StatementParams {
            io_format: IoFormat::Binary,
            cardinality: Cardinality::Many,
        }
    }
    pub fn io_format(&mut self, fmt: IoFormat) -> &mut Self {
        self.io_format = fmt;
        self
    }
    pub fn cardinality(&mut self, card: Cardinality) -> &mut Self {
        self.cardinality = card;
        self
    }
}


impl<'a> Sequence<'a> {

    pub fn response<T: QueryResult>(self, state: T::State)
        -> QueryResponse<'a, T>
    {
        assert!(self.active);  // TODO(tailhook) maybe debug_assert
        reader::QueryResponse {
            seq: self,
            buffer: Vec::new(),
            error: None,
            complete: false,
            state,
        }
    }

    pub async fn response_blobs(mut self) -> Result<(Vec<Bytes>, Bytes), Error>
    {
        assert!(self.active);  // TODO(tailhook) maybe debug_assert
        let mut data = Vec::new();
        let complete = loop {
            match self.reader.message().await? {
                ServerMessage::Data(m) => data.extend(m.data),
                ServerMessage::CommandComplete0(m) => break Ok(m.status_data),
                ServerMessage::CommandComplete1(d) => {
                    self.process_complete(&d)?;
                    break Ok(d.status_data);
                }
                ServerMessage::StateDataDescription(d) => {
                    self.set_state_description(d)?;
                }
                ServerMessage::ErrorResponse(e) => break Err(e),
                msg => {
                    return Err(ProtocolOutOfOrderError::with_message(format!(
                        "unsolicited packet: {}", PartialDebug(msg))));
                }
            }
        };
        match self.reader.message().await? {
            ServerMessage::ReadyForCommand(r) => {
                self.reader.consume_ready(r);
                self.end_clean();
                return complete.map(|m| (data, m)).map_err(|e| e.into());
            }
            msg => {
                return Err(ProtocolOutOfOrderError::with_message(format!(
                    "unsolicited packet: {}", PartialDebug(msg))));
            }
        }
    }

    pub fn process_complete(&mut self, cmp: &CommandComplete1)
        -> Result<(), Error>
    {
        if cmp.state.data.len() != 0 {
            *self.state.state = cmp.state.clone();
        }
        Ok(())
    }

    pub fn end_clean(&mut self) {
        self.active = false;
        *self.state.mode = Mode::Normal {
            idle_since: Instant::now(),
        };
    }

    pub fn set_state_description(&mut self, descr: StateDataDescription)
        -> Result<(), Error>
    {
        *self.state.state_desc = descr.typedesc;
        Ok(())
    }

    pub fn get_state(&self) -> &State {
        &self.state.state
    }
}

impl Connection {
    #[cfg(feature="unstable")]
    /// Set state of this connection
    pub fn set_state(&mut self, state: State) {
        self.state = state;
    }
    #[cfg(feature="unstable")]
    /// Get state of this connection
    pub fn get_state(&self) -> State {
        self.state.clone()
    }
    #[cfg(feature="unstable")]
    /// Get state descriptor of this connection
    pub fn get_state_desc(&self) -> RawTypedesc {
        self.state_desc.clone()
    }
    pub fn protocol(&self) -> &ProtocolVersion {
        return &self.version
    }
    pub async fn passive_wait<T>(&mut self) -> T {
        let (_, mut reader, _) = self.split();
        reader.passive_wait().await.ok();
        // any erroneous or successful read (even 0) means need reconnect
        self.mode = Mode::Dirty;
        pending::<()>().await;
        unreachable!();
    }
    async fn do_pings(&mut self, interval: Duration) -> Result<(), Error> {
        use async_std::io;

        let (mut writer, mut reader, state) = self.split();

        if *state.mode == Mode::AwaitingPing {
            Self::synchronize_ping(&mut reader, state.mode).await?;
        }

        while let Mode::Normal { idle_since: last_pong } = *state.mode {
            match io::timeout(
                interval.saturating_sub(Instant::now() - last_pong),
                reader.passive_wait()
            ).await {
                Err(e) if e.kind() == io::ErrorKind::TimedOut => (),
                Err(e) => {
                    *state.mode = Mode::Dirty;
                    return Err(ClientConnectionError::with_source(e))?;
                }
                Ok(_) => unreachable!(),
            }

            *state.mode = Mode::Dirty;
            writer.send_messages(&[ClientMessage::Sync]).await?;
            *state.mode = Mode::AwaitingPing;
            Self::synchronize_ping(&mut reader, state.mode).await?;
        }
        Ok(())
    }
    async fn background_pings<T>(&mut self, interval: Duration) -> T {
        self.do_pings(interval).await
            .map_err(|e| {
                log::info!("Connection error during background pings: {}", e)
            })
            .ok();
        debug_assert_eq!(self.mode, Mode::Dirty);
        pending::<()>().await;
        unreachable!();
    }
    async fn synchronize_ping<'a>(
        reader: &mut Reader<'a>, mode: &mut Mode
    ) -> Result<(), Error> {
        debug_assert_eq!(*mode, Mode::AwaitingPing);
        if let Err(e) = reader.wait_ready().await {
            *mode = Mode::Dirty;
            Err(e)
        } else {
            *mode = Mode::Normal { idle_since: Instant::now() };
            Ok(())
        }
    }
    fn calc_ping_interval(&self) -> PingInterval {
        if let Some(config) = self.params.get::<SystemConfig>() {
            if let Some(timeout) = config.session_idle_timeout {
                if timeout.is_zero() {
                    log::info!(
                        "Server disabled session_idle_timeout; \
                         pings are disabled."
                    );
                    PingInterval::Disabled
                } else {
                    let interval = Duration::from_secs(
                        (
                            timeout.saturating_sub(
                                Duration::from_secs(1)
                            ).as_secs_f64() * 0.9
                        ).ceil() as u64
                    );
                    if interval.is_zero() {
                        log::warn!(
                            "session_idle_timeout={:?} is too short; \
                             pings are disabled.",
                            timeout,
                        );
                        PingInterval::Disabled
                    } else {
                        log::info!(
                            "Setting ping interval to {:?} as \
                             session_idle_timeout={:?}",
                            interval, timeout,
                        );
                        PingInterval::Interval(interval)
                    }
                }
            } else {
                PingInterval::Unknown
            }
        } else {
            PingInterval::Unknown
        }
    }
    #[cfg(feature="unstable")]
    pub async fn ping_while<T, F>(&mut self, other: F) -> T
        where F: Future<Output = T>
    {
        if self.ping_interval == PingInterval::Unknown {
            self.ping_interval = self.calc_ping_interval();
        }
        if let PingInterval::Interval(interval) = self.ping_interval {
            let rv = other.race(self.background_pings(interval)).await;
            if self.mode == Mode::AwaitingPing {
                let (_, ref mut reader, state) = self.split();
                Self::synchronize_ping(reader, state.mode).await.ok();
            }
            rv
        } else {
            other.await
        }
    }
    pub fn is_consistent(&self) -> bool {
        matches!(self.mode, Mode::Normal {
            idle_since: _,
        })
    }
    pub async fn terminate(mut self) -> Result<(), Error> {
        let mut seq = self.start_sequence().await?;
        seq.send_messages(&[ClientMessage::Terminate]).await?;
        match seq.message().await {
            Err(e) if e.is::<ClientConnectionEosError>() => Ok(()),
            Err(e) => Err(e),
            Ok(msg) => Err(ProtocolError::with_message(format!(
                "unsolicited message {:?}", msg))),
        }
    }
    pub async fn start_sequence<'x>(&'x mut self)
        -> Result<Sequence<'x>, Error>
    {
        let (writer, reader, state) = self.split();
        if !matches!(*state.mode, Mode::Normal {
            idle_since: _,
        }) {
            return Err(ClientInconsistentError::with_message(
                "Connection is inconsistent state. Please reconnect."));
        }
        *state.mode = Mode::Dirty;
        Ok(Sequence {
            writer,
            reader,
            state,
            active: true,
        })
    }

    pub fn get_param<T: ServerParam>(&self)
        -> Option<&<T as typemap::Key>::Value>
        where <T as typemap::Key>::Value: fmt::Debug + Send + Sync
    {
        self.params.get::<T>()
    }
    pub fn transaction_state(&self) -> TransactionState {
        self.transaction_state
    }
    fn split(&mut self) -> (Writer, Reader, PartialState<'_>) {
        let reader = Reader {
            proto: &self.version,
            buf: &mut self.input_buf,
            stream: &mut self.input,
            transaction_state: &mut self.transaction_state,
        };
        let writer = Writer {
            proto: &self.version,
            outbuf: &mut self.output_buf,
            stream: &mut self.output,
        };
        let state = PartialState {
            mode: &mut self.mode,
            state_desc: &mut self.state_desc,
            state: &mut self.state,
        };
        (writer, reader, state)
    }
}

impl<'a> Writer<'a> {

    pub async fn send_messages<'x, I>(&mut self, msgs: I) -> Result<(), Error>
        where I: IntoIterator<Item=&'x ClientMessage>
    {
        self.outbuf.truncate(0);
        for msg in msgs {
            msg.encode(&mut Output::new(
                &self.proto,
                self.outbuf,
            )).map_err(ClientEncodingError::with_source)?;
        }
        self.stream.write_all(&self.outbuf[..]).await
            .map_err(ClientConnectionError::with_source)?;
        Ok(())
    }

}


impl<'a> Sequence<'a> {
    pub async fn send_messages<'x, I>(&mut self, msgs: I)
        -> Result<(), Error>
        where I: IntoIterator<Item=&'x ClientMessage>
    {
        assert!(self.active);  // TODO(tailhook) maybe debug_assert
        self.writer.send_messages(msgs).await
    }

    pub async fn expect_ready(&mut self) -> Result<(), Error> {
        assert!(self.active);  // TODO(tailhook) maybe debug_assert
        self.reader.wait_ready().await?;
        self.end_clean();
        Ok(())
    }

    pub fn message(&mut self) -> reader::MessageFuture<'_, 'a> {
        assert!(self.active);  // TODO(tailhook) maybe debug_assert
        self.reader.message()
    }

    // TODO(tailhook) figure out if this is the best way
    pub async fn err_sync(&mut self) -> Result<(), Error> {
        assert!(self.active);  // TODO(tailhook) maybe debug_assert
        self.writer.send_messages(&[ClientMessage::Sync]).await?;
        timeout(Duration::from_secs(10), self.expect_ready()).await
            .map_err(ClientConnectionTimeoutError::with_source)??;
        Ok(())
    }

    pub async fn _process_exec(&mut self) -> Result<Bytes, Error> {
        assert!(self.active);  // TODO(tailhook) maybe debug_assert
        let status = loop {
            let msg = self.reader.message().await?;
            match msg {
                ServerMessage::CommandComplete0(c) => {
                    self.reader.wait_ready().await?;
                    self.end_clean();
                    break c.status_data;
                }
                ServerMessage::CommandComplete1(c) => {
                    self.process_complete(&c)?;
                    self.reader.wait_ready().await?;
                    self.end_clean();
                    break c.status_data;
                }
                ServerMessage::StateDataDescription(d) => {
                    self.set_state_description(d)?;
                }
                ServerMessage::ErrorResponse(err) => {
                    self.reader.wait_ready().await?;
                    self.end_clean();
                    return Err(err.into());
                }
                ServerMessage::Data(_) => { }
                msg => {
                    eprintln!("WARNING: unsolicited message {:?}", msg);
                }
            }
        };
        Ok(status)
    }

    pub(crate) async fn _query<A>(&mut self, request: &str, arguments: &A,
        bld: &StatementParams)
        -> Result<Typedesc, Error>
        where A: QueryArgs + ?Sized,
    {
        assert!(self.active);  // TODO(tailhook) maybe debug_assert
        if self.writer.proto.is_1() {
            self._query_1(request, arguments, bld).await
        } else {
            self._query_0(request, arguments, bld).await
        }
    }

    async fn _query_0<A>(&mut self, request: &str, arguments: &A,
        bld: &StatementParams)
        -> Result<Typedesc, Error>
        where A: QueryArgs + ?Sized,
    {
        let statement_name = Bytes::from_static(b"");
        self.send_messages(&[
            ClientMessage::Prepare(Prepare {
                headers: HashMap::new(),
                io_format: bld.io_format,
                expected_cardinality: bld.cardinality,
                statement_name: statement_name.clone(),
                command_text: String::from(request),
            }),
            ClientMessage::Flush,
        ]).await?;

        loop {
            let msg = self.reader.message().await?;
            match msg {
                ServerMessage::PrepareComplete(..) => {
                    break;
                }
                ServerMessage::ErrorResponse(err) => {
                    self.err_sync().await?;
                    return Err(err.into());
                }
                _ => {
                    return Err(ProtocolOutOfOrderError::with_message(format!(
                        "Unsolicited message {:?}", msg)));
                }
            }
        }

        self.send_messages(&[
            ClientMessage::DescribeStatement(DescribeStatement {
                headers: HashMap::new(),
                aspect: DescribeAspect::DataDescription,
                statement_name: statement_name.clone(),
            }),
            ClientMessage::Flush,
        ]).await?;

        let data_description = loop {
            let msg = self.reader.message().await?;
            match msg {
                ServerMessage::CommandDataDescription0(data_desc) => {
                    break data_desc;
                }
                ServerMessage::ErrorResponse(err) => {
                    self.err_sync().await?;
                    return Err(err.into());
                }
                _ => {
                    return Err(ProtocolOutOfOrderError::with_message(format!(
                        "Unsolicited message {:?}", msg)));
                }
            }
        };
        let desc = data_description.output()
            .map_err(ProtocolEncodingError::with_source)?;
        let inp_desc = data_description.input()
            .map_err(ProtocolEncodingError::with_source)?;

        let mut arg_buf = BytesMut::with_capacity(8);
        arguments.encode(&mut Encoder::new(
            &inp_desc.as_query_arg_context(),
            &mut arg_buf,
        ))?;

        self.send_messages(&[
            ClientMessage::Execute0(Execute0 {
                headers: HashMap::new(),
                statement_name: statement_name.clone(),
                arguments: arg_buf.freeze(),
            }),
            ClientMessage::Sync,
        ]).await?;
        Ok(desc)
    }

    async fn _query_1<A>(&mut self, request: &str, arguments: &A,
        bld: &StatementParams)
        -> Result<Typedesc, Error>
        where A: QueryArgs + ?Sized,
    {
        self.send_messages(&[
            ClientMessage::Parse(Parse {
                annotations: HashMap::new(),
                allowed_capabilities: Capabilities::ALL,
                compilation_flags: CompilationFlags::INJECT_OUTPUT_OBJECT_IDS,
                implicit_limit: None,
                output_format: bld.io_format,
                expected_cardinality: bld.cardinality,
                command_text: String::from(request),
                state: self.get_state().clone(),
            }),
            ClientMessage::Flush,
        ]).await?;

        let data_description = loop {
            let msg = self.reader.message().await?;
            match msg {
                ServerMessage::CommandDataDescription1(desc) => {
                    break desc;
                }
                ServerMessage::ErrorResponse(err) => {
                    self.err_sync().await?;
                    return Err(err.into());
                }
                _ => {
                    return Err(ProtocolOutOfOrderError::with_message(format!(
                        "Unsolicited message {:?}", msg)));
                }
            }
        };

        let desc = data_description.output()
            .map_err(ProtocolEncodingError::with_source)?;
        let inp_desc = data_description.input()
            .map_err(ProtocolEncodingError::with_source)?;

        let mut arg_buf = BytesMut::with_capacity(8);
        arguments.encode(&mut Encoder::new(
            &inp_desc.as_query_arg_context(),
            &mut arg_buf,
        ))?;

        self.send_messages(&[
            ClientMessage::Execute1(Execute1 {
                annotations: HashMap::new(),
                allowed_capabilities: Capabilities::ALL,
                compilation_flags: CompilationFlags::INJECT_OUTPUT_OBJECT_IDS,
                implicit_limit: None,
                output_format: bld.io_format,
                expected_cardinality: bld.cardinality,
                command_text: String::from(request),
                state: self.get_state().clone(),
                input_typedesc_id: data_description.input.id,
                output_typedesc_id: data_description.output.id,
                arguments: arg_buf.freeze(),
            }),
            ClientMessage::Sync,
        ]).await?;
        Ok(desc)
    }
}

impl Connection {
    pub async fn execute<S>(&mut self, request: S)
        -> Result<Bytes, Error>
        where S: ToString,
    {
        if self.version.is_1() {
            self.execute1(request).await
        }  else {
            self.execute0(request).await
        }
    }
    async fn execute0<S>(&mut self, request: S)
        -> Result<Bytes, Error>
        where S: ToString,
    {
        let mut seq = self.start_sequence().await?;
        seq.send_messages(&[
            ClientMessage::ExecuteScript(ExecuteScript {
                headers: HashMap::new(),
                script_text: request.to_string(),
            }),
        ]).await?;
        let status = loop {
            match seq.message().await? {
                ServerMessage::CommandComplete0(c) => {
                    seq.expect_ready().await?;
                    break c.status_data;
                }
                ServerMessage::ErrorResponse(err) => {
                    seq.expect_ready().await?;
                    return Err(err.into());
                }
                msg => {
                    eprintln!("WARNING: unsolicited message {:?}", msg);
                }
            }
        };
        Ok(status)
    }

    async fn execute1<S>(&mut self, request: S)
        -> Result<Bytes, Error>
        where S: ToString,
    {
        //let state_typedesc_id = seq.get_state_typedesc_id();
        let mut seq = self.start_sequence().await?;
        seq.send_messages(&[
            ClientMessage::Execute1(Execute1 {
                annotations: HashMap::new(),
                allowed_capabilities: Capabilities::ALL,
                compilation_flags: CompilationFlags::INJECT_OUTPUT_OBJECT_IDS,
                implicit_limit: None,
                output_format: IoFormat::None,
                expected_cardinality: Cardinality::Many,
                command_text: request.to_string(),
                state: seq.get_state().clone(),
                input_typedesc_id: Uuid::from_u128(0),
                output_typedesc_id: Uuid::from_u128(0),
                arguments: Bytes::new(),
            }),
            ClientMessage::Sync,
        ]).await?;
        let status = loop {
            match seq.message().await? {
                ServerMessage::CommandComplete1(c) => {
                    seq.process_complete(&c)?;
                    seq.expect_ready().await?;
                    break c.status_data;
                }
                ServerMessage::StateDataDescription(d) => {
                    seq.set_state_description(d)?;
                }
                ServerMessage::ErrorResponse(err) => {
                    seq.expect_ready().await?;
                    return Err(err.into());
                }
                msg => {
                    eprintln!("WARNING: unsolicited message {:?}", msg);
                }
            }
        };
        Ok(status)
    }

    pub async fn query<R, A>(&mut self, request: &str, arguments: &A)
        -> Result<QueryResponse<'_, R>, Error>
        where R: QueryResult,
              A: QueryArgs,
    {
        let mut seq = self.start_sequence().await?;
        let desc = seq._query(
            request, arguments,
            &StatementParams::new(),
        ).await?;
        match desc.root_pos() {
            Some(root_pos) => {
                let ctx = desc.as_queryable_context();
                let state = R::prepare(&ctx, root_pos)?;
                Ok(seq.response(state))
            }
            None => {
                let completion_message = seq._process_exec().await?;
                Err(NoResultExpected::with_message(
                    String::from_utf8_lossy(&completion_message[..])
                    .to_string()))?
            }
        }
    }

    pub async fn query_row<R, A>(&mut self, request: &str, arguments: &A)
        -> Result<R, Error>
        where R: Queryable,
              A: QueryArgs,
    {
        let mut query = self.query(request, arguments).await?;
        if let Some(result) = query.next().await.transpose()? {
            if let Some(_) = query.next().await.transpose()? {
                query.skip_remaining().await?;
                return Err(ProtocolError::with_message(
                    "extra row returned for query_row"
                ));
            }
            Ok(result)
        } else {
            return Err(NoDataError::build());
        }
    }

    pub async fn query_row_opt<R, A>(&mut self, request: &str, arguments: &A)
        -> Result<Option<R>, Error>
        where R: Queryable,
              A: QueryArgs,
    {
        let mut query = self.query(request, arguments).await?;
        if let Some(result) = query.next().await.transpose()? {
            if let Some(_) = query.next().await.transpose()? {
                return Err(ProtocolError::with_message(
                    "extra row returned for query_row"
                ));
            }
            Ok(Some(result))
        } else {
            Ok(None)
        }
    }

    pub async fn query_json<A>(&mut self, request: &str, arguments: &A)
        -> Result<QueryResponse<'_, String>, Error>
        where A: QueryArgs,
    {
        let mut seq = self.start_sequence().await?;
        let desc = seq._query(
            request, arguments,
            &StatementParams::new().io_format(IoFormat::Json),
        ).await?;
        match desc.root_pos() {
            Some(root_pos) => {
                let ctx = desc.as_queryable_context();
                let state = String::prepare(&ctx, root_pos)?;
                Ok(seq.response(state))
            }
            None => {
                let completion_message = seq._process_exec().await?;
                Err(NoResultExpected::with_message(
                    String::from_utf8_lossy(&completion_message[..])
                    .to_string()))?
            }
        }
    }

    pub async fn query_json_els<A>(&mut self, request: &str, arguments: &A)
        -> Result<QueryResponse<'_, String>, Error>
        where A: QueryArgs,
    {
        let mut seq = self.start_sequence().await?;
        let desc = seq._query(
            request, arguments,
            &StatementParams::new().io_format(IoFormat::JsonElements),
        ).await?;
        match desc.root_pos() {
            Some(root_pos) => {
                let ctx = desc.as_queryable_context();
                let state = String::prepare(&ctx, root_pos)?;
                Ok(seq.response(state))
            }
            None => {
                let completion_message = seq._process_exec().await?;
                Err(NoResultExpected::with_message(
                    String::from_utf8_lossy(&completion_message[..])
                    .to_string()))?
            }
        }
    }

    #[allow(dead_code)]
    pub async fn execute_args<A>(&mut self, request: &str, arguments: &A)
        -> Result<Bytes, Error>
        where A: QueryArgs,
    {
        let mut seq = self.start_sequence().await?;
        seq._query(request, arguments, &StatementParams::new()).await?;
        return seq._process_exec().await;
    }

    pub async fn get_version(&mut self) -> Result<String, Error> {
        self.query_row("SELECT sys::get_version_as_str()", &()).await
        .context("cannot fetch database version")
    }
}
