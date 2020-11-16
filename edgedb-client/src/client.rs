use std::collections::HashMap;
use std::default::Default;
use std::fmt;
use std::str;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{self, Context};
use async_std::prelude::StreamExt;
use async_std::future::{timeout, pending};
use async_std::io::prelude::WriteExt;
use async_std::io::ReadExt;
use async_listen::ByteStream;
use bytes::{Bytes, BytesMut};
use typemap::TypeMap;

use edgedb_protocol::client_message::ClientMessage;
use edgedb_protocol::client_message::{Prepare, IoFormat, Cardinality};
use edgedb_protocol::client_message::{DescribeStatement, DescribeAspect};
use edgedb_protocol::client_message::{Execute, ExecuteScript};
use edgedb_protocol::codec::Codec;
use edgedb_protocol::server_message::ServerMessage;
use edgedb_protocol::server_message::{TransactionState};
use edgedb_protocol::queryable::{Queryable, Decoder};
use edgedb_protocol::value::Value;
use edgedb_protocol::descriptors::OutputTypedesc;

use crate::server_params::ServerParam;
use crate::reader::{self, QueryableDecoder, QueryResponse, Reader};
use crate::errors::NoResultExpected;

pub use crate::features::ProtocolVersion;


/// A single connection to the EdgeDB
pub struct Connection {
    pub(crate) stream: ByteStream,
    pub(crate) input_buf: BytesMut,
    pub(crate) output_buf: BytesMut,
    pub(crate) version: ProtocolVersion,
    pub(crate) params: TypeMap<dyn typemap::DebugAny + Send + Sync>,
    pub(crate) transaction_state: TransactionState,
    pub(crate) dirty: bool,
}

pub struct Sequence<'a> {
    pub writer: Writer<'a>,
    pub reader: Reader<'a>,
    pub(crate) active: bool,
    dirty: &'a mut bool,
    proto: &'a ProtocolVersion,
}


pub struct Writer<'a> {
    stream: &'a ByteStream,
    outbuf: &'a mut BytesMut,
}


impl<'a> Sequence<'a> {

    pub fn response<D: reader::Decode>(self, decoder: D)
        -> QueryResponse<'a, D>
    {
        assert!(self.active);  // TODO(tailhook) maybe debug_assert
        reader::QueryResponse {
            seq: self,
            buffer: Vec::new(),
            error: None,
            complete: false,
            decoder,
        }
    }

    pub fn end_clean(&mut self) {
        self.active = false;
        *self.dirty = false;
    }

    fn decoder(&self) -> Decoder {
        let mut dec = Decoder::default();
        dec.has_implicit_tid = self.proto.has_implicit_tid();
        return dec;
    }
}

impl Connection {
    pub fn protocol(&self) -> &ProtocolVersion {
        return &self.version
    }
    pub async fn passive_wait<T>(&mut self) -> T {
        let mut buf = [0u8; 1];
        self.stream.read(&mut buf[..]).await.ok();
        // any erroneous or successful read (even 0) means need reconnect
        self.dirty = true;
        pending::<()>().await;
        unreachable!();
    }
    pub fn is_consistent(&self) -> bool {
        !self.dirty
    }
    pub async fn terminate(mut self) -> anyhow::Result<()> {
        let mut seq = self.start_sequence().await?;
        seq.send_messages(&[ClientMessage::Terminate]).await?;
        match seq.message().await {
            Err(reader::ReadError::Eos) => Ok(()),
            Err(e) => Err(e)?,
            Ok(msg) => anyhow::bail!("unsolicited message {:?}", msg),
        }
    }
    pub async fn start_sequence<'x>(&'x mut self)
        -> anyhow::Result<Sequence<'x>>
    {
        if self.dirty {
            anyhow::bail!("Connection is inconsistent state. \
                Please reconnect.");
        }
        self.dirty = true;
        let reader = Reader {
            buf: &mut self.input_buf,
            stream: &self.stream,
            transaction_state: &mut self.transaction_state,
        };
        let writer = Writer {
            outbuf: &mut self.output_buf,
            stream: &self.stream,
        };
        Ok(Sequence {
            writer,
            reader,
            active: true,
            dirty: &mut self.dirty,
            proto: &self.version,
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
}

impl<'a> Writer<'a> {

    pub async fn send_messages<'x, I>(&mut self, msgs: I)
        -> Result<(), anyhow::Error>
        where I: IntoIterator<Item=&'x ClientMessage>
    {
        self.outbuf.truncate(0);
        for msg in msgs {
            msg.encode(&mut self.outbuf)?;
        }
        self.stream.write_all(&self.outbuf[..]).await?;
        Ok(())
    }

}


impl<'a> Sequence<'a> {
    pub async fn send_messages<'x, I>(&mut self, msgs: I)
        -> Result<(), anyhow::Error>
        where I: IntoIterator<Item=&'x ClientMessage>
    {
        assert!(self.active);  // TODO(tailhook) maybe debug_assert
        self.writer.send_messages(msgs).await
    }

    pub async fn expect_ready(&mut self) -> Result<(), reader::ReadError> {
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
    pub async fn err_sync(&mut self) -> Result<(), anyhow::Error> {
        assert!(self.active);  // TODO(tailhook) maybe debug_assert
        self.writer.send_messages(&[ClientMessage::Sync]).await?;
        timeout(Duration::from_secs(10), self.expect_ready()).await??;
        Ok(())
    }

    pub async fn _process_exec(&mut self) -> anyhow::Result<Bytes> {
        assert!(self.active);  // TODO(tailhook) maybe debug_assert
        let status = loop {
            match self.reader.message().await? {
                ServerMessage::CommandComplete(c) => {
                    self.reader.wait_ready().await?;
                    self.end_clean();
                    break c.status_data;
                }
                ServerMessage::ErrorResponse(err) => {
                    self.reader.wait_ready().await?;
                    self.end_clean();
                    return Err(anyhow::anyhow!(err));
                }
                ServerMessage::Data(_) => { }
                msg => {
                    eprintln!("WARNING: unsolicited message {:?}", msg);
                }
            }
        };
        Ok(status)
    }

    async fn _query(&mut self, request: &str, arguments: &Value,
        io_format: IoFormat)
        -> Result<OutputTypedesc, anyhow::Error >
    {
        assert!(self.active);  // TODO(tailhook) maybe debug_assert
        let statement_name = Bytes::from_static(b"");

        self.send_messages(&[
            ClientMessage::Prepare(Prepare {
                headers: HashMap::new(),
                io_format,
                expected_cardinality: Cardinality::Many,
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
                    return Err(anyhow::anyhow!(err));
                }
                _ => {
                    return Err(anyhow::anyhow!(
                        "Unsolicited message {:?}", msg));
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
                ServerMessage::CommandDataDescription(data_desc) => {
                    break data_desc;
                }
                ServerMessage::ErrorResponse(err) => {
                    self.err_sync().await?;
                    return Err(anyhow::anyhow!(err));
                }
                _ => {
                    return Err(anyhow::anyhow!(
                        "Unsolicited message {:?}", msg));
                }
            }
        };
        let desc = data_description.output()?;
        let incodec = data_description.input()?.build_codec()?;

        let mut arg_buf = BytesMut::with_capacity(8);
        incodec.encode(&mut arg_buf, &arguments)?;

        self.send_messages(&[
            ClientMessage::Execute(Execute {
                headers: HashMap::new(),
                statement_name: statement_name.clone(),
                arguments: arg_buf.freeze(),
            }),
            ClientMessage::Sync,
        ]).await?;
        Ok(desc)
    }
}

impl Connection {
    pub async fn execute<S>(&mut self, request: S)
        -> Result<Bytes, anyhow::Error>
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
                ServerMessage::CommandComplete(c) => {
                    seq.expect_ready().await?;
                    break c.status_data;
                }
                ServerMessage::ErrorResponse(err) => {
                    seq.expect_ready().await?;
                    return Err(anyhow::anyhow!(err));
                }
                msg => {
                    eprintln!("WARNING: unsolicited message {:?}", msg);
                }
            }
        };
        Ok(status)
    }

    pub async fn query<R>(&mut self, request: &str, arguments: &Value)
        -> anyhow::Result<QueryResponse<'_, QueryableDecoder<R>>>
        where R: Queryable,
    {
        let mut seq = self.start_sequence().await?;
        let desc = seq._query(request, arguments, IoFormat::Binary).await?;
        match desc.root_pos() {
            Some(root_pos) => {
                let mut ctx = desc.as_queryable_context();
                ctx.has_implicit_tid = seq.proto.has_implicit_tid();
                R::check_descriptor(&ctx, root_pos)?;
                let decoder = seq.decoder();
                Ok(seq.response(QueryableDecoder::new(decoder)))
            }
            None => {
                let completion_message = seq._process_exec().await?;
                Err(NoResultExpected { completion_message })?
            }
        }
    }

    pub async fn query_row<R>(&mut self, request: &str, arguments: &Value)
        -> anyhow::Result<R>
        where R: Queryable,
    {
        let mut query = self.query(request, arguments).await?;
        if let Some(result) = query.next().await.transpose()? {
            if let Some(_) = query.next().await.transpose()? {
                query.skip_remaining().await?;
                anyhow::bail!("extra row returned for query_row");
            }
            Ok(result)
        } else {
            anyhow::bail!("no results returned")
        }
    }

    pub async fn query_row_opt<R>(&mut self, request: &str, arguments: &Value)
        -> anyhow::Result<Option<R>>
        where R: Queryable,
    {
        let mut query = self.query(request, arguments).await?;
        if let Some(result) = query.next().await.transpose()? {
            if let Some(_) = query.next().await.transpose()? {
                anyhow::bail!("extra row returned for query_row");
            }
            Ok(Some(result))
        } else {
            Ok(None)
        }
    }

    pub async fn query_json(&mut self, request: &str, arguments: &Value)
        -> anyhow::Result<QueryResponse<'_, QueryableDecoder<String>>>
    {
        let mut seq = self.start_sequence().await?;
        let desc = seq._query(request, arguments, IoFormat::Json).await?;
        match desc.root_pos() {
            Some(root_pos) => {
                let mut ctx = desc.as_queryable_context();
                ctx.has_implicit_tid = seq.proto.has_implicit_tid();
                String::check_descriptor(&ctx, root_pos)?;
                let decoder = seq.decoder();
                Ok(seq.response(QueryableDecoder::new(decoder)))
            }
            None => {
                let completion_message = seq._process_exec().await?;
                Err(NoResultExpected { completion_message })?
            }
        }
    }

    pub async fn query_json_els(&mut self, request: &str, arguments: &Value)
        -> Result<
            QueryResponse<'_, QueryableDecoder<String>>,
            anyhow::Error
        >
    {
        let mut seq = self.start_sequence().await?;
        let desc = seq._query(request, arguments,
            IoFormat::JsonElements).await?;
        match desc.root_pos() {
            Some(root_pos) => {
                let mut ctx = desc.as_queryable_context();
                ctx.has_implicit_tid = seq.proto.has_implicit_tid();
                String::check_descriptor(&ctx, root_pos)?;
                let decoder = seq.decoder();
                Ok(seq.response(QueryableDecoder::new(decoder)))
            }
            None => {
                let completion_message = seq._process_exec().await?;
                Err(NoResultExpected { completion_message })?
            }
        }
    }

    pub async fn query_dynamic(&mut self, request: &str, arguments: &Value)
        -> anyhow::Result<QueryResponse<'_, Arc<dyn Codec>>>
    {
        let mut seq = self.start_sequence().await?;
        let desc = seq._query(request, arguments, IoFormat::Binary).await?;
        let codec = desc.build_codec()?;
        Ok(seq.response(codec))
    }


    #[allow(dead_code)]
    pub async fn execute_args(&mut self, request: &str, arguments: &Value)
        -> Result<Bytes, anyhow::Error>
    {
        let mut seq = self.start_sequence().await?;
        seq._query(request, arguments, IoFormat::Binary).await?;
        return seq._process_exec().await;
    }

    pub async fn get_version(&mut self) -> Result<String, anyhow::Error> {
        self.query_row(
            "SELECT sys::get_version_as_str()",
            &Value::empty_tuple(),
        ).await
        .context("cannot fetch database version")
    }
}


