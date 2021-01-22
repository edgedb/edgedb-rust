use std::io;
use std::cmp::{min, max};
use std::convert::TryInto;
use std::future::{Future};
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;
use std::slice;
use std::task::{Poll, Context};

use async_std::io::Read as AsyncRead;
use async_std::stream::{Stream, StreamExt};
use async_listen::ByteStream;
use bytes::{Bytes, BytesMut, BufMut};
use snafu::{Snafu, ResultExt, Backtrace};

use edgedb_protocol::server_message::{ServerMessage, ErrorResponse};
use edgedb_protocol::server_message::{ReadyForCommand, TransactionState};
use edgedb_protocol::errors::{DecodeError};
use edgedb_protocol::queryable::{Queryable, Decoder};
use edgedb_protocol::codec::Codec;
use edgedb_protocol::value::Value;

use crate::client;


const BUFFER_SIZE: usize = 8192;
const MAX_BUFFER: usize = 1_048_576;

pub struct Reader<'a> {
    pub(crate) stream: &'a ByteStream,
    pub(crate) buf: &'a mut BytesMut,
    pub(crate) transaction_state: &'a mut TransactionState,
}

pub struct MessageFuture<'a, 'r: 'a> {
    reader: &'a mut Reader<'r>,
}

// Note: query response expects query *followed by* Sync messsage
pub struct QueryResponse<'a, D> {
    pub(crate) seq: client::Sequence<'a>,
    pub(crate) complete: bool,
    pub(crate) error: Option<ErrorResponse>,
    pub(crate) buffer: Vec<Bytes>,
    pub(crate) decoder: D,
}

#[derive(Debug, Snafu)]
#[non_exhaustive]
pub enum ReadError {
    #[snafu(display("error decoding message"))]
    DecodeErr { source: DecodeError },
    #[snafu(display("error reading data"))]
    Io { source: io::Error },
    #[snafu(display("server message out of order: {:?}", message))]
    OutOfOrder { message: ServerMessage, backtrace: Backtrace },
    #[snafu(display("request error: {}", error))]
    RequestError { error: ErrorResponse, backtrace: Backtrace },
    #[snafu(display("end of stream"))]
    Eos,
}

pub trait Decode {
    type Output;
    fn decode(&self, msg: Bytes)
        -> Result<Self::Output, DecodeError>;
}

pub struct QueryableDecoder<T> {
    decoder: Decoder,
    phantom: PhantomData<*const T>,
}

unsafe impl<T> Send for QueryableDecoder<T> {}
impl<D> Unpin for QueryResponse<'_, D> {}

impl<T> QueryableDecoder<T> {
    pub fn new(decoder: Decoder) -> QueryableDecoder<T> {
        QueryableDecoder {
            decoder,
            phantom: PhantomData,
        }
    }
}

impl<T: Queryable> Decode for QueryableDecoder<T> {
    type Output = T;
    fn decode(&self, msg: Bytes) -> Result<T, DecodeError> {
        Queryable::decode(&self.decoder, &msg)
    }
}

impl Decode for Arc<dyn Codec> {
    type Output = Value;
    fn decode(&self, msg: Bytes)
        -> Result<Self::Output, DecodeError>
    {
        (&**self).decode(&msg)
    }
}

impl<'r> Reader<'r> {
    pub fn message(&mut self) -> MessageFuture<'_, 'r> {
        MessageFuture {
            reader: self,
        }
    }
    pub fn consume_ready(&mut self, ready: ReadyForCommand) {
        *self.transaction_state = ready.transaction_state;
    }
    pub async fn wait_ready(&mut self) -> Result<(), ReadError> {
        loop {
            let msg = self.message().await?;
            match msg {
                ServerMessage::ReadyForCommand(ready) => {
                    self.consume_ready(ready);
                    return Ok(())
                }
                // TODO(tailhook) should we react on messages somehow?
                //                At list parse LogMessage's?
                _ => {},
            }
        }
    }
    fn poll_message(&mut self, cx: &mut Context)
        -> Poll<Result<ServerMessage, ReadError>>
    {
        let Reader { ref mut buf, ref mut stream, .. } = self;
        let frame_len = loop {
            let mut next_read = BUFFER_SIZE;
            let buf_len = buf.len();
            if buf_len > 5 {
                let len = u32::from_be_bytes(
                    buf[1..5].try_into().unwrap())
                    as usize;
                if buf_len >= len + 1 {
                    break len+1;
                }
                next_read = max(min(len + 1 - buf_len, MAX_BUFFER),
                                BUFFER_SIZE);
                debug_assert!(next_read > 0);
            }

            buf.reserve(next_read);
            unsafe {
                // this is safe because the underlying ByteStream always
                // initializes read bytes
                let chunk = buf.chunk_mut();
                let dest: &mut [u8] = slice::from_raw_parts_mut(
                    chunk.as_mut_ptr(), chunk.len());
                match Pin::new(&mut *stream).poll_read(cx, dest) {
                    Poll::Ready(Ok(0)) => {
                        return Poll::Ready(Err(ReadError::Eos));
                    }
                    Poll::Ready(Ok(bytes)) => {
                        buf.advance_mut(bytes);
                        continue;
                    }
                    Poll::Ready(r @ Err(_)) => { r.context(Io)?; }
                    Poll::Pending => return Poll::Pending,
                }
            }
        };
        let frame = buf.split_to(frame_len).freeze();
        let result = ServerMessage::decode(&frame).context(DecodeErr)?;
        log::debug!(target: "edgedb::incoming::frame",
                    "Frame Contents: {:#?}", result);
        return Poll::Ready(Ok(result));
    }
}

impl Future for MessageFuture<'_, '_> {
    type Output = Result<ServerMessage, ReadError>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        self.reader.poll_message(cx)
    }
}

impl<D> QueryResponse<'_, D>
    where D: Decode,
{
    pub async fn skip_remaining(mut self) -> Result<(), ReadError> {
        while let Some(_) = self.next().await.transpose()?  {}
        Ok(())
    }
    pub async fn get_completion(mut self) -> anyhow::Result<Bytes> {
        Ok(self.seq._process_exec().await?)
    }
}

impl<D> Stream for QueryResponse<'_, D>
    where D: Decode,
{
    type Item = Result<D::Output, ReadError>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context)
        -> Poll<Option<Self::Item>>
    {
        assert!(self.seq.active);  // TODO(tailhook) maybe debug_assert
        let QueryResponse {
            ref mut buffer,
            ref mut complete,
            ref mut error,
            ref mut seq,
            ref decoder,
        } = *self;
        while buffer.len() == 0 {
            match seq.reader.poll_message(cx) {
                Poll::Ready(Ok(ServerMessage::Data(data))) if error.is_none()
                => {
                    if *complete {
                        return
                            OutOfOrder { message: ServerMessage::Data(data) }
                            .fail()?;
                    }
                    buffer.extend(data.data.into_iter().rev());
                }
                Poll::Ready(Ok(m @ ServerMessage::CommandComplete(_)))
                if error.is_none()
                => {
                    if *complete {
                        OutOfOrder { message: m }.fail()?;
                    }
                    *complete = true;
                }
                Poll::Ready(Ok(ServerMessage::ReadyForCommand(r))) => {
                    if let Some(error) = error.take() {
                        seq.reader.consume_ready(r);
                        seq.end_clean();
                        return Poll::Ready(Some(
                            RequestError { error }.fail()));
                    } else {
                        if !*complete {
                            return OutOfOrder {
                                message: ServerMessage::ReadyForCommand(r)
                            }.fail()?;
                        }
                        seq.reader.consume_ready(r);
                        seq.end_clean();
                        return Poll::Ready(None);
                    }
                }
                Poll::Ready(Ok(ServerMessage::ErrorResponse(e))) => {
                    *error = Some(e);
                    continue;
                }
                Poll::Ready(Ok(message)) => {
                    OutOfOrder { message }.fail()?;
                }
                Poll::Ready(Err(e)) => {
                    return Poll::Ready(Some(Err(e)));
                }
                Poll::Pending => return Poll::Pending,
            }
        }
        let chunk = buffer.pop().unwrap();
        Poll::Ready(Some(decoder.decode(chunk).context(DecodeErr)))
    }
}
