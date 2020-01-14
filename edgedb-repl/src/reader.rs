use std::io;
use std::cmp::{min, max};
use std::convert::TryInto;
use std::future::{Future};
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Poll, Context};

use async_std::io::Read as AsyncRead;
use async_std::stream::Stream;
use bytes::{Bytes, BytesMut, BufMut};
use snafu::{Snafu, ResultExt, Backtrace};

use edgedb_protocol::server_message::{ServerMessage};
use edgedb_protocol::errors::{DecodeError};
use edgedb_protocol::queryable::Queryable;
use edgedb_protocol::codec::Codec;
use edgedb_protocol::value::Value;

const BUFFER_SIZE: usize = 8192;
const MAX_BUFFER: usize = 1_048_576;

pub struct Reader<T> {
    stream: T,
    buf: BytesMut,
}

pub struct MessageFuture<'a, T> {
    reader: &'a mut Reader<T>,
}

pub struct QueryResponse<'a, T, D> {
    reader: &'a mut Reader<T>,
    complete: bool,
    buffer: Vec<Bytes>,
    decoder: D,
}

impl<T, D> Unpin for QueryResponse<'_, T, D> where T: Unpin {}

#[derive(Debug, Snafu)]
#[non_exhaustive]
pub enum ReadError {
    #[snafu(display("error decoding message: {}", source))]
    DecodeErr { source: DecodeError },
    #[snafu(display("error reading data: {}", source))]
    Io { source: io::Error },
    #[snafu(display("server message out of order: {:?}", message))]
    OutOfOrder { message: ServerMessage, backtrace: Backtrace },
    #[snafu(display("end of stream"))]
    Eos,
}

pub trait Decode {
    type Output;
    fn decode(&self, msg: Bytes) -> Result<Self::Output, DecodeError>;
}

pub struct QueryableDecoder<T>(PhantomData<*const T>);

unsafe impl<T> Send for QueryableDecoder<T> {}

impl<T> QueryableDecoder<T> {
    pub fn new() -> QueryableDecoder<T> {
        QueryableDecoder(PhantomData)
    }
}

impl<T: Queryable> Decode for QueryableDecoder<T> {
    type Output = T;
    fn decode(&self, msg: Bytes) -> Result<T, DecodeError> {
        Queryable::decode(&mut io::Cursor::new(msg))
    }
}

impl Decode for Arc<dyn Codec> {
    type Output = Value;
    fn decode(&self, msg: Bytes) -> Result<Self::Output, DecodeError> {
        self.decode_value(&mut io::Cursor::new(msg))
    }
}


impl<T: AsyncRead + Unpin> Reader<T> {
    pub fn new(stream: T) -> Reader<T> {
        return Reader {
            stream,
            buf: BytesMut::with_capacity(BUFFER_SIZE),
        }
    }
    pub fn message(&mut self) -> MessageFuture<T> {
        MessageFuture {
            reader: self,
        }
    }
    pub fn response<D: Decode>(&mut self, decoder: D) -> QueryResponse<T, D> {
        QueryResponse {
            reader: self,
            buffer: Vec::new(),
            complete: false,
            decoder,
        }
    }
    pub async fn wait_ready(&mut self) -> Result<(), ReadError> {
        loop {
            let msg = self.message().await?;
            match msg {
                ServerMessage::ReadyForCommand(..) => return Ok(()),
                // TODO(tailhook) should we react on messages somehow?
                //                At list parse LogMessage's?
                _ => {},
            }
        }
    }
    fn poll_message(&mut self, cx: &mut Context)
        -> Poll<Result<ServerMessage, ReadError>>
    {
        let Reader { ref mut buf, ref mut stream } = self;
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
                match Pin::new(&mut *stream).poll_read(cx, buf.bytes_mut()) {
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
        return Poll::Ready(Ok(result));
    }
}

impl<'a, T> Future for MessageFuture<'a, T>
    where T: AsyncRead + Unpin,
{
    type Output = Result<ServerMessage, ReadError>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        self.reader.poll_message(cx)
    }
}

impl<'a, T, D> Stream for QueryResponse<'a, T, D>
    where T: AsyncRead + Unpin,
          D: Decode,
{
    type Item = Result<D::Output, ReadError>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context)
        -> Poll<Option<Self::Item>>
    {
        let QueryResponse {
            ref mut buffer,
            ref mut complete,
            ref mut reader,
            ref decoder,
        } = *self;
        while buffer.len() == 0 {
            match reader.poll_message(cx) {
                Poll::Ready(Ok(ServerMessage::Data(data))) => {
                    if *complete {
                        return
                            OutOfOrder { message: ServerMessage::Data(data) }
                            .fail()?;
                    }
                    buffer.extend(data.data.into_iter().rev());
                }
                Poll::Ready(Ok(m @ ServerMessage::CommandComplete(_))) => {
                    if *complete {
                        OutOfOrder { message: m }.fail()?;
                    }
                    *complete = true;
                }
                Poll::Ready(Ok(m @ ServerMessage::ReadyForCommand(_))) => {
                    if !*complete {
                        OutOfOrder { message: m }.fail()?;
                    }
                    return Poll::Ready(None);
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
