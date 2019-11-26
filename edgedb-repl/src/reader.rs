use std::cmp::{min, max};
use std::convert::TryInto;
use std::future::{Future};
use std::task::{Poll, Context};
use std::pin::Pin;

use async_std::io;
use bytes::{BytesMut, BufMut};
use snafu::{Snafu, ResultExt};

use edgedb_protocol::server_message::{ServerMessage};
use edgedb_protocol::errors::{DecodeError};

const BUFFER_SIZE: usize = 8192;
const MAX_BUFFER: usize = 1_048_576;

pub struct Reader<T> {
    stream: T,
    buf: BytesMut,
}

pub struct MessageFuture<'a, T> {
    reader: &'a mut Reader<T>,
}

#[derive(Debug, Snafu)]
pub enum ReadError {
    #[snafu(display("error decoding message: {}", source))]
    Decode { source: DecodeError },
    #[snafu(display("error reading data: {}", source))]
    Io { source: io::Error },
    #[doc(hidden)]
    __NonExhaustive,
}


impl<T: io::Read + Unpin> Reader<T> {
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
}

impl<'a, T> Future for MessageFuture<'a, T>
    where T: io::Read + Unpin,
{
    type Output = Result<ServerMessage, ReadError>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        let Reader { ref mut buf, ref mut stream } = &mut self.reader;
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
        let result = ServerMessage::decode(&frame).context(Decode)?;
        return Poll::Ready(Ok(result));
    }
}
