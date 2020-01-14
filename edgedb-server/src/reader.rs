use std::cmp::{min, max};
use std::convert::TryInto;
use std::future::{Future};
use std::task::{Poll, Context};
use std::pin::Pin;

use async_std::io::Read;
use bytes::{BytesMut, BufMut};
use async_listen::ByteStream;

use edgedb_protocol::client_message::{ClientMessage};

use crate::connection::ConnectionErr;


const BUFFER_SIZE: usize = 8192;
const MAX_BUFFER: usize = 1_048_576;


pub struct Reader<'a> {
    name: &'a str,
    stream: &'a ByteStream,
    buf: BytesMut,
}

pub struct MessageFuture<'a, 'b: 'a> {
    reader: &'a mut Reader<'b>,
}

impl<'a> Reader<'a> {
    pub fn new(stream: &'a ByteStream, log_name: &'a str) -> Reader<'a> {
        return Reader {
            name: log_name,
            stream,
            buf: BytesMut::with_capacity(BUFFER_SIZE),
        }
    }
    pub fn message<'x>(&'x mut self) -> MessageFuture<'x, 'a> {
        MessageFuture {
            reader: self,
        }
    }
}

impl<'a, 'b> Future for MessageFuture<'a, 'b> {
    type Output = Result<ClientMessage, ()>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        let Reader {ref mut buf, ref name, ref mut stream} = &mut self.reader;
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
                match Pin::new(&mut &**stream).poll_read(cx, buf.bytes_mut()) {
                    Poll::Ready(Ok(0)) => {
                        log::debug!("{}: connection closed by peer", name);
                        return Poll::Ready(Err(()));
                    }
                    Poll::Ready(Ok(bytes)) => {
                        buf.advance_mut(bytes);
                        continue;
                    }
                    Poll::Ready(r @ Err(_)) => {
                        r.connection_err(name)?;
                    }
                    Poll::Pending => return Poll::Pending,
                }
            }
        };
        let frame = buf.split_to(frame_len).freeze();
        let result = ClientMessage::decode(&frame)
            .map_err(|e| {
                log::debug!("{}: Decode error: {}", name, e);
            })?;
        return Poll::Ready(Ok(result));
    }
}
