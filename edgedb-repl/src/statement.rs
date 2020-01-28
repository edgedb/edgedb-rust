use std::fmt;
use std::error;
use std::pin::Pin;
use std::task::{Poll, Context};
use std::future::Future;
use std::mem::transmute;

use anyhow;
use async_std::io::{Read as AsyncRead};
use bytes::{Bytes, BytesMut, BufMut};

use edgeql_parser::preparser::{full_statement, Continuation};


#[derive(Debug)]
pub struct EndOfFile;

pub struct ReadStatement<'a, T> {
    buf: &'a mut BytesMut,
    stream: &'a mut T,
    continuation: Option<Continuation>,
}


impl<'a, T> ReadStatement<'a, T> {
    pub fn new(buf: &'a mut BytesMut, stream: &'a mut T)
        -> ReadStatement<'a, T>
    {
        ReadStatement { buf, stream, continuation: None }
    }
}

impl<'a, T> Future for ReadStatement<'a, T>
    where T: AsyncRead + Unpin,
{
    type Output = Result<Bytes, anyhow::Error>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        let ReadStatement { buf, stream, ref mut continuation } = &mut *self;
        let statement_len = loop {
            match full_statement(&buf[..], continuation.take()) {
                Ok(len) => break len,
                Err(cont) => *continuation = Some(cont),
            };
            buf.reserve(8192);
            unsafe {
                // this is save because the underlying ByteStream always
                // initializes read bytes
                let dest: &mut [u8] = transmute(buf.bytes_mut());
                match Pin::new(&mut *stream).poll_read(cx, dest) {
                    Poll::Ready(Ok(0)) => {
                        return Poll::Ready(Err(EndOfFile.into()));
                    }
                    Poll::Ready(Ok(bytes)) => {
                        buf.advance_mut(bytes);
                        continue;
                    }
                    Poll::Ready(err @ Err(_)) => { err?; }
                    Poll::Pending => return Poll::Pending,
                }
            }
        };
        let data = buf.split_to(statement_len).freeze();
        return Poll::Ready(Ok(data));
    }
}

impl fmt::Display for EndOfFile {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        "end of file".fmt(f)
    }
}

impl error::Error for EndOfFile {
}
