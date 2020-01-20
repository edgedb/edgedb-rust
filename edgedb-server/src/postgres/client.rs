use std::io;
use std::future::{Future};
use std::pin::Pin;
use std::task::{Poll, Context};
use std::mem::transmute;

use async_std::io::Read as AsyncRead;
use async_std::io::prelude::WriteExt;
use async_std::net::TcpStream;
use async_listen::ByteStream;
use bytes::{BytesMut, BufMut};
use postgres_protocol::message::frontend as out;
use postgres_protocol::message::backend as inp;
use snafu::{Snafu, ResultExt};

use crate::postgres::Dsn;


#[derive(Debug, Snafu)]
#[non_exhaustive]
pub enum ReadError {
    #[snafu(display("error decoding message: {}", source))]
    DecodeErr { source: io::Error },
    #[snafu(display("error necoding message: {}", source))]
    EncodeErr { source: io::Error },
    #[snafu(display("error reading data: {}", source))]
    Io { source: io::Error },
    #[snafu(display("end of stream"))]
    Eos,
}

pub struct Client {
    name: String,
    stream: ByteStream,
    inbuf: BytesMut,
}

pub struct MessageFuture<'a> {
    client: &'a mut Client,
}

impl Client {
    pub async fn connect(dsn: &Dsn) -> Result<Client, ReadError> {
        // TODO(tailhook) insert a timeout
        let tcp = TcpStream::connect(dsn.addr()).await.context(Io)?;
        let mut cli = Client {
            name: tcp.peer_addr().map(|x| x.to_string())
                     .unwrap_or_else(|_| "<addr-error>".into()),
            stream: ByteStream::new_tcp_detached(tcp),
            inbuf: BytesMut::with_capacity(8192),
        };
        let mut buf = BytesMut::new();
        out::startup_message(vec![
            ("client_encoding", "utf-8"),
            ("search_path", "edgedb"),
            ("timezone", "UTC"),
            ("default_transaction_isolation", "repeatable read"),
            ("user", dsn.username()),
            ("database", dsn.database()),
        ].into_iter(), &mut buf).context(EncodeErr)?;
        cli.stream.write_all(&buf).await.context(Io)?;
        let msg = cli.message().await?;
        println!("GOT MESSAGE");
        todo!();
    }
    fn message<'x>(&'x mut self) -> MessageFuture<'x> {
        MessageFuture {
            client: self,
        }
    }

    fn poll_message(&mut self, cx: &mut Context)
        -> Poll<Result<inp::Message, ReadError>>
    {
        let Client { inbuf: ref mut buf, ref mut stream, .. } = self;
        let msg = loop {
            match inp::Message::parse(buf).context(DecodeErr)? {
                Some(msg) => break msg,
                None => {}
            }
            // Message::parse already reserves the space for next message
            // in the buffer
            unsafe {
                // this is save because the underlying ByteStream always
                // initializes read bytes
                let dest: &mut [u8] = transmute(buf.bytes_mut());
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
        return Poll::Ready(Ok(msg));
    }
}


impl<'a> Future for MessageFuture<'a> {
    type Output = Result<inp::Message, ReadError>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        self.client.poll_message(cx)
    }
}
