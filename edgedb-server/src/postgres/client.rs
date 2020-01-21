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
use fallible_iterator::FallibleIterator;
use postgres_protocol::message::frontend as out;
use postgres_protocol::message::backend as inp;
use snafu::{Snafu, OptionExt, ResultExt, Backtrace};

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
    #[snafu(display("server message out of order"))]
    OutOfOrder { backtrace: Backtrace },
    #[snafu(display("postgres error: {}", message))]
    PostgresErr {
        message: String,
        backtrace: Backtrace
    },
    #[snafu(display("end of stream"))]
    Eos,
}

#[allow(dead_code)] // TODO(tailhook)
struct BackendKey {
    pid: i32,
    secret: i32,
}

#[allow(dead_code)] // TODO(tailhook)
pub struct Client {
    name: String,
    stream: ByteStream,
    inbuf: BytesMut,
    backend_key: BackendKey,
}

pub struct MessageFuture<'a> {
    buf: &'a mut BytesMut,
    stream: &'a mut ByteStream,
}

fn convert_err(e: &inp::ErrorResponseBody) -> Result<(), ReadError> {
    let mut iter = e.fields();
    let mut message = None;
    while let Some(item) = iter.next().context(DecodeErr)? {
        if item.type_() == 77 {
            message = Some(item.value().to_string());
        }
    }
    PostgresErr {
        message: message.unwrap_or_else(|| "unknown error".into()),
    }.fail()?
}

impl Client {
    pub async fn connect(dsn: &Dsn) -> Result<Client, ReadError> {
        // TODO(tailhook) insert a timeout
        let tcp = TcpStream::connect(dsn.addr()).await.context(Io)?;
        let mut stream = ByteStream::new_tcp_detached(tcp);
        let name = stream.peer_addr().map(|x| x.to_string())
                         .unwrap_or_else(|_| "<addr-error>".into());
        let mut buf = BytesMut::new();
        out::startup_message(vec![
            ("client_encoding", "utf-8"),
            ("search_path", "edgedb"),
            ("timezone", "UTC"),
            ("default_transaction_isolation", "repeatable read"),
            ("user", dsn.username()),
            ("database", dsn.database()),
        ].into_iter(), &mut buf).context(EncodeErr)?;
        stream.write_all(&buf).await.context(Io)?;
        let mut backend_key = None;
        let msg = message(&mut buf, &mut stream).await?;
        match msg {
            inp::Message::ErrorResponse(e) => {
                convert_err(&e)?;
                unreachable!();
            }
            inp::Message::ReadyForQuery(_) => {
                todo!("READY");
            }
            inp::Message::AuthenticationOk => {}
            _ => return OutOfOrder.fail()?,
        }
        loop {
            let msg = message(&mut buf, &mut stream).await?;
            match msg {
                inp::Message::ErrorResponse(e) => {
                    convert_err(&e)?;
                    unreachable!();
                }
                inp::Message::ParameterStatus(param) => {
                    log::debug!("Param {:?} = {:?}",
                        param.name(), param.value());
                }
                inp::Message::BackendKeyData(keydata) => {
                    log::info!("{}: Backend pid {}",
                        name, keydata.process_id());
                    backend_key = Some(BackendKey {
                        pid: keydata.process_id(),
                        secret: keydata.secret_key(),
                    });
                }
                inp::Message::ReadyForQuery(_) => {
                    break;
                }
                _ => return OutOfOrder.fail()?,
            }
        }

        let _cli = Client {
            name,
            stream,
            inbuf: BytesMut::with_capacity(8192),
            backend_key: backend_key.context(OutOfOrder)?,
        };
        todo!("READY");
    }
    #[allow(dead_code)] // TODO(tailhook)
    fn message<'x>(&'x mut self) -> MessageFuture<'x> {
        MessageFuture {
            buf: &mut self.inbuf,
            stream: &mut self.stream,
        }
    }
}

fn message<'x>(buf: &'x mut BytesMut, stream: &'x mut ByteStream)
    -> MessageFuture<'x>
{
    MessageFuture { buf, stream }
}


impl<'a> Future for MessageFuture<'a> {
    type Output = Result<inp::Message, ReadError>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        let MessageFuture { buf, stream } = &mut *self;
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
