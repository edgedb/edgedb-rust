use bytes::BytesMut;

use async_std::io::prelude::WriteExt;
use async_listen::ByteStream;

use edgedb_protocol::server_message::{ServerMessage};
use crate::connection::ConnectionErr;

const BUFFER_SIZE: usize = 8192;

pub struct Writer<'a> {
    name: &'a str,
    stream: &'a ByteStream,
    buf: BytesMut,
}

impl<'a> Writer<'a> {
    pub fn new(stream: &'a ByteStream, log_name: &'a str) -> Writer<'a> {
        return Writer {
            name: log_name,
            stream,
            buf: BytesMut::with_capacity(BUFFER_SIZE),
        }
    }
    pub async fn send_message(&mut self, msg: ServerMessage) -> Result<(), ()>
    {
        self.buf.truncate(0);
        msg.encode(&mut self.buf)
            .map_err(|e| {
                log::error!("{}: Can't encode message {:?}: {}",
                    self.name, msg, e);
            })?;
        self.stream.write_all(&self.buf[..])
            .await.connection_err(self.name)?;
        Ok(())
    }
}
