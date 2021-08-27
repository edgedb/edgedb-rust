use std::cmp::{min, max};
use std::convert::TryInto;
use std::fmt;
use std::future::{Future};
use std::io::{Cursor};
use std::pin::Pin;
use std::slice;
use std::str;
use std::task::{Poll, Context};

use async_std::io::Read as AsyncRead;
use async_std::stream::{Stream, StreamExt};
use bytes::{Bytes, BytesMut, BufMut};
use futures_util::io::ReadHalf;
use tls_api::TlsStream;

use edgedb_errors::{ClientConnectionError, ClientConnectionEosError};
use edgedb_errors::{Error, ErrorKind};
use edgedb_errors::{ProtocolOutOfOrderError, ProtocolEncodingError};
use edgedb_protocol::encoding::Input;
use edgedb_protocol::features::ProtocolVersion;
use edgedb_protocol::server_message::{ReadyForCommand, TransactionState};
use edgedb_protocol::server_message::{ServerMessage, ErrorResponse};
use edgedb_protocol::{QueryResult};

use crate::client;


const BUFFER_SIZE: usize = 8192;
const MAX_BUFFER: usize = 1_048_576;


struct PartialDebug<V>(V);

pub struct Reader<'a> {
    pub(crate) proto: &'a ProtocolVersion,
    pub(crate) stream: &'a mut ReadHalf<TlsStream>,
    pub(crate) buf: &'a mut BytesMut,
    pub(crate) transaction_state: &'a mut TransactionState,
}

pub struct MessageFuture<'a, 'r: 'a> {
    reader: &'a mut Reader<'r>,
}

// Note: query response expects query *followed by* Sync messsage
pub struct QueryResponse<'a, T: QueryResult> {
    pub(crate) seq: client::Sequence<'a>,
    pub(crate) complete: bool,
    pub(crate) error: Option<ErrorResponse>,
    pub(crate) buffer: Vec<Bytes>,
    pub(crate) state: T::State,
}

impl<T: QueryResult> Unpin for QueryResponse<'_, T> {}


impl<V: fmt::Debug> fmt::Display for PartialDebug<V> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use std::io::Write;

        let mut buf = [0u8; 32];
        let mut cur = Cursor::new(&mut buf[..]);
        // Suppress error, in case buffer is overflown
        write!(&mut cur, "{:?}", self.0).ok();
        let end = cur.position() as usize;
        if end >= buf.len() {
            buf[buf.len()-3] = b'.';
            buf[buf.len()-2] = b'.';
            buf[buf.len()-1] = b'.';
        }
        fmt::Write::write_str(f, str::from_utf8(&buf[..end]).unwrap())
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
    pub async fn wait_ready(&mut self) -> Result<(), Error> {
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
        -> Poll<Result<ServerMessage, Error>>
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
                // this is safe because the underlying TlsStream always
                // initializes read bytes
                let chunk = buf.chunk_mut();
                let dest: &mut [u8] = slice::from_raw_parts_mut(
                    chunk.as_mut_ptr(), chunk.len());
                match Pin::new(&mut *stream).poll_read(cx, dest) {
                    Poll::Ready(Ok(0)) => {
                        return Poll::Ready(
                            Err(ClientConnectionEosError::build())
                        );
                    }
                    Poll::Ready(Ok(bytes)) => {
                        buf.advance_mut(bytes);
                        continue;
                    }
                    Poll::Ready(Err(e)) => {
                        return Poll::Ready(
                            Err(ClientConnectionError::with_source(e))
                        );
                    }
                    Poll::Pending => return Poll::Pending,
                }
            }
        };
        let frame = buf.split_to(frame_len).freeze();
        let result = ServerMessage::decode(&mut Input::new(
            self.proto.clone(),
            frame,
        )).map_err(ProtocolEncodingError::with_source)?;
        log::debug!(target: "edgedb::incoming::frame",
                    "Frame Contents: {:#?}", result);
        return Poll::Ready(Ok(result));
    }
}

impl Future for MessageFuture<'_, '_> {
    type Output = Result<ServerMessage, Error>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        self.reader.poll_message(cx)
    }
}

impl<T: QueryResult> QueryResponse<'_, T> {
    pub async fn skip_remaining(mut self) -> Result<(), Error> {
        while let Some(_) = self.next().await.transpose()?  {}
        Ok(())
    }
    pub async fn get_completion(mut self) -> Result<Bytes, Error> {
        Ok(self.seq._process_exec().await?)
    }
}

impl<T: QueryResult> Stream for QueryResponse<'_, T> {
    type Item = Result<T, Error>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context)
        -> Poll<Option<Self::Item>>
    {
        assert!(self.seq.active);  // TODO(tailhook) maybe debug_assert
        let QueryResponse {
            ref mut buffer,
            ref mut complete,
            ref mut error,
            ref mut seq,
            ref mut state,
        } = *self;
        while buffer.len() == 0 {
            match seq.reader.poll_message(cx) {
                Poll::Ready(Ok(ServerMessage::Data(data))) if error.is_none()
                => {
                    if *complete {
                        return Poll::Ready(Some(
                            Err(ProtocolOutOfOrderError::with_message(format!(
                                "unsolicited packet: {}", PartialDebug(data))))
                        ));
                    }
                    buffer.extend(data.data.into_iter().rev());
                }
                Poll::Ready(Ok(m @ ServerMessage::CommandComplete(_)))
                if error.is_none()
                => {
                    if *complete {
                        return Poll::Ready(Some(
                            Err(ProtocolOutOfOrderError::with_message(format!(
                                "unsolicited packet: {}", PartialDebug(m))))
                        ));
                    }
                    *complete = true;
                }
                Poll::Ready(Ok(ServerMessage::ReadyForCommand(r))) => {
                    if let Some(error) = error.take() {
                        seq.reader.consume_ready(r);
                        seq.end_clean();
                        return Poll::Ready(Some(Err(error.into())));
                    } else {
                        if !*complete {
                            let pkt = ServerMessage::ReadyForCommand(r);
                            return Poll::Ready(Some(
                                Err(ProtocolOutOfOrderError::with_message(
                                    format!("unsolicited packet: {}",
                                            PartialDebug(pkt))))
                            ));
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
                    return Poll::Ready(Some(
                        Err(ProtocolOutOfOrderError::with_message(format!(
                            "unsolicited packet: {}", PartialDebug(message))))
                    ));
                }
                Poll::Ready(Err(e)) => {
                    return Poll::Ready(Some(Err(e)));
                }
                Poll::Pending => return Poll::Pending,
            }
        }
        let chunk = buffer.pop().unwrap();
        Poll::Ready(Some(T::decode(state, &chunk)
            .map_err(ProtocolEncodingError::with_source)))
    }
}
