use std::collections::HashMap;
use std::u32;
use std::u16;
use std::str;
use std::convert::TryFrom;
use std::io::Cursor;

use bytes::{Bytes, BytesMut, BufMut, Buf};
use snafu::{Snafu, Backtrace, ResultExt, OptionExt, ensure};


#[derive(Snafu, Debug)]
pub enum DecodeError {
    #[snafu(display("unexpected end of frame"))]
    Underflow { backtrace: Backtrace },
    #[snafu(display("invalid utf8 when decoding string: {}", source))]
    InvalidUtf8 { backtrace: Backtrace, source: str::Utf8Error },
    #[doc(hidden)]
    __NonExhaustive1,
}

#[derive(Snafu, Debug)]
pub enum EncodeError {
    #[snafu(display("message doesn't fit 4GiB"))]
    MessageTooLong { backtrace: Backtrace },
    #[snafu(display("string is larger than 64KiB"))]
    StringTooLong { backtrace: Backtrace },
    #[snafu(display("more than 64Ki extensions"))]
    TooManyExtensions { backtrace: Backtrace },
    #[snafu(display("more than 64Ki headers"))]
    TooManyHeaders { backtrace: Backtrace },
    #[snafu(display("more than 64Ki params"))]
    TooManyParams { backtrace: Backtrace },
    #[snafu(display("more than 64Ki attributes"))]
    TooManyAttributes { backtrace: Backtrace },
    #[snafu(display("unknown message types can't be encoded"))]
    UnknownMessageCantBeEncoded { backtrace: Backtrace },
    #[doc(hidden)]
    __NonExhaustive2,
}


// TODO(tailhook) non-exhaustive
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Message {
    ClientHandshake(ClientHandshake),
    ServerHandshake(ServerHandshake),
    UnknownMessage(u8, Bytes),
    ErrorResponse(ErrorResponse),
    #[doc(hidden)]
    __NonExhaustive,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientHandshake {
    pub major_ver: u16,
    pub minor_ver: u16,
    pub params: HashMap<String, String>,
    pub extensions: HashMap<String, Headers>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorSeverity {
    Error,
    Fatal,
    Panic,
    Unknown(u8),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ErrorResponse {
    pub severity: ErrorSeverity,
    pub code: u32,
    pub message: String,
    pub headers: Headers,
}

pub type Headers = HashMap<u16, Bytes>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerHandshake {
    pub major_ver: u16,
    pub minor_ver: u16,
    pub extensions: HashMap<String, Headers>,
}

trait Encode {
    fn encode(&self, buf: &mut BytesMut)
        -> Result<(), EncodeError>;
}

trait Decode: Sized {
    fn decode(buf: &mut Cursor<Bytes>)
        -> Result<Self, DecodeError>;
}

fn encode<T: Encode>(buf: &mut BytesMut, code: u8, msg: &T)
    -> Result<(), EncodeError>
{
    buf.reserve(5);
    buf.put_u8(code);
    let base = buf.len();
    buf.extend_from_slice(&[0; 4]);

    msg.encode(buf)?;

    let size = u32::try_from(buf.len() - base).ok().context(MessageTooLong)?;
    buf[base..base+4].copy_from_slice(&size.to_be_bytes()[..]);
    Ok(())
}

impl Message {
    pub fn encode(&self, buf: &mut BytesMut) -> Result<(), EncodeError> {
        use Message::*;
        match self {
            ClientHandshake(h) => encode(buf, 0x56, h),
            ServerHandshake(h) => encode(buf, 0x76, h),
            ErrorResponse(h) => encode(buf, 0x45, h),

            UnknownMessage(_, _) => UnknownMessageCantBeEncoded.fail()?,

            // TODO(tailhook) maybe return error ?
            __NonExhaustive => panic!("Invalid Message"),
        }
    }
    /// Decode exactly one frame from the buffer
    ///
    /// This expect full frame already be in the buffer. It can return
    /// arbitrary error or be silent if message is only partially present
    /// in the buffer or if extra data present.
    pub fn decode(buf: &Bytes) -> Result<Message, DecodeError> {
        use self::Message as M;
        let mut data = Cursor::new(buf.slice_from(5));
        match buf[0] {
            0x56 => ClientHandshake::decode(&mut data).map(M::ClientHandshake),
            0x76 => ServerHandshake::decode(&mut data).map(M::ServerHandshake),
            0x45 => ErrorResponse::decode(&mut data).map(M::ErrorResponse),
            code => Ok(M::UnknownMessage(code, data.into_inner())),
        }
    }
}

impl Encode for ClientHandshake {
    fn encode(&self, buf: &mut BytesMut)
        -> Result<(), EncodeError>
    {
        buf.reserve(8);
        buf.put_u16_be(self.major_ver);
        buf.put_u16_be(self.minor_ver);
        buf.put_u16_be(u16::try_from(self.params.len()).ok()
            .context(TooManyParams)?);
        for (k, v) in &self.params {
            k.encode(buf)?;
            v.encode(buf)?;
        }
        buf.reserve(2);
        buf.put_u16_be(u16::try_from(self.extensions.len()).ok()
            .context(TooManyExtensions)?);
        for (name, headers) in &self.extensions {
            name.encode(buf)?;
            buf.reserve(2);
            buf.put_u16_be(u16::try_from(headers.len()).ok()
                .context(TooManyHeaders)?);
            for (&name, value) in headers {
                buf.reserve(2);
                buf.put_u16_be(name);
                value.encode(buf)?;
            }
        }
        Ok(())
    }
}

impl Decode for ClientHandshake {
    fn decode(buf: &mut Cursor<Bytes>) -> Result<Self, DecodeError> {
        ensure!(buf.remaining() >= 8, Underflow);
        let major_ver = buf.get_u16_be();
        let minor_ver = buf.get_u16_be();
        let num_params = buf.get_u16_be();
        let mut params = HashMap::new();
        for _ in 0..num_params {
            params.insert(String::decode(buf)?, String::decode(buf)?);
        }

        ensure!(buf.remaining() >= 2, Underflow);
        let num_ext = buf.get_u16_be();
        let mut extensions = HashMap::new();
        for _ in 0..num_ext {
            let name = String::decode(buf)?;
            ensure!(buf.remaining() >= 2, Underflow);
            let num_headers = buf.get_u16_be();
            let mut headers = HashMap::new();
            for _ in 0..num_headers {
                ensure!(buf.remaining() >= 4, Underflow);
                headers.insert(buf.get_u16_be(), Bytes::decode(buf)?);
            }
            extensions.insert(name, headers);
        }
        Ok(ClientHandshake {
            major_ver, minor_ver, params, extensions,
        })
    }
}

impl Encode for ServerHandshake {
    fn encode(&self, buf: &mut BytesMut)
        -> Result<(), EncodeError>
    {
        buf.reserve(6);
        buf.put_u16_be(self.major_ver);
        buf.put_u16_be(self.minor_ver);
        buf.put_u16_be(u16::try_from(self.extensions.len()).ok()
            .context(TooManyExtensions)?);
        for (name, headers) in &self.extensions {
            name.encode(buf)?;
            buf.reserve(2);
            buf.put_u16_be(u16::try_from(headers.len()).ok()
                .context(TooManyHeaders)?);
            for (&name, value) in headers {
                buf.reserve(2);
                buf.put_u16_be(name);
                value.encode(buf)?;
            }
        }
        Ok(())
    }
}

impl Decode for ServerHandshake {
    fn decode(buf: &mut Cursor<Bytes>) -> Result<Self, DecodeError> {
        ensure!(buf.remaining() >= 6, Underflow);
        let major_ver = buf.get_u16_be();
        let minor_ver = buf.get_u16_be();
        let num_ext = buf.get_u16_be();
        let mut extensions = HashMap::new();
        for _ in 0..num_ext {
            let name = String::decode(buf)?;
            ensure!(buf.remaining() >= 2, Underflow);
            let num_headers = buf.get_u16_be();
            let mut headers = HashMap::new();
            for _ in 0..num_headers {
                headers.insert(buf.get_u16_be(), Bytes::decode(buf)?);
            }
            extensions.insert(name, headers);
        }
        Ok(ServerHandshake {
            major_ver, minor_ver, extensions,
        })
    }
}

impl Encode for String {
    fn encode(&self, buf: &mut BytesMut)
        -> Result<(), EncodeError>
    {
        buf.reserve(2 + self.len());
        buf.put_u32_be(u32::try_from(self.len()).ok()
            .context(StringTooLong)?);
        buf.extend(self.as_bytes());
        Ok(())
    }
}

impl Encode for Bytes {
    fn encode(&self, buf: &mut BytesMut)
        -> Result<(), EncodeError>
    {
        buf.reserve(2 + self.len());
        buf.put_u32_be(u32::try_from(self.len()).ok()
            .context(StringTooLong)?);
        buf.extend(&self[..]);
        Ok(())
    }
}

impl Encode for ErrorResponse {
    fn encode(&self, buf: &mut BytesMut)
        -> Result<(), EncodeError>
    {
        buf.reserve(11);
        buf.put_u8(self.severity.to_u8());
        buf.put_u32_be(self.code);
        self.message.encode(buf)?;
        buf.reserve(2);
        buf.put_u16_be(u16::try_from(self.headers.len()).ok()
            .context(TooManyHeaders)?);
        for (&name, value) in &self.headers {
            buf.reserve(2);
            buf.put_u16_be(name);
            value.encode(buf)?;
        }
        Ok(())
    }
}

impl Decode for ErrorResponse {
    fn decode(buf: &mut Cursor<Bytes>) -> Result<ErrorResponse, DecodeError> {
        ensure!(buf.remaining() >= 11, Underflow);
        let severity = ErrorSeverity::from_u8(buf.get_u8());
        let code = buf.get_u32_be();
        let message = String::decode(buf)?;
        ensure!(buf.remaining() >= 2, Underflow);
        let num_headers = buf.get_u16_be();
        let mut headers = HashMap::new();
        for _ in 0..num_headers {
            ensure!(buf.remaining() >= 4, Underflow);
            headers.insert(buf.get_u16_be(), Bytes::decode(buf)?);
        }
        return Ok(ErrorResponse {
            severity, code, message, headers,
        })
    }
}

impl Decode for String {
    fn decode(buf: &mut Cursor<Bytes>) -> Result<Self, DecodeError> {
        ensure!(buf.remaining() >= 4, Underflow);
        let len = buf.get_u32_be() as usize;
        // TODO(tailhook) ensure size < i32::MAX
        ensure!(buf.remaining() >= len, Underflow);
        let buf_pos = buf.position() as usize;
        let result = str::from_utf8(&buf.bytes()[..len])
            .map(String::from)
            .context(InvalidUtf8);
        buf.advance(len);
        return result;
    }
}

impl Decode for Bytes {
    fn decode(buf: &mut Cursor<Bytes>) -> Result<Self, DecodeError> {
        ensure!(buf.remaining() >= 4, Underflow);
        let len = buf.get_u32_be() as usize;
        // TODO(tailhook) ensure size < i32::MAX
        ensure!(buf.remaining() >= len, Underflow);
        let buf_pos = buf.position() as usize;
        let result = buf.get_ref().slice(buf_pos, buf_pos + len);
        buf.advance(len);
        Ok(result)
    }
}

impl ErrorSeverity {
    fn from_u8(code: u8) -> ErrorSeverity {
        use ErrorSeverity::*;
        match code {
            120 => Error,
            200 => Fatal,
            255 => Panic,
            _ => Unknown(code),
        }
    }
    fn to_u8(&self) -> u8 {
        use ErrorSeverity::*;
        match *self {
            Error => 120,
            Fatal => 200,
            Panic => 255,
            Unknown(code) => code,
        }
    }
}
