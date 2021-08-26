use std::sync::Arc;

use bytes::{BytesMut, BufMut};

use edgedb_errors::{Error, ErrorKind};
use edgedb_errors::{ClientEncodingError, ProtocolError};

use crate::codec::{Codec, build_codec};
use crate::descriptors::Descriptor;
use crate::descriptors::TypePos;
use crate::features::ProtocolVersion;
use crate::value::Value;


pub struct Encoder<'a> {
    ctx: &'a DescriptorContext<'a>,
    buf: &'a mut BytesMut,
}

pub trait QueryArg: Sized {
}

pub trait QueryArgs: Sized {
    fn encode(&self, encoder: &mut Encoder)
        -> Result<(), Error>;
}

pub struct DescriptorContext<'a> {
    #[allow(dead_code)]
    pub(crate) proto: &'a ProtocolVersion,
    pub(crate) root_pos: TypePos,
    pub(crate) descriptors: &'a [Descriptor],
}

impl<'a> Encoder<'a> {
    pub fn new(ctx: &'a DescriptorContext<'a>, buf: &'a mut BytesMut)
        -> Encoder<'a>
    {
        Encoder { ctx, buf }
    }
}

impl DescriptorContext<'_> {
    pub fn build_codec(&self) -> Result<Arc<dyn Codec>, Error> {
        build_codec(Some(self.root_pos), self.descriptors)
        .map_err(|e| ProtocolError::with_source(e)
            .context("error decoding input codec"))
    }
}

impl QueryArgs for () {
    fn encode(&self, enc: &mut Encoder)
        -> Result<(), Error>
    {
        enc.buf.reserve(4);
        enc.buf.put_u32(0);
        Ok(())
    }
}

impl QueryArgs for Value {
    fn encode(&self, enc: &mut Encoder)
        -> Result<(), Error>
    {
        let codec = enc.ctx.build_codec()?;
        codec.encode(&mut enc.buf, self)
            .map_err(ClientEncodingError::with_source)
    }
}
