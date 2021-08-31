use std::convert::TryFrom;
use std::sync::Arc;

use bytes::{BytesMut, BufMut};
use snafu::OptionExt;
use uuid::Uuid;

use edgedb_errors::{Error, ErrorKind};
use edgedb_errors::{ClientEncodingError, ProtocolError, DescriptorMismatch};

use crate::codec::{Codec, build_codec};
use crate::descriptors::Descriptor;
use crate::descriptors::TypePos;
use crate::errors;
use crate::features::ProtocolVersion;
use crate::value::Value;


pub struct Encoder<'a> {
    pub(crate) ctx: &'a DescriptorContext<'a>,
    pub(crate) buf: &'a mut BytesMut,
}

pub trait QueryArg: Sized {
    fn encode_slot(&self, encoder: &mut Encoder)
        -> Result<(), Error>;
    fn check_descriptor(ctx: &DescriptorContext, pos: TypePos)
        -> Result<(), Error>;
}

pub trait ScalarArg: Sized {
    fn encode(&self, encoder: &mut Encoder)
        -> Result<(), Error>;
    fn check_descriptor(ctx: &DescriptorContext, pos: TypePos)
        -> Result<(), Error>;
}

pub trait QueryArgs: Sized {
    fn encode(&self, encoder: &mut Encoder)
        -> Result<(), Error>;
}

pub struct DescriptorContext<'a> {
    #[allow(dead_code)]
    pub(crate) proto: &'a ProtocolVersion,
    pub(crate) root_pos: Option<TypePos>,
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
    pub fn get(&self, type_pos: TypePos)
        -> Result<&Descriptor, Error>
    {
        self.descriptors.get(type_pos.0 as usize)
            .ok_or_else(|| ProtocolError::with_message(
                "invalid type descriptor"))
    }
    pub fn build_codec(&self) -> Result<Arc<dyn Codec>, Error> {
        build_codec(self.root_pos, self.descriptors)
        .map_err(|e| ProtocolError::with_source(e)
            .context("error decoding input codec"))
    }
    pub fn wrong_type(&self, descriptor: &Descriptor, expected: &str) -> Error
    {
        DescriptorMismatch::with_message(format!(
            "unexpected type {:?}, expected {}",
            descriptor, expected))
    }
    pub fn field_number(&self, expected: usize, unexpected: usize)
        -> Error
    {
        DescriptorMismatch::with_message(format!(
            "expected {} fields, got {}",
            expected, unexpected))
    }
}

impl<T: ScalarArg> ScalarArg for &T {
    fn encode(&self, encoder: &mut Encoder)
        -> Result<(), Error>
    {
        (*self).encode(encoder)
    }

    fn check_descriptor(ctx: &DescriptorContext, pos: TypePos)
        -> Result<(), Error>
    {
        T::check_descriptor(ctx, pos)
    }
}

impl QueryArgs for () {
    fn encode(&self, enc: &mut Encoder)
        -> Result<(), Error>
    {
        if enc.ctx.root_pos.is_some() {
            if enc.ctx.proto.is_at_most(0, 11) {
                let root = enc.ctx.root_pos.and_then(|p| enc.ctx.get(p).ok());
                match root {
                    Some(Descriptor::Tuple(t))
                    if t.id == Uuid::from_u128(0xFF)
                    && t.element_types.is_empty()
                    => {}
                    _ => return Err(DescriptorMismatch::with_message(
                            "query arguments expected")),
                };
            } else {
                return Err(DescriptorMismatch::with_message(
                    "query arguments expected"));
            }
        }
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

impl<T: ScalarArg> QueryArg for T {
    fn encode_slot(&self, enc: &mut Encoder) -> Result<(), Error> {
        let pos = enc.buf.len();
        ScalarArg::encode(self, enc)?;
        let len = enc.buf.len()-pos-4;
        enc.buf[pos..pos+4].copy_from_slice(&i32::try_from(len)
                .ok().context(errors::ElementTooLong)
                .map_err(ClientEncodingError::with_source)?
                .to_be_bytes());
        Ok(())
    }
    fn check_descriptor(ctx: &DescriptorContext, pos: TypePos)
        -> Result<(), Error>
    {
        T::check_descriptor(ctx, pos)
    }
}

impl<T: ScalarArg> QueryArg for Option<T> {
    fn encode_slot(&self, enc: &mut Encoder) -> Result<(), Error> {
        if let Some(val) = self {
            QueryArg::encode_slot(val, enc)
        } else {
            enc.buf.put_i32(-1);
            Ok(())
        }
    }
    fn check_descriptor(ctx: &DescriptorContext, pos: TypePos)
        -> Result<(), Error>
    {
        T::check_descriptor(ctx, pos)
    }
}

macro_rules! implement_tuple {
    ( $count:expr, $($name:ident,)+ ) => {
        impl<$($name:QueryArg),+> QueryArgs for ($($name,)+) {
            fn encode(&self, enc: &mut Encoder)
                -> Result<(), Error>
            {
                #![allow(non_snake_case)]
                let root_pos = enc.ctx.root_pos
                    .ok_or_else(|| DescriptorMismatch::with_message(
                        "provided {} positional arguments, \
                         but no arguments expected by the server"))?;
                let desc = enc.ctx.get(root_pos)?;
                match desc {
                    Descriptor::ObjectShape(desc) => {
                        if desc.elements.len() != $count {
                            return Err(enc.ctx.field_number(
                                $count, desc.elements.len()));
                        }
                        let mut els = desc.elements.iter().enumerate();
                        $(
                            let (idx, el) = els.next().unwrap();
                            if el.name.parse() != Ok(idx) {
                                return Err(DescriptorMismatch::with_message(
                                    format!("expected positional arguments, \
                                             got {} instead of {}",
                                             el.name, idx)));
                            }
                            $name::check_descriptor(enc.ctx, el.type_pos)?;
                        )+
                    }
                    _ => return Err(enc.ctx.wrong_type(desc, "tuple"))
                }

                enc.buf.reserve(4 + 8*$count);
                enc.buf.put_u32($count);
                let ($(ref $name,)+) = self;
                $(
                    enc.buf.reserve(8);
                    enc.buf.put_u32(0);
                    QueryArg::encode_slot($name, enc)?;
                )*
                Ok(())
            }
        }
    }
}

implement_tuple!{1, T0, }
implement_tuple!{2, T0, T1, }
implement_tuple!{3, T0, T1, T2, }
implement_tuple!{4, T0, T1, T2, T3, }
implement_tuple!{5, T0, T1, T2, T3, T4, }
implement_tuple!{6, T0, T1, T2, T3, T4, T5, }
implement_tuple!{7, T0, T1, T2, T3, T4, T5, T6, }
implement_tuple!{8, T0, T1, T2, T3, T4, T5, T6, T7, }
implement_tuple!{9, T0, T1, T2, T3, T4, T5, T6, T7, T8, }
implement_tuple!{10, T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, }
implement_tuple!{11, T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, }
implement_tuple!{12, T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, }
