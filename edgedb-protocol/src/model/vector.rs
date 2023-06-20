use std::ops::{Deref, DerefMut};

use bytes::Buf;
use snafu::ensure;

use crate::codec;
use crate::descriptors::{TypePos};
use crate::errors::{self, DecodeError};
use crate::queryable::{DescriptorMismatch};
use crate::queryable::{Queryable, Decoder, DescriptorContext};
use crate::serialization::decode::queryable::scalars::check_scalar;

/// A structure that represents `ext::pgvector::vector`
#[derive(Debug, PartialEq, Clone)]
pub struct Vector(pub Vec<f32>);

impl Deref for Vector {
    type Target = Vec<f32>;
    fn deref(&self) -> &Vec<f32> {
        &self.0
    }
}

impl DerefMut for Vector {
    fn deref_mut(&mut self) -> &mut Vec<f32> {
        &mut self.0
    }
}

impl Queryable for Vector {
    fn decode(_decoder: &Decoder, mut buf: &[u8]) -> Result<Self, DecodeError> {
        ensure!(buf.remaining() >= 4, errors::Underflow);
        let length = buf.get_u16() as usize;
        let _reserved = buf.get_u16();
        ensure!(buf.remaining() >= length*4, errors::Underflow);
        let vec = (0..length)
            .map(|_| f32::from_bits(buf.get_u32()))
            .collect();
        Ok(Vector(vec))
    }

    fn check_descriptor(ctx: &DescriptorContext, type_pos: TypePos)
        -> Result<(), DescriptorMismatch>
    {
        check_scalar(ctx, type_pos,
                     codec::PGVECTOR_VECTOR, "ext::pgvector::vector")
    }
}
