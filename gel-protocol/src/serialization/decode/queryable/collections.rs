use crate::descriptors::{Descriptor, TypePos};
use crate::errors::DecodeError;
use crate::queryable::DescriptorMismatch;
use crate::queryable::{Decoder, DescriptorContext, Queryable};
use crate::serialization::decode::DecodeArrayLike;
use std::iter::FromIterator;

impl<T: Queryable> Queryable for Option<T> {
    type Args = T::Args;

    fn decode(decoder: &Decoder, args: &Self::Args, buf: &[u8]) -> Result<Self, DecodeError> {
        Ok(Some(T::decode(decoder, args, buf)?))
    }

    fn decode_optional(
        decoder: &Decoder,
        args: &Self::Args,
        buf: Option<&[u8]>,
    ) -> Result<Self, DecodeError> {
        buf.map(|buf| T::decode(decoder, args, buf)).transpose()
    }

    fn check_descriptor(
        ctx: &DescriptorContext,
        type_pos: TypePos,
    ) -> Result<T::Args, DescriptorMismatch> {
        T::check_descriptor(ctx, type_pos)
    }
}

struct Collection<T>(T);

impl<T: IntoIterator + FromIterator<<T as IntoIterator>::Item>> Collection<T>
where
    <T as IntoIterator>::Item: Queryable,
{
    fn decode(
        decoder: &Decoder,
        args: &<<T as IntoIterator>::Item as Queryable>::Args,
        buf: &[u8],
    ) -> Result<T, DecodeError> {
        let elements = DecodeArrayLike::new_collection(buf)?;
        let elements = elements.map(|e| <T as IntoIterator>::Item::decode(decoder, args, e?));
        elements.collect::<Result<T, DecodeError>>()
    }

    fn decode_optional(
        decoder: &Decoder,
        args: &<<T as IntoIterator>::Item as Queryable>::Args,
        buf: Option<&[u8]>,
    ) -> Result<T, DecodeError> {
        match buf {
            Some(buf) => Self::decode(decoder, args, buf),
            None => Ok(T::from_iter(std::iter::empty())),
        }
    }

    fn check_descriptor(
        ctx: &DescriptorContext,
        type_pos: TypePos,
    ) -> Result<<<T as IntoIterator>::Item as Queryable>::Args, DescriptorMismatch> {
        let desc = ctx.get(type_pos)?;
        let element_type_pos = match desc {
            Descriptor::Set(desc) => desc.type_pos,
            Descriptor::Array(desc) => desc.type_pos,
            _ => return Err(ctx.wrong_type(desc, "array or set")),
        };
        <T as IntoIterator>::Item::check_descriptor(ctx, element_type_pos)
    }
}

impl<T: Queryable> Queryable for Vec<T> {
    type Args = T::Args;

    fn decode(decoder: &Decoder, args: &T::Args, buf: &[u8]) -> Result<Self, DecodeError> {
        Collection::<Vec<T>>::decode(decoder, args, buf)
    }

    fn decode_optional(
        decoder: &Decoder,
        args: &T::Args,
        buf: Option<&[u8]>,
    ) -> Result<Self, DecodeError> {
        Collection::<Vec<T>>::decode_optional(decoder, args, buf)
    }

    fn check_descriptor(
        ctx: &DescriptorContext,
        type_pos: TypePos,
    ) -> Result<T::Args, DescriptorMismatch> {
        Collection::<Vec<T>>::check_descriptor(ctx, type_pos)
    }
}
