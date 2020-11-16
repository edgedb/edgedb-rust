use crate::queryable::{Queryable, DescriptorContext, Decoder};
use crate::queryable::{DescriptorMismatch};
use crate::errors::DecodeError;
use crate::descriptors::{Descriptor, TypePos};
use crate::serialization::decode::DecodeTupleLike;

macro_rules! implement_tuple {
    ( $count:expr, $($name:ident,)+ ) => (
        impl<$($name:Queryable),+> Queryable for ($($name,)+) {
            fn decode(decoder: &Decoder, buf: &[u8])
                -> Result<Self, DecodeError>
            {
                let mut elements = DecodeTupleLike::new_tuple(buf, $count)?;
                Ok((
                    $(
                        <$name as crate::queryable::Queryable>::
                            decode_optional(decoder, elements.read()?)?,
                    )+
                ))
            }

            fn check_descriptor(ctx: &DescriptorContext, type_pos: TypePos)
            -> Result<(), DescriptorMismatch>
            {
                let desc = ctx.get(type_pos)?;
                match desc {
                    Descriptor::Tuple(desc) => {
                        if desc.element_types.len() != $count {
                            return Err(ctx.field_number($count, desc.element_types.len()));
                        }
                        let mut element_types = desc.element_types.iter().copied();
                        $($name::check_descriptor(ctx, element_types.next().unwrap())?;)+
                        Ok(())
                    }
                    _ => Err(ctx.wrong_type(desc, "tuple"))
                }
            }
        }
    )
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
