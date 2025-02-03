use crate::descriptors::{Descriptor, TypePos};
use crate::errors::DecodeError;
use crate::queryable::DescriptorMismatch;
use crate::queryable::{Decoder, DescriptorContext, Queryable};
use crate::serialization::decode::DecodeTupleLike;

macro_rules! implement_tuple {
    ( $count:expr, $(($name:ident, $index:tt),)+ ) => (
        impl<$($name:Queryable),+> Queryable for ($($name,)+) {
            type Args = (
                $(
                    <$name as crate::queryable::Queryable>::Args,
                )+
            );

            fn decode(decoder: &Decoder, args: &Self::Args, buf: &[u8])
                -> Result<Self, DecodeError>
            {
                let mut elements = DecodeTupleLike::new_tuple(buf, $count)?;
                Ok((
                    $(
                        <$name as crate::queryable::Queryable>::
                            decode_optional(decoder, &args.$index, elements.read()?)?,
                    )+
                ))
            }

            fn check_descriptor(ctx: &DescriptorContext, type_pos: TypePos)
            -> Result<Self::Args, DescriptorMismatch>
            {
                let desc = ctx.get(type_pos)?;
                match desc {
                    Descriptor::Tuple(desc) => {
                        if desc.element_types.len() != $count {
                            return Err(ctx.field_number($count, desc.element_types.len()));
                        }
                        let mut element_types = desc.element_types.iter().copied();
                        Ok((
                            $($name::check_descriptor(ctx, element_types.next().unwrap())?,)+
                        ))
                    }
                    _ => Err(ctx.wrong_type(desc, "tuple"))
                }
            }
        }
    )
}

implement_tuple! {
    1,
    (T0, 0),
}
implement_tuple! {
    2,
    (T0, 0),
    (T1, 1),
}
implement_tuple! {
    3,
    (T0, 0),
    (T1, 1),
    (T2, 2),
}
implement_tuple! {
    4,
    (T0, 0),
    (T1, 1),
    (T2, 2),
    (T3, 3),
}
implement_tuple! {
    5,
    (T0, 0),
    (T1, 1),
    (T2, 2),
    (T3, 3),
    (T4, 4),
}
implement_tuple! {
    6,
    (T0, 0),
    (T1, 1),
    (T2, 2),
    (T3, 3),
    (T4, 4),
    (T5, 5),
}
implement_tuple! {
    7,
    (T0, 0),
    (T1, 1),
    (T2, 2),
    (T3, 3),
    (T4, 4),
    (T5, 5),
    (T6, 6),
}
implement_tuple! {
    8,
    (T0, 0),
    (T1, 1),
    (T2, 2),
    (T3, 3),
    (T4, 4),
    (T5, 5),
    (T6, 6),
    (T7, 7),
}
implement_tuple! {
    9,
    (T0, 0),
    (T1, 1),
    (T2, 2),
    (T3, 3),
    (T4, 4),
    (T5, 5),
    (T6, 6),
    (T7, 7),
    (T8, 8),
}
implement_tuple! {
    10,
    (T0, 0),
    (T1, 1),
    (T2, 2),
    (T3, 3),
    (T4, 4),
    (T5, 5),
    (T6, 6),
    (T7, 7),
    (T8, 8),
    (T9, 9),
}
implement_tuple! {
    11,
    (T0, 0),
    (T1, 1),
    (T2, 2),
    (T3, 3),
    (T4, 4),
    (T5, 5),
    (T6, 6),
    (T7, 7),
    (T8, 8),
    (T9, 9),
    (T10, 10),
}
implement_tuple! {
    12,
    (T0, 0),
    (T1, 1),
    (T2, 2),
    (T3, 3),
    (T4, 4),
    (T5, 5),
    (T6, 6),
    (T7, 7),
    (T8, 8),
    (T9, 9),
    (T10, 10),
    (T11, 11),
}
