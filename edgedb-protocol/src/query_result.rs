/*!
Contains the [QueryResult](crate::query_result::QueryResult) trait.
*/

use std::sync::Arc;

use bytes::Bytes;

use edgedb_errors::{Error, ErrorKind};
use edgedb_errors::{ProtocolEncodingError, DescriptorMismatch};

use crate::codec::Codec;
use crate::queryable::{Queryable, Decoder, DescriptorContext};
use crate::descriptors::TypePos;
use crate::value::Value;

pub trait Sealed: Sized {}

/// A trait representing single result from a query.
///
/// This is implemented for scalars and tuples. To receive a shape from EdgeDB
/// derive [`Queryable`](Queryable) for a structure. This will automatically
/// implement `QueryResult` for you.
pub trait QueryResult: Sealed {
    type State;
    fn prepare(ctx: &DescriptorContext, root_pos: TypePos)
        -> Result<Self::State, Error>;
    fn decode(state: &mut Self::State, msg: &Bytes)
        -> Result<Self, Error>;
}

impl<T: Queryable> Sealed for T {
}

impl Sealed for Value {
}

impl<T: Queryable> QueryResult for T {
    type State = Decoder;
    fn prepare(ctx: &DescriptorContext, root_pos: TypePos)
        -> Result<Decoder, Error>
    {
        T::check_descriptor(ctx, root_pos)
            .map_err(DescriptorMismatch::with_source)?;
        Ok(Decoder {
            has_implicit_id: ctx.has_implicit_id,
            has_implicit_tid: ctx.has_implicit_tid,
            has_implicit_tname: ctx.has_implicit_tname,
        })
    }
    fn decode(decoder: &mut Decoder, msg: &Bytes)
        -> Result<Self, Error>
    {
        Queryable::decode(decoder, msg)
            .map_err(ProtocolEncodingError::with_source)
    }
}

impl QueryResult for Value {
    type State = Arc<dyn Codec>;
    fn prepare(ctx: &DescriptorContext, root_pos: TypePos)
        -> Result<Arc<dyn Codec>, Error>
    {
        ctx.build_codec(root_pos)
    }
    fn decode(codec: &mut Arc<dyn Codec>, msg: &Bytes)
        -> Result<Self, Error>
    {
        codec.decode(msg)
            .map_err(ProtocolEncodingError::with_source)
    }
}

