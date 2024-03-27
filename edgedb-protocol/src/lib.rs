/*!
([Website reference](https://www.edgedb.com/docs/reference/protocol/index)) The EdgeDB protocol for Edgedb-Rust.

EdgeDB types used for data modeling can be seen on the [model](crate::model) crate, in which the [Value](crate::value::Value)
enum provides the quickest overview of all the possible types encountered using the client. Many of the variants hold Rust
standard library types while others contain types defined in this protocol. Some types such as [Duration](crate::model::Duration)
appear to be standard library types but are unique to the EdgeDB protocol.

Other parts of this crate pertain to the rest of the EdgeDB protocol (e.g. client + server message formats), plus various traits
for working with the client such as:

* [QueryArg](crate::query_arg::QueryArg): a single argument for a query
* [QueryArgs](crate::query_arg::QueryArgs): a tuple of query arguments
* [Queryable](crate::queryable::Queryable): for the Queryable derive macro
* [QueryResult](crate::query_result::QueryResult): single result from a query (scalars and tuples)

The Value enum:

```rust,ignore
pub enum Value {
    Nothing,
    Uuid(Uuid),
    Str(String),
    Bytes(Bytes),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Float32(f32),
    Float64(f64),
    BigInt(BigInt),
    ConfigMemory(ConfigMemory),
    Decimal(Decimal),
    Bool(bool),
    Datetime(Datetime),
    LocalDatetime(LocalDatetime),
    LocalDate(LocalDate),
    LocalTime(LocalTime),
    Duration(Duration),
    RelativeDuration(RelativeDuration),
    DateDuration(DateDuration),
    Json(Json),
    Set(Vec<Value>),
    Object {
        shape: ObjectShape,
        fields: Vec<Option<Value>>,
    },
    SparseObject(SparseObject),
    Tuple(Vec<Value>),
    NamedTuple {
        shape: NamedTupleShape,
        fields: Vec<Value>,
    },
    Array(Vec<Value>),
    Enum(EnumValue),
    Range(Range<Box<Value>>),
}
```
*/

mod query_result; // sealed trait should remain non-public

pub mod encoding;
pub mod common;
pub mod features;
pub mod serialization;
pub mod client_message;
pub mod server_message;
pub mod errors;
pub mod error_response;
pub mod descriptors;
pub mod value;
pub mod codec;
pub mod queryable;
#[macro_use]
pub mod query_arg;
pub mod model;

pub use query_result::QueryResult;
