/*!
Contains the [Value](crate::value::Value) enum.
*/
use bytes::Bytes;

pub use crate::codec::EnumValue;
use crate::codec::{NamedTupleShape, ObjectShape, ShapeElement};
use crate::common::Cardinality;
use crate::model::{BigInt, ConfigMemory, Decimal, Range, Uuid};
use crate::model::{DateDuration, Json, RelativeDuration};
use crate::model::{Datetime, Duration, LocalDate, LocalDatetime, LocalTime};

#[derive(Clone, Debug, PartialEq)]
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
    Vector(Vec<f32>),
    Enum(EnumValue),
    Range(Range<Box<Value>>),
}

#[derive(Clone, Debug)]
pub struct SparseObject {
    pub(crate) shape: ObjectShape,
    pub(crate) fields: Vec<Option<Option<Value>>>,
}

impl Value {
    pub fn kind(&self) -> &'static str {
        use Value::*;
        match self {
            Array(..) => "array",
            BigInt(..) => "bigint",
            Bool(..) => "bool",
            Bytes(..) => "bytes",
            ConfigMemory(..) => "cfg::memory",
            DateDuration(..) => "cal::date_duration",
            Datetime(..) => "datetime",
            Decimal(..) => "decimal",
            Duration(..) => "duration",
            Enum(..) => "enum",
            Float32(..) => "float32",
            Float64(..) => "float64",
            Int16(..) => "int16",
            Int32(..) => "int32",
            Int64(..) => "int64",
            Json(..) => "json",
            LocalDate(..) => "cal::local_date",
            LocalDatetime(..) => "cal::local_datetime",
            LocalTime(..) => "cal::local_time",
            NamedTuple { .. } => "named_tuple",
            Nothing => "nothing",
            Object { .. } => "object",
            Range { .. } => "range",
            RelativeDuration(..) => "cal::relative_duration",
            Set(..) => "set",
            SparseObject { .. } => "sparse_object",
            Str(..) => "str",
            Tuple(..) => "tuple",
            Uuid(..) => "uuid",
            Vector(..) => "ext::pgvector::vector",
        }
    }
    pub fn empty_tuple() -> Value {
        Value::Tuple(Vec::new())
    }

    pub fn try_from_uuid(input: &str) -> Result<Self, uuid::Error> {
        Ok(Self::Uuid(Uuid::parse_str(input)?))
    }
}

impl SparseObject {
    /// Create a new sparse object from key-value pairs
    ///
    /// Note: this method has two limitations:
    /// 1. Shape created uses `AtMostOne` cardinality for all the elements.
    /// 2. There are no extra shape elements
    /// Both of these are irrelevant when serializing the object.
    pub fn from_pairs<N: ToString, V: Into<Option<Value>>>(
        iter: impl IntoIterator<Item = (N, V)>,
    ) -> SparseObject {
        let mut elements = Vec::new();
        let mut fields = Vec::new();
        for (key, val) in iter.into_iter() {
            elements.push(ShapeElement {
                flag_implicit: false,
                flag_link_property: false,
                flag_link: false,
                cardinality: Some(Cardinality::AtMostOne),
                name: key.to_string(),
            });
            fields.push(Some(val.into()));
        }
        SparseObject {
            shape: ObjectShape::new(elements),
            fields,
        }
    }
    /// Create an empty sparse object
    pub fn empty() -> SparseObject {
        SparseObject {
            shape: ObjectShape::new(Vec::new()),
            fields: Vec::new(),
        }
    }
    pub fn pairs(&self) -> impl Iterator<Item = (&str, Option<&Value>)> {
        self.shape
            .0
            .elements
            .iter()
            .zip(&self.fields)
            .filter_map(|(el, opt)| opt.as_ref().map(|opt| (&*el.name, opt.as_ref())))
    }
}

impl PartialEq for SparseObject {
    fn eq(&self, other: &SparseObject) -> bool {
        let mut num = 0;
        let o = &other.shape.0.elements;
        for (el, value) in self.shape.0.elements.iter().zip(&self.fields) {
            if let Some(value) = value {
                num += 1;
                if let Some(pos) = o.iter().position(|e| e.name == el.name) {
                    if other.fields[pos].as_ref() != Some(value) {
                        return false;
                    }
                }
            }
        }
        let other_num = other.fields.iter().filter(|e| e.is_some()).count();
        num == other_num
    }
}

impl From<String> for Value {
    fn from(s: String) -> Value {
        Value::Str(s)
    }
}

impl From<&str> for Value {
    fn from(s: &str) -> Value {
        Value::Str(s.to_string())
    }
}

impl From<bool> for Value {
    fn from(b: bool) -> Value {
        Value::Bool(b)
    }
}

impl From<i16> for Value {
    fn from(s: i16) -> Value {
        Value::Int16(s)
    }
}

impl From<i32> for Value {
    fn from(s: i32) -> Value {
        Value::Int32(s)
    }
}

impl From<i64> for Value {
    fn from(s: i64) -> Value {
        Value::Int64(s)
    }
}

impl From<f32> for Value {
    fn from(num: f32) -> Value {
        Value::Float32(num)
    }
}

impl From<f64> for Value {
    fn from(num: f64) -> Value {
        Value::Float64(num)
    }
}

impl From<BigInt> for Value {
    fn from(model: BigInt) -> Value {
        Value::BigInt(model)
    }
}

impl From<Decimal> for Value {
    fn from(v: Decimal) -> Value {
        Value::Decimal(v)
    }
}

impl From<Uuid> for Value {
    fn from(v: Uuid) -> Value {
        Value::Uuid(v)
    }
}

impl From<Json> for Value {
    fn from(v: Json) -> Value {
        Value::Json(v)
    }
}

impl From<Duration> for Value {
    fn from(v: Duration) -> Value {
        Value::Duration(v)
    }
}

impl From<Datetime> for Value {
    fn from(v: Datetime) -> Value {
        Value::Datetime(v)
    }
}

impl From<LocalDate> for Value {
    fn from(v: LocalDate) -> Value {
        Value::LocalDate(v)
    }
}

impl From<LocalDatetime> for Value {
    fn from(v: LocalDatetime) -> Value {
        Value::LocalDatetime(v)
    }
}
