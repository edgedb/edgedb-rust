use bigdecimal::BigDecimal;
use chrono::format::{Item, Numeric, Pad, Fixed};
use chrono::{NaiveDateTime, NaiveDate, NaiveTime};
use humantime::format_rfc3339;
use lazy_static::lazy_static;
use num_bigint::BigInt;
use std::convert::TryInto;

use edgedb_protocol::value::Value;
use crate::print::formatter::Formatter;

static DATETIME_FORMAT: &[Item<'static>] = &[
    Item::Numeric(Numeric::Year, Pad::Zero),
    Item::Literal("-"),
    Item::Numeric(Numeric::Month, Pad::Zero),
    Item::Literal("-"),
    Item::Numeric(Numeric::Day, Pad::Zero),
    Item::Literal("T"),
    Item::Numeric(Numeric::Hour, Pad::Zero),
    Item::Literal(":"),
    Item::Numeric(Numeric::Minute, Pad::Zero),
    Item::Literal(":"),
    Item::Numeric(Numeric::Second, Pad::Zero),
    Item::Fixed(Fixed::Nanosecond),
];
static DATE_FORMAT: &[Item<'static>] = &[
    Item::Numeric(Numeric::Year, Pad::Zero),
    Item::Literal("-"),
    Item::Numeric(Numeric::Month, Pad::Zero),
    Item::Literal("-"),
    Item::Numeric(Numeric::Day, Pad::Zero),
];
static TIME_FORMAT: &[Item<'static>] = &[
    Item::Numeric(Numeric::Hour, Pad::Zero),
    Item::Literal(":"),
    Item::Numeric(Numeric::Minute, Pad::Zero),
    Item::Literal(":"),
    Item::Numeric(Numeric::Second, Pad::Zero),
    Item::Fixed(Fixed::Nanosecond),
];

pub(in crate::print) trait FormatExt {
    fn format<F: Formatter>(&self, prn: &mut F) -> Result<(), F::Error>;
}

impl FormatExt for Value {
    fn format<F: Formatter>(&self, prn: &mut F) -> Result<(), F::Error> {
        use Value as V;
        match self {
            V::Nothing => prn.const_scalar("Nothing"),
            V::Uuid(u) => prn.const_scalar(u),
            V::Str(s) => prn.const_scalar(format_args!("{:?}", s)),
            V::Bytes(b) => prn.const_scalar(format_args!("{:?}", b)),
            V::Int16(v) => prn.const_scalar(v),
            V::Int32(v) => prn.const_scalar(v),
            V::Int64(v) => prn.const_scalar(v),
            V::Float32(v) => prn.const_scalar(v),
            V::Float64(v) => prn.const_scalar(v),
            V::BigInt(v) => prn.const_scalar::<BigInt>(v.into()),
            V::Decimal(v) => prn.const_scalar::<BigDecimal>(v.into()),
            V::Bool(v) => prn.const_scalar(v),
            V::Datetime(t) => prn.typed("datetime", format_rfc3339(*t)),
            V::LocalDatetime(dt) => {
                match TryInto::<NaiveDateTime>::try_into(dt) {
                    Ok(naive) => prn.typed("cal::local_datetime",
                        naive.format_with_items(DATETIME_FORMAT.iter())),
                    Err(e) => prn.error("cal::local_datetime", e),
                }
            }
            V::LocalDate(d) => {
                match TryInto::<NaiveDate>::try_into(d) {
                    Ok(naive) => prn.typed("cal::local_date",
                        naive.format_with_items(DATE_FORMAT.iter())),
                    Err(e) => prn.error("cal::local_date", e),
                }
            }
            V::LocalTime(t) => {
                prn.typed("cal::local_time",
                    Into::<NaiveTime>::into(t)
                        .format_with_items(TIME_FORMAT.iter()))
            }
            V::Duration(d) => {
                // TODO(tailhook) implement more DB-like duration display
                prn.const_scalar(format_args!("{}{:?}",
                    if d.is_negative() { "-" } else { "" }, d.abs_duration()))
            }
            V::Json(d) => prn.const_scalar(format!("{:?}", d)),
            V::Set(..) => todo!(),
            V::Object { .. } => todo!(),
            V::Tuple(..) => todo!(),
            V::NamedTuple { .. } => todo!(),
            V::Array(..) => todo!(),
            V::Enum(v) => prn.const_scalar(&**v),
        }
    }
}
