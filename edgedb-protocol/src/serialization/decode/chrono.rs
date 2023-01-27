use chrono::{DateTime, NaiveDateTime, NaiveDate, NaiveTime, Utc};
use crate::errors::DecodeError;
use crate::serialization::decode::raw_scalar::RawCodec;


impl<'t> RawCodec<'t> for DateTime<Utc> {
    fn decode(buf: &[u8]) -> Result<Self, DecodeError> {
        crate::model::Datetime::decode(buf).map(Into::into)
    }
}

impl<'t> RawCodec<'t> for NaiveDateTime {
    fn decode(buf: &[u8]) -> Result<Self, DecodeError> {
        crate::model::LocalDatetime::decode(buf).map(Into::into)
    }
}

impl<'t> RawCodec<'t> for NaiveDate {
    fn decode(buf: &[u8]) -> Result<Self, DecodeError> {
        crate::model::LocalDate::decode(buf).map(Into::into)
    }
}

impl<'t> RawCodec<'t> for NaiveTime {
    fn decode(buf: &[u8]) -> Result<Self, DecodeError> {
        crate::model::LocalTime::decode(buf).map(Into::into)
    }
}
