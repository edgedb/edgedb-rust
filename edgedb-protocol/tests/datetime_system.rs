use std::convert::{TryFrom, TryInto};
use std::time::{Duration as StdDuration, SystemTime, UNIX_EPOCH};

use bytes::{Buf, BytesMut};
use edgedb_protocol::codec::{self, Codec};
use edgedb_protocol::model::{Datetime, Duration};
use edgedb_protocol::value::Value;
use test_case::test_case;

// ========
// Datetime
// ========
//
// Note: pre-1970 dates have hardcoded unix time and they are below

// Maximum
// -------
#[test_case(
    /*input*/ "9999-12-31T23:59:59.999999499Z",
    // Note: Can't round up here, so   --^
    /*micros*/ 252455615999999999,
    /*formatted*/ "9999-12-31T23:59:59.999999000Z"
    ; "maximum"
)]
// Rounding in Various Ranges >= 1970
// ---------------------------------
#[test_case(
    /*input*/ "1997-07-05T01:02:03.000009500Z",
    /*micros*/ -78620276999990,
    /*formatted*/ "1997-07-05T01:02:03.000010000Z"
    ; "negative postgres timestamp, round up"
)]
#[test_case(
    /*input*/ "1997-07-05T01:02:03.000009500Z",
    /*micros*/ -78620276999990,
    /*formatted*/ "1997-07-05T01:02:03.000010000Z"
    ; "negative postgres timestamp, 9501"
)]
#[test_case(
    /*input*/ "1997-07-05T01:02:03.000009499Z",
    /*micros*/ -78620276999991,
    /*formatted*/ "1997-07-05T01:02:03.000009000Z"
    ; "negative postgres timestamp, 9499"
)]
#[test_case(
    /*input*/ "1997-07-05T01:02:03.000000500Z",
    /*micros*/ -78620277000000,
    /*formatted*/ "1997-07-05T01:02:03Z"
    ; "negative postgres timestamp, round down"
)]
#[test_case(
    /*input*/ "1997-07-05T01:02:03.000000501Z",
    /*micros*/ -78620276999999,
    /*formatted*/ "1997-07-05T01:02:03.000001000Z"
    ; "negative postgres timestamp, 501"
)]
#[test_case(
    /*input*/ "1997-07-05T01:02:03.000000499Z",
    /*micros*/ -78620277000000,
    /*formatted*/ "1997-07-05T01:02:03Z"
    ; "negative postgres timestamp, 499"
)]
#[test_case(
    /*input*/ "1999-12-31T23:59:59.999999500Z",
    /*micros*/ 0,
    /*formatted*/ "2000-01-01T00:00:00Z"
    ; "postgres timestamp to zero"
)]
#[test_case(
    /*input*/ "2014-02-27T00:00:00.000001500Z",
    /*micros*/ 446774400000002,
    /*formatted*/ "2014-02-27T00:00:00.000002000Z"
    ; "positive timestamp, round up"
)]
#[test_case(
    /*input*/ "2014-02-27T00:00:00.000001501Z",
    /*micros*/ 446774400000002,
    /*formatted*/ "2014-02-27T00:00:00.000002000Z"
    ; "positive timestamp, 1501"
)]
#[test_case(
    /*input*/ "2014-02-27T00:00:00.000001499Z",
    /*micros*/ 446774400000001,
    /*formatted*/ "2014-02-27T00:00:00.000001000Z"
    ; "positive timestamp, 1499"
)]
#[test_case(
    /*input*/ "2022-02-24T05:43:03.000002500Z",
    /*micros*/ 698996583000002,
    /*formatted*/ "2022-02-24T05:43:03.000002000Z"
    ; "positive timestamp, round down"
)]
#[test_case(
    /*input*/ "2022-02-24T05:43:03.000002501Z",
    /*micros*/ 698996583000003,
    /*formatted*/ "2022-02-24T05:43:03.000003000Z"
    ; "positive timestamp, 2501"
)]
#[test_case(
    /*input*/ "2022-02-24T05:43:03.000002499Z",
    /*micros*/ 698996583000002,
    /*formatted*/ "2022-02-24T05:43:03.000002000Z"
    ; "positive timestamp, 2499"
)]
fn datetime(input: &str, micros: i64, formatted: &str) {
    let system = humantime::parse_rfc3339(input).unwrap();
    let edgedb: Datetime = system.try_into().unwrap();
    // different format but we assert microseconds anyways
    // assert_eq!(format!("{:?}", edgedb), formatted);

    let mut buf = BytesMut::new();
    let val = Value::Datetime(edgedb.clone());
    codec::Datetime.encode(&mut buf, &val).unwrap();
    let serialized_micros = buf.get_i64();

    assert_eq!(serialized_micros, micros);

    let rev = SystemTime::try_from(edgedb).unwrap();
    assert_eq!(humantime::format_rfc3339(rev).to_string(), formatted);
}

#[test_case(
    /*input: "0001-01-01T00:00:00.000000Z",*/
        StdDuration::new(62135596800, 0),
    /*micros*/ -63082281600000000,
    /*formatted: "0001-01-01T00:00:00Z"*/
    /*output*/ StdDuration::new(62135596800, 0)
    ; "minimum"
)]
// Rounding in pre Unix Epoch
// --------------------------
#[test_case(
    /*input: "1814-03-09T01:02:03.000005500Z",*/
        StdDuration::new(4917106676, 999994500),
    /*micros*/ -5863791476999994,
    /*formatted "1814-03-09T01:02:03.000006000Z"*/
    /*output*/ StdDuration::new(4917106676, 999994000)
    ; "negative unix timestamp, round up"
)]
#[test_case(
    /*input: "1814-03-09T01:02:03.000005501Z",*/
        StdDuration::new(4917106676, 999994499),
    /*micros*/ -5863791476999994,
    /*formatted: "1814-03-09T01:02:03.000006000Z"*/
    /*output*/ StdDuration::new(4917106676, 999994000)
    ; "negative unix timestamp, 5501"
)]
#[test_case(
    /*input: "1814-03-09T01:02:03.000005499Z",*/
        StdDuration::new(4917106676, 999994501),
    /*micros*/ -5863791476999995,
    /*formatted: "1814-03-09T01:02:03.000005000Z"*/
    /*output*/ StdDuration::new(4917106676, 999995000)
    ; "negative unix timestamp, 5499"
)]
#[test_case(
    /*input: "1856-08-27T01:02:03.000004500Z",*/
        StdDuration::new(3576869876, 999995500),
    /*micros*/ -4523554676999996,
    /*formatted: "1856-08-27T01:02:03.000004000Z"*/
    /*output*/ StdDuration::new(3576869876, 999996000)
    ; "negative unix timestamp, round down"
)]
#[test_case(
    /*input: "1856-08-27T01:02:03.000004501Z",*/
        StdDuration::new(3576869876, 999995499),
    /*micros*/ -4523554676999995,
    /*formatted:"1856-08-27T01:02:03.000005000Z" */
    /*output*/ StdDuration::new(3576869876, 999995000)
    ; "negative unix timestamp, 4501"
)]
#[test_case(
    /*input: "1856-08-27T01:02:03.000004499Z",*/
        StdDuration::new(3576869876, 999995501),
    /*micros*/ -4523554676999996,
    /*formatted: "1856-08-27T01:02:03.000004000Z"*/
    /*output*/ StdDuration::new(3576869876, 999996000)
    ; "negative unix timestamp, 4499"
)]
#[test_case(
    /*input: "1969-12-31T23:59:59.999999500Z",*/
        StdDuration::new(0, 500),
    /*micros*/ -946684800000000,
    /*formatted: "1970-01-01T00:00:00Z"*/
    /*output*/ StdDuration::new(0, 0)
    ; "unix timestamp to zero"
)]
fn datetime_pre_1970(input: StdDuration, micros: i64, output: StdDuration) {
    let edgedb: Datetime = (UNIX_EPOCH - input).try_into().unwrap();
    // different format but we assert microseconds anyways
    // assert_eq!(format!("{:?}", edgedb), formatted);

    let mut buf = BytesMut::new();
    let val = Value::Datetime(edgedb.clone());
    codec::Datetime.encode(&mut buf, &val).unwrap();
    let serialized_micros = buf.get_i64();

    assert_eq!(serialized_micros, micros);

    let rev = SystemTime::try_from(edgedb).unwrap();
    assert_eq!(rev, UNIX_EPOCH - output);
}

#[test_case(
    /*input*/ StdDuration::new(0, 0),
    /*micros*/ 0,
    /*output*/ StdDuration::new(0, 0)
    ; "Zero"
)]
#[test_case(
    /*input*/ StdDuration::new(1234, 567890123),
    /*micros*/ 1234567890,
    /*output*/ StdDuration::new(1234, 567890000)
    ; "Some value"
)]
#[test_case(
    /*input*/ StdDuration::new(1, 2500),
    /*micros*/ 1000002,
    /*output*/ StdDuration::new(1, 2000)
    ; "round down"
)]
#[test_case(
    /*input*/ StdDuration::new(23, 2499),
    /*micros*/ 23000002,
    /*output*/ StdDuration::new(23, 2000)
    ; "2499 nanos"
)]
#[test_case(
    /*input*/ StdDuration::new(456, 2501),
    /*micros*/ 456000003,
    /*output*/ StdDuration::new(456, 3000)
    ; "2501 nanos"
)]
#[test_case(
    /*input*/ StdDuration::new(5789, 3500),
    /*micros*/ 5789000004,
    /*output*/ StdDuration::new(5789, 4000)
    ; "round up"
)]
#[test_case(
    /*input*/ StdDuration::new(12345, 3499),
    /*micros*/ 12345000003,
    /*output*/ StdDuration::new(12345, 3000)
    ; "3499 nanos"
)]
#[test_case(
    /*input*/ StdDuration::new(789012, 3501),
    /*micros*/ 789012000004,
    /*output*/ StdDuration::new(789012, 4000)
    ; "3501 nanos"
)]
fn duration(input: StdDuration, micros: i64, output: StdDuration) {
    let edgedb: Duration = input.try_into().unwrap();

    let mut buf = BytesMut::new();
    let val = Value::Duration(edgedb.clone());
    codec::Duration.encode(&mut buf, &val).unwrap();
    let serialized_micros = buf.get_i64();

    assert_eq!(serialized_micros, micros);

    let rev: StdDuration = edgedb.try_into().unwrap();
    assert_eq!(rev, output);
}
