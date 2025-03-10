#[cfg(feature = "chrono")]
mod chrono {

    use std::convert::TryInto;
    use std::str::FromStr;

    use bytes::{Buf, BytesMut};
    use gel_protocol::codec::{self, Codec};
    use gel_protocol::model::{Datetime, LocalDatetime, LocalTime};
    use gel_protocol::value::Value;
    use test_case::test_case;

    // ========
    // Datetime
    // ========

    // Minimum and Maximum
    // -------------------
    #[test_case(
        /*input*/ "9999-12-31T23:59:59.999999499Z",
        // Note: Can't round up here, so   --^
        /*micros*/ 252455615999999999,
        /*formatted*/ "9999-12-31T23:59:59.999999Z"
        ; "maximum"
    )]
    #[test_case(
        /*input*/ "0001-01-01T00:00:00.000000Z",
        /*micros*/ -63082281600000000,
        /*formatted*/ "0001-01-01T00:00:00Z"
        ; "minimum"
    )]
    // Rounding in Various Ranges
    // --------------------------
    #[test_case(
        /*input*/ "1814-03-09T01:02:03.000005500Z",
        /*micros*/ -5863791476999994,
        /*formatted*/ "1814-03-09T01:02:03.000006Z"
        ; "negative unix timestamp, round up"
    )]
    #[test_case(
        /*input*/ "1814-03-09T01:02:03.000005501Z",
        /*micros*/ -5863791476999994,
        /*formatted*/ "1814-03-09T01:02:03.000006Z"
        ; "negative unix timestamp, 5501"
    )]
    #[test_case(
        /*input*/ "1814-03-09T01:02:03.000005499Z",
        /*micros*/ -5863791476999995,
        /*formatted*/ "1814-03-09T01:02:03.000005Z"
        ; "negative unix timestamp, 5499"
    )]
    #[test_case(
        /*input*/ "1856-08-27T01:02:03.000004500Z",
        /*micros*/ -4523554676999996,
        /*formatted*/ "1856-08-27T01:02:03.000004Z"
        ; "negative unix timestamp, round down"
    )]
    #[test_case(
        /*input*/ "1856-08-27T01:02:03.000004501Z",
        /*micros*/ -4523554676999995,
        /*formatted*/ "1856-08-27T01:02:03.000005Z"
        ; "negative unix timestamp, 4501"
    )]
    #[test_case(
        /*input*/ "1856-08-27T01:02:03.000004499Z",
        /*micros*/ -4523554676999996,
        /*formatted*/ "1856-08-27T01:02:03.000004Z"
        ; "negative unix timestamp, 4499"
    )]
    #[test_case(
        /*input*/ "1969-12-31T23:59:59.999999500Z",
        /*micros*/ -946684800000000,
        /*formatted*/ "1970-01-01T00:00:00Z"
        ; "unix timestamp to zero"
    )]
    #[test_case(
        /*input*/ "1997-07-05T01:02:03.000009500Z",
        /*micros*/ -78620276999990,
        /*formatted*/ "1997-07-05T01:02:03.000010Z"
        ; "negative postgres timestamp, round up"
    )]
    #[test_case(
        /*input*/ "1997-07-05T01:02:03.000009500Z",
        /*micros*/ -78620276999990,
        /*formatted*/ "1997-07-05T01:02:03.000010Z"
        ; "negative postgres timestamp, 9501"
    )]
    #[test_case(
        /*input*/ "1997-07-05T01:02:03.000009499Z",
        /*micros*/ -78620276999991,
        /*formatted*/ "1997-07-05T01:02:03.000009Z"
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
        /*formatted*/ "1997-07-05T01:02:03.000001Z"
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
        /*formatted*/ "2014-02-27T00:00:00.000002Z"
        ; "positive timestamp, round up"
    )]
    #[test_case(
        /*input*/ "2014-02-27T00:00:00.000001501Z",
        /*micros*/ 446774400000002,
        /*formatted*/ "2014-02-27T00:00:00.000002Z"
        ; "positive timestamp, 1501"
    )]
    #[test_case(
        /*input*/ "2014-02-27T00:00:00.000001499Z",
        /*micros*/ 446774400000001,
        /*formatted*/ "2014-02-27T00:00:00.000001Z"
        ; "positive timestamp, 1499"
    )]
    #[test_case(
        /*input*/ "2022-02-24T05:43:03.000002500Z",
        /*micros*/ 698996583000002,
        /*formatted*/ "2022-02-24T05:43:03.000002Z"
        ; "positive timestamp, round down"
    )]
    #[test_case(
        /*input*/ "2022-02-24T05:43:03.000002501Z",
        /*micros*/ 698996583000003,
        /*formatted*/ "2022-02-24T05:43:03.000003Z"
        ; "positive timestamp, 2501"
    )]
    #[test_case(
        /*input*/ "2022-02-24T05:43:03.000002499Z",
        /*micros*/ 698996583000002,
        /*formatted*/ "2022-02-24T05:43:03.000002Z"
        ; "positive timestamp, 2499"
    )]
    fn datetime(input: &str, micros: i64, formatted: &str) {
        let chrono = chrono::DateTime::<chrono::Utc>::from_str(input).unwrap();
        let edgedb: Datetime = chrono.try_into().unwrap();
        assert_eq!(format!("{:?}", edgedb), formatted);

        let mut buf = BytesMut::new();
        let val = Value::Datetime(edgedb);
        codec::Datetime.encode(&mut buf, &val).unwrap();
        let serialized_micros = buf.get_i64();

        assert_eq!(serialized_micros, micros);

        let rev = chrono::DateTime::<chrono::Utc>::from(edgedb);
        assert_eq!(format!("{:?}", rev), formatted);
    }

    // ==============
    // Local Datetime
    // ==============

    // Minimum and Maximum
    // -------------------
    #[test_case(
        /*input*/ "9999-12-31T23:59:59.999999499",
        // Note: Can't round up here, so   --^
        /*micros*/ 252455615999999999,
        /*formatted*/ "9999-12-31T23:59:59.999999"
        ; "maximum"
    )]
    #[test_case(
        /*input*/ "0001-01-01T00:00:00.000000",
        /*micros*/ -63082281600000000,
        /*formatted*/ "0001-01-01T00:00:00"
        ; "minimum"
    )]
    // Rounding in Various Ranges
    // --------------------------
    #[test_case(
        /*input*/ "1814-03-09T01:02:03.000005500",
        /*micros*/ -5863791476999994,
        /*formatted*/ "1814-03-09T01:02:03.000006"
        ; "negative unix timestamp, round up"
    )]
    #[test_case(
        /*input*/ "1814-03-09T01:02:03.000005501",
        /*micros*/ -5863791476999994,
        /*formatted*/ "1814-03-09T01:02:03.000006"
        ; "negative unix timestamp, 5501"
    )]
    #[test_case(
        /*input*/ "1814-03-09T01:02:03.000005499",
        /*micros*/ -5863791476999995,
        /*formatted*/ "1814-03-09T01:02:03.000005"
        ; "negative unix timestamp, 5499"
    )]
    #[test_case(
        /*input*/ "1856-08-27T01:02:03.000004500",
        /*micros*/ -4523554676999996,
        /*formatted*/ "1856-08-27T01:02:03.000004"
        ; "negative unix timestamp, round down"
    )]
    #[test_case(
        /*input*/ "1856-08-27T01:02:03.000004501",
        /*micros*/ -4523554676999995,
        /*formatted*/ "1856-08-27T01:02:03.000005"
        ; "negative unix timestamp, 4501"
    )]
    #[test_case(
        /*input*/ "1856-08-27T01:02:03.000004499",
        /*micros*/ -4523554676999996,
        /*formatted*/ "1856-08-27T01:02:03.000004"
        ; "negative unix timestamp, 4499"
    )]
    #[test_case(
        /*input*/ "1969-12-31T23:59:59.999999500",
        /*micros*/ -946684800000000,
        /*formatted*/ "1970-01-01T00:00:00"
        ; "unix timestamp to zero"
    )]
    #[test_case(
        /*input*/ "1997-07-05T01:02:03.000009500",
        /*micros*/ -78620276999990,
        /*formatted*/ "1997-07-05T01:02:03.000010"
        ; "negative postgres timestamp, round up"
    )]
    #[test_case(
        /*input*/ "1997-07-05T01:02:03.000009500",
        /*micros*/ -78620276999990,
        /*formatted*/ "1997-07-05T01:02:03.000010"
        ; "negative postgres timestamp, 9501"
    )]
    #[test_case(
        /*input*/ "1997-07-05T01:02:03.000009499",
        /*micros*/ -78620276999991,
        /*formatted*/ "1997-07-05T01:02:03.000009"
        ; "negative postgres timestamp, 9499"
    )]
    #[test_case(
        /*input*/ "1997-07-05T01:02:03.000000500",
        /*micros*/ -78620277000000,
        /*formatted*/ "1997-07-05T01:02:03"
        ; "negative postgres timestamp, round down"
    )]
    #[test_case(
        /*input*/ "1997-07-05T01:02:03.000000501",
        /*micros*/ -78620276999999,
        /*formatted*/ "1997-07-05T01:02:03.000001"
        ; "negative postgres timestamp, 501"
    )]
    #[test_case(
        /*input*/ "1997-07-05T01:02:03.000000499",
        /*micros*/ -78620277000000,
        /*formatted*/ "1997-07-05T01:02:03"
        ; "negative postgres timestamp, 499"
    )]
    #[test_case(
        /*input*/ "1999-12-31T23:59:59.999999500",
        /*micros*/ 0,
        /*formatted*/ "2000-01-01T00:00:00"
        ; "postgres timestamp to zero"
    )]
    #[test_case(
        /*input*/ "2014-02-27T00:00:00.000001500",
        /*micros*/ 446774400000002,
        /*formatted*/ "2014-02-27T00:00:00.000002"
        ; "positive timestamp, round up"
    )]
    #[test_case(
        /*input*/ "2014-02-27T00:00:00.000001501",
        /*micros*/ 446774400000002,
        /*formatted*/ "2014-02-27T00:00:00.000002"
        ; "positive timestamp, 1501"
    )]
    #[test_case(
        /*input*/ "2014-02-27T00:00:00.000001499",
        /*micros*/ 446774400000001,
        /*formatted*/ "2014-02-27T00:00:00.000001"
        ; "positive timestamp, 1499"
    )]
    #[test_case(
        /*input*/ "2022-02-24T05:43:03.000002500",
        /*micros*/ 698996583000002,
        /*formatted*/ "2022-02-24T05:43:03.000002"
        ; "positive timestamp, round down"
    )]
    #[test_case(
        /*input*/ "2022-02-24T05:43:03.000002501",
        /*micros*/ 698996583000003,
        /*formatted*/ "2022-02-24T05:43:03.000003"
        ; "positive timestamp, 2501"
    )]
    #[test_case(
        /*input*/ "2022-02-24T05:43:03.000002499",
        /*micros*/ 698996583000002,
        /*formatted*/ "2022-02-24T05:43:03.000002"
        ; "positive timestamp, 2499"
    )]
    fn local_datetime(input: &str, micros: i64, formatted: &str) {
        let chrono = chrono::NaiveDateTime::from_str(input).unwrap();
        let edgedb: LocalDatetime = chrono.try_into().unwrap();
        assert_eq!(format!("{:?}", edgedb), formatted);

        let mut buf = BytesMut::new();
        let val = Value::LocalDatetime(edgedb);
        codec::LocalDatetime.encode(&mut buf, &val).unwrap();
        let serialized_micros = buf.get_i64();

        assert_eq!(serialized_micros, micros);

        let rev = chrono::NaiveDateTime::from(edgedb);
        assert_eq!(format!("{:?}", rev), formatted);
    }

    // ==========
    // Local Time
    // ==========
    #[test_case(
        /*input*/ "23:59:59.999999500",
        // Note: Can't round up here, so   --^
        /*micros*/ 0,
        /*formatted*/ "00:00:00"
        ; "wraparound"
    )]
    #[test_case(
        /*input*/ "00:00:00.000000",
        /*micros*/ 0,
        /*formatted*/ "00:00:00"
        ; "minimum"
    )]
    #[test_case(
        /*input*/ "23:59:59.999999",
        /*micros*/ 86399999999,
        /*formatted*/ "23:59:59.999999"
        ; "maximum"
    )]
    #[test_case(
        /*input*/ "01:02:03.000005500",
        /*micros*/ 3723000006,
        /*formatted*/ "01:02:03.000006"
        ; "round up"
    )]
    #[test_case(
        /*input*/ "01:02:03.000005501",
        /*micros*/ 3723000006,
        /*formatted*/ "01:02:03.000006"
        ; "5501"
    )]
    #[test_case(
        /*input*/ "01:02:03.000005499",
        /*micros*/ 3723000005,
        /*formatted*/ "01:02:03.000005"
        ; "5499"
    )]
    #[test_case(
        /*input*/ "01:02:03.000004500",
        /*micros*/ 3723000004,
        /*formatted*/ "01:02:03.000004"
        ; "round down"
    )]
    #[test_case(
        /*input*/ "01:02:03.000004501",
        /*micros*/ 3723000005,
        /*formatted*/ "01:02:03.000005"
        ; "4501"
    )]
    #[test_case(
        /*input*/ "01:02:03.000004499",
        /*micros*/ 3723000004,
        /*formatted*/ "01:02:03.000004"
        ; "4499"
    )]
    fn local_time(input: &str, micros: i64, formatted: &str) {
        let chrono = chrono::NaiveTime::from_str(input).unwrap();
        let edgedb: LocalTime = chrono.into();
        assert_eq!(format!("{:?}", edgedb), formatted);

        let mut buf = BytesMut::new();
        let val = Value::LocalTime(edgedb);
        codec::LocalTime.encode(&mut buf, &val).unwrap();
        let serialized_micros = buf.get_i64();

        assert_eq!(serialized_micros, micros);

        let rev = chrono::NaiveTime::from(edgedb);
        assert_eq!(format!("{:?}", rev), formatted);
    }
}
