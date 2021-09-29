use std::str::FromStr;
use bytes::{Bytes, BytesMut};
use uuid::Uuid;

use crate::features::ProtocolVersion;
use crate::query_arg::{Encoder, DescriptorContext, ScalarArg};
use crate::serialization::decode::RawCodec;
use crate::model::Json;

fn encode(val: impl ScalarArg) -> Bytes {
    let proto = ProtocolVersion::current();
    let ctx = DescriptorContext {
        proto: &proto,
        root_pos: None,
        descriptors: &[],
    };
    let mut buf = BytesMut::new();
    let mut encoder = Encoder::new(&ctx, &mut buf);
    ScalarArg::encode(&val, &mut encoder).expect("encoded");
    return buf.freeze();
}

fn decode<'x, T: RawCodec<'x>>(bytes: &'x [u8]) -> T {
    <T as RawCodec>::decode(bytes).expect("decoded")
}

macro_rules! encoding_eq {
    ($data: expr, $bytes: expr) => {
        let lambda = |_a| (); // type inference hack
        let data = $data;
        lambda(&data); // type inference hack
        let val = decode($bytes);
        lambda(&val); // type inference hack
        assert_eq!(val, data, "decoding failed");
        println!("Decoded value: {:?}", val);

        let buf = encode($data);
        println!("Encoded value: {:?}", &buf[..]);
        assert_eq!(&buf[..], $bytes, "encoding failed");
    }
}

#[test]
fn bool() {
    encoding_eq!(true, b"\x01");
    encoding_eq!(false, b"\x00");
}

#[test]
fn str() {
    encoding_eq!("hello", b"hello");
    encoding_eq!(r#""world!""#, b"\"world!\"");
    encoding_eq!(String::from("hello"), b"hello");
    encoding_eq!(String::from(r#""world!""#), b"\"world!\"");
}

#[test]
fn json() {
    let val = unsafe { Json::new_unchecked("{}".into()) };
    assert_eq!(&encode(val)[..], b"\x01{}");
    assert_eq!(&decode::<Json>(b"\x01{}")[..], "{}");
}

#[test]
fn int16() {
    encoding_eq!(0i16, b"\0\0");
    encoding_eq!(0x105i16, b"\x01\x05");
    encoding_eq!(i16::MAX, b"\x7F\xFF");
    encoding_eq!(i16::MIN, b"\x80\x00");
    encoding_eq!(-1i16, b"\xFF\xFF");
}

#[test]
fn int32() {
    encoding_eq!(0i32, b"\0\0\0\0");
    encoding_eq!(0x105i32, b"\0\0\x01\x05");
    encoding_eq!(i32::MAX, b"\x7F\xFF\xFF\xFF");
    encoding_eq!(i32::MIN, b"\x80\x00\x00\x00");
    encoding_eq!(-1i32, b"\xFF\xFF\xFF\xFF");
}


#[test]
fn int64() {
    encoding_eq!(0i64, b"\0\0\0\0\0\0\0\0");
    encoding_eq!(0x105i64, b"\0\0\0\0\0\0\x01\x05");
    encoding_eq!(i64::MAX, b"\x7F\xFF\xFF\xFF\xFF\xFF\xFF\xFF");
    encoding_eq!(i64::MIN, b"\x80\x00\x00\x00\x00\x00\x00\x00");
    encoding_eq!(-1i64, b"\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF");
}

#[test]
fn float32() {
    encoding_eq!(0.0f32, b"\0\0\0\0");
    encoding_eq!(-0.0f32, b"\x80\0\0\0");
    encoding_eq!(1.0f32, b"?\x80\0\0");
    encoding_eq!(-1.123f32, b"\xbf\x8f\xbew");
    assert_eq!(&encode(f32::NAN)[..], b"\x7f\xc0\0\0");
    assert_eq!(&encode(f32::INFINITY)[..], b"\x7f\x80\0\0");
    assert_eq!(&encode(f32::NEG_INFINITY)[..], b"\xff\x80\0\0");
    assert!(decode::<f32>(b"\x7f\xc0\0\0").is_nan());
    assert!(decode::<f32>(b"\x7f\x80\0\0").is_infinite());
    assert!(decode::<f32>(b"\x7f\x80\0\0").is_sign_positive());
    assert!(decode::<f32>(b"\xff\x80\0\0").is_infinite());
    assert!(decode::<f32>(b"\xff\x80\0\0").is_sign_negative());
}

#[test]
fn float64() {
    encoding_eq!(0.0, b"\0\0\0\0\0\0\0\0");
    encoding_eq!(-0.0, b"\x80\0\0\0\0\0\0\0");
    encoding_eq!(1.0, b"?\xf0\0\0\0\0\0\0");
    encoding_eq!(1e100, b"T\xb2I\xad%\x94\xc3}");
    assert_eq!(&encode(f64::NAN)[..], b"\x7f\xf8\0\0\0\0\0\0");
    assert_eq!(&encode(f64::INFINITY)[..], b"\x7f\xf0\0\0\0\0\0\0");
    assert_eq!(&encode(f64::NEG_INFINITY)[..], b"\xff\xf0\0\0\0\0\0\0");
    assert!(decode::<f64>(b"\x7f\xf8\0\0\0\0\0\0").is_nan());
    assert!(decode::<f64>(b"\x7f\xf0\0\0\0\0\0\0").is_infinite());
    assert!(decode::<f64>(b"\x7f\xf0\0\0\0\0\0\0").is_sign_positive());
    assert!(decode::<f64>(b"\xff\xf0\0\0\0\0\0\0").is_infinite());
    assert!(decode::<f64>(b"\xff\xf0\0\0\0\0\0\0").is_sign_negative());
}

#[test]
fn bytes() {
    encoding_eq!(&b"hello"[..], b"hello");
    encoding_eq!(&b""[..], b"");
    encoding_eq!(&b"\x00\x01\x02\x03\x81"[..], b"\x00\x01\x02\x03\x81");
    encoding_eq!(b"hello".to_vec(), b"hello");
    encoding_eq!(b"".to_vec(), b"");
    encoding_eq!(b"\x00\x01\x02\x03\x81".to_vec(), b"\x00\x01\x02\x03\x81");
}

#[test]
#[cfg(feature="bigdecimal")]
fn decimal() {
    use std::convert::TryInto;
    use bigdecimal::BigDecimal;
    use crate::model::Decimal;

    fn dec(s: &str) -> Decimal {
        bdec(s).try_into().expect("bigdecimal -> decimal")
    }

    fn bdec(s: &str) -> BigDecimal {
        BigDecimal::from_str(s).expect("bigdecimal")
    }

    encoding_eq!(bdec("42.00"), b"\0\x01\0\0\0\0\0\x02\0*");
    encoding_eq!(dec("42.00"), b"\0\x01\0\0\0\0\0\x02\0*");

    encoding_eq!(bdec("12345678.901234567"),
        b"\0\x05\0\x01\0\0\0\t\x04\xd2\x16.#4\r\x80\x1bX");
    encoding_eq!(dec("12345678.901234567"),
        b"\0\x05\0\x01\0\0\0\t\x04\xd2\x16.#4\r\x80\x1bX");
    encoding_eq!(bdec("1e100"), b"\0\x01\0\x19\0\0\0\0\0\x01");
    encoding_eq!(dec("1e100"), b"\0\x01\0\x19\0\0\0\0\0\x01");
    encoding_eq!(bdec("-703367234220692490200000000000000000000000000"),
        b"\0\x06\0\x0b@\0\0\0\0\x07\x01P\x1cB\x08\x9e$!\0\xc8");
    encoding_eq!(dec("-703367234220692490200000000000000000000000000"),
        b"\0\x06\0\x0b@\0\0\0\0\x07\x01P\x1cB\x08\x9e$!\0\xc8");
    encoding_eq!(bdec("-7033672342206924902e26"),
        b"\0\x06\0\x0b@\0\0\0\0\x07\x01P\x1cB\x08\x9e$!\0\xc8");
    encoding_eq!(dec("-7033672342206924902e26"),
        b"\0\x06\0\x0b@\0\0\0\0\x07\x01P\x1cB\x08\x9e$!\0\xc8");
}

#[test]
#[cfg(feature="num-bigint")]
fn bigint() {
    use std::convert::TryInto;
    use crate::model::BigInt;

    fn bint1(val: i32) -> num_bigint::BigInt {
        val.into()
    }
    fn bint2(val: i32) -> BigInt {
        bint1(val).try_into().unwrap()
    }

    fn bint1s(val: &str) -> num_bigint::BigInt {
        val.parse().unwrap()
    }
    fn bint2s(val: &str) -> BigInt {
        bint1s(val).try_into().unwrap()
    }

    encoding_eq!(bint1(42), b"\0\x01\0\0\0\0\0\0\0*");
    encoding_eq!(bint2(42), b"\0\x01\0\0\0\0\0\0\0*");
    encoding_eq!(bint1(30000), b"\0\x01\0\x01\0\0\0\0\0\x03");
    encoding_eq!(bint2(30000), b"\0\x01\0\x01\0\0\0\0\0\x03");
    encoding_eq!(bint1(30001), b"\0\x02\0\x01\0\0\0\0\0\x03\0\x01");
    encoding_eq!(bint1(-15000), b"\0\x02\0\x01@\0\0\0\0\x01\x13\x88");
    encoding_eq!(bint2(-15000), b"\0\x02\0\x01@\0\0\0\0\x01\x13\x88");
    encoding_eq!(bint1s("1000000000000000000000"), b"\0\x01\0\x05\0\0\0\0\0\n");
    encoding_eq!(bint2s("1000000000000000000000"), b"\0\x01\0\x05\0\0\0\0\0\n");
}

#[test]
fn uuid() {
    encoding_eq!(
        Uuid::from_str("4928cc1e-2065-11ea-8848-7b53a6adb383").unwrap(),
        b"I(\xcc\x1e e\x11\xea\x88H{S\xa6\xad\xb3\x83");
}
