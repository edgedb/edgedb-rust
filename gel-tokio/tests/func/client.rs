use std::str::FromStr;

use futures_util::stream::{self, StreamExt};
use gel_errors::NoDataError;
use gel_protocol::model::{Json, Uuid};
use gel_protocol::named_args;
use gel_protocol::value::{EnumValue, Value};
use gel_tokio::{Client, Queryable};
use serde::{Deserialize, Serialize};

use crate::server::SERVER;

#[tokio::test]
async fn simple() -> anyhow::Result<()> {
    let client = Client::new(&SERVER.config);
    client.ensure_connected().await?;

    let value = client.query::<i64, _>("SELECT 7*93", &()).await?;
    assert_eq!(value, vec![651]);

    let value = client.query_single::<i64, _>("SELECT 5*11", &()).await?;
    assert_eq!(value, Some(55));

    let value = client
        .query_single::<i64, _>("SELECT <int64>{}", &())
        .await?;
    assert_eq!(value, None);

    let value = client
        .query_required_single::<i64, _>("SELECT 5*11", &())
        .await?;
    assert_eq!(value, 55);

    let err = client
        .query_required_single::<i64, _>("SELECT <int64>{}", &())
        .await
        .unwrap_err();
    assert!(err.is::<NoDataError>());

    let value = client.query_json("SELECT 'x' ++ 'y'", &()).await?;
    assert_eq!(value.as_ref(), r#"["xy"]"#);

    let value = client.query_single_json("SELECT 'x' ++ 'y'", &()).await?;
    assert_eq!(value.as_deref(), Some(r#""xy""#));

    let value = client.query_single_json("SELECT <str>{}", &()).await?;
    assert_eq!(value.as_deref(), None);

    let value = client.query_json("SELECT <str>{}", &()).await?;
    assert_eq!(value, Json::new_unchecked("[]".to_string()));

    let err = client
        .query_required_single_json("SELECT <int64>{}", &())
        .await
        .unwrap_err();
    assert!(err.is::<NoDataError>());

    client.execute("SELECT 1+1", &()).await?;
    client
        .execute("START MIGRATION TO {}; ABORT MIGRATION", &())
        .await?;

    // basic enum param
    let enum_query = "SELECT <str>(<test::State>$0) = 'waiting'";
    assert!(client
        .query_required_single::<bool, _>(enum_query, &(Value::Enum(EnumValue::from("waiting")),))
        .await
        .unwrap());

    // unsupported: enum param as Value::Str
    client
        .query_required_single::<bool, (Value,)>(enum_query, &(Value::Str("waiting".to_string()),))
        .await
        .unwrap_err();

    // unsupported: enum param as String
    client
        .query_required_single::<bool, (String,)>(enum_query, &("waiting".to_string(),))
        .await
        .unwrap_err();

    // enum param as &str
    assert!(client
        .query_required_single::<bool, (&'_ str,)>(enum_query, &("waiting",),)
        .await
        .unwrap());

    // named args
    let value = client
        .query_required_single::<String, _>(
            "select (
            std::array_join(<array<str>>$msg1, ' ')
            ++ (<optional str>$question ?? ' the ultimate question of life')
            ++ ': '
            ++ <str><int64>$answer
        );",
            &named_args! {
                "msg1" => vec!["the".to_string(), "answer".to_string(), "to".to_string()],
                "question" => None::<String>,
                "answer" => 42_i64,
            },
        )
        .await
        .unwrap();
    assert_eq!(
        value.as_str(),
        "the answer to the ultimate question of life: 42"
    );

    // args for values
    let uuid = "43299d0a-f993-4dcb-a8a2-50041bf5af79";
    let value = client
        .query_required_single::<Uuid, _>(
            "select <uuid>$my_uuid;",
            &named_args! {
                "my_uuid" => Uuid::from_str("43299d0a-f993-4dcb-a8a2-50041bf5af79").unwrap(),
            },
        )
        .await
        .unwrap();
    assert_eq!(value, Uuid::from_str(uuid).unwrap());

    Ok(())
}

#[tokio::test]
async fn parallel_queries() -> anyhow::Result<()> {
    let client = Client::new(&SERVER.config);
    client.ensure_connected().await?;

    let result = stream::iter(0..10i64)
        .map(|idx| {
            let cli = client.clone();
            async move {
                cli.query_required_single::<i64, _>("SELECT <int64>$0*10", &(idx,))
                    .await
            }
        })
        .buffer_unordered(7)
        .collect::<Vec<_>>()
        .await;
    let mut result: Vec<_> = result.into_iter().collect::<Result<_, _>>()?;
    result.sort();

    assert_eq!(result, (0..100).step_by(10).collect::<Vec<_>>());

    Ok(())
}

#[tokio::test]
async fn json() -> anyhow::Result<()> {
    let client = Client::new(&SERVER.config);
    client.ensure_connected().await?;

    client
        .execute::<_>(
            "insert test::OtpPhoneRequest {
            phone := '0123456789',
            sent_at := datetime_of_statement(),
            otp := 98271
        }",
            &(),
        )
        .await
        .unwrap();

    #[derive(Clone, Debug, Serialize, Deserialize, Queryable)]
    #[gel(json)]
    pub struct OtpPhoneRequest {
        pub phone: String,
        pub otp: i32,
    }

    let res = client.query::<OtpPhoneRequest, _>(
        "select <json>(select test::OtpPhoneRequest { phone, otp } filter .phone = '0123456789')",
        &()
    )
    .await?;
    let res = res.into_iter().next().unwrap();
    assert_eq!(res.phone, "0123456789");
    assert_eq!(res.otp, 98271);

    Ok(())
}

#[tokio::test]
async fn big_num() -> anyhow::Result<()> {
    let client = Client::new(&SERVER.config);
    client.ensure_connected().await?;

    let res = client
        .query_required_single::<Value, _>("select 1234567890123456789012345678900000n", &())
        .await
        .unwrap();
    let Value::BigInt(res) = res else { panic!() };
    assert_eq!(res.to_string(), "1234567890123456789012345678900000");

    let res = client
        .query_required_single::<Value, _>("select 1234567891234567890.12345678900000n", &())
        .await
        .unwrap();
    let Value::Decimal(res) = res else { panic!() };
    assert_eq!(res.to_string(), "1234567891234567890.12345678900000");

    let res = client
        .query_required_single::<Value, _>("select 0.00012n", &())
        .await
        .unwrap();
    let Value::Decimal(res) = res else { panic!() };
    assert!(!res.negative());
    assert_eq!(res.decimal_digits(), 5);
    assert_eq!(res.digits(), [1, 2000]);
    assert_eq!(res.weight() * 4, -4);
    assert_eq!(res.to_string(), "0.00012");

    let res = client
        .query_required_single::<Value, _>("select <decimal>0.000000000000000000001", &())
        .await
        .unwrap();
    let Value::Decimal(res) = res else { panic!() };
    assert!(!res.negative());
    assert_eq!(res.decimal_digits(), 21);
    assert_eq!(res.digits(), [1000]);
    assert_eq!(res.weight() * 4, -24);
    assert_eq!(res.to_string(), "0.000000000000000000001");

    Ok(())
}

#[tokio::test]
async fn bytes() -> anyhow::Result<()> {
    let client = Client::new(&SERVER.config);
    client.ensure_connected().await?;

    #[derive(Queryable)]
    struct MyResult {
        data: bytes::Bytes,
    }

    let res = client
        .query_required_single::<MyResult, _>("select { data := b'101' } limit 1", &())
        .await
        .unwrap();

    assert_eq!(res.data, b"101"[..]);
    Ok(())
}

#[tokio::test]
async fn time() -> anyhow::Result<()> {
    let client = Client::new(&SERVER.config);
    client.ensure_connected().await?;

    let res: gel_protocol::model::Duration = client
        .query_required_single("select <duration>'45.6 seconds';", &())
        .await
        .unwrap();
    assert_eq!(res, gel_protocol::model::Duration::from_micros(45_600_000));

    Ok(())
}

#[tokio::test]
async fn wrong_field_number() -> anyhow::Result<()> {
    let client = Client::new(&SERVER.config);
    client.ensure_connected().await?;

    #[derive(Queryable, PartialEq, Debug)]
    struct Thing {
        a: String,
        b: String,
    }
    let err = client
        .query_required_single::<Thing, _>("select { a := 'hello' }", &())
        .await
        .unwrap_err();
    assert_eq!(
        format!("{err:#}"),
        "DescriptorMismatch: expected 2 fields, got 1"
    );

    let err = client
        .query_required_single::<Thing, _>("select { a := 'hello', b := 'world', c := 42 }", &())
        .await
        .unwrap_err();
    assert_eq!(
        format!("{err:#}"),
        "DescriptorMismatch: expected 2 fields, got 3"
    );

    let err = client
        .query_required_single::<Thing, _>("select { a := 'hello', c := 'world' }", &())
        .await
        .unwrap_err();
    assert_eq!(format!("{err:#}"), "DescriptorMismatch: expected field b");

    Ok(())
}

#[tokio::test]
async fn warnings() -> anyhow::Result<()> {
    let client = Client::new(&SERVER.config);
    client.ensure_connected().await?;

    let res = client
        .query_verbose::<i64, _>("select std::_warn_on_call()", &())
        .await
        .unwrap();
    assert_eq!(res.warnings.len(), 1);

    // TODO: test that the warning is logged

    Ok(())
}

#[tokio::test]
async fn vector() -> anyhow::Result<()> {
    let client = Client::new(&SERVER.config);
    client.ensure_connected().await?;

    let res: Value = client
        .query_required_single("select <ext::pgvector::vector>[9.9, 8.8]", &())
        .await
        .unwrap();
    let Value::Vector(res) = res else { panic!() };
    assert_eq!(res, vec![9.9, 8.8]);

    let res: gel_protocol::model::Vector = client
        .query_required_single("select <ext::pgvector::vector>[9.9, 8.8]", &())
        .await
        .unwrap();
    assert_eq!(res.0, vec![9.9, 8.8]);

    let arg = Value::Vector(vec![9.9, 8.8]);
    let res: f32 = client
        .query_required_single(
            "select (<array<float32>>(<ext::pgvector::vector>$0))[0]",
            &(arg,),
        )
        .await
        .unwrap();
    assert_eq!(res, 9.9);

    let arg = gel_protocol::model::Vector(vec![9.9, 8.8]);
    let res: f32 = client
        .query_required_single(
            "select (<array<float32>>(<ext::pgvector::vector>$0))[0]",
            &(arg,),
        )
        .await
        .unwrap();
    assert_eq!(res, 9.9);

    Ok(())
}

#[tokio::test]
async fn props_in_wrong_order() -> anyhow::Result<()> {
    let client = Client::new(&SERVER.config);
    client.ensure_connected().await?;

    #[derive(Debug, PartialEq, Queryable)]
    struct Foo {
        hello: String,
        world: i64,
    }

    let res = client
        .query_required_single::<Foo, _>("select { world := 42, hello := 'hello' }", &())
        .await
        .unwrap();

    assert_eq!(
        res,
        Foo {
            hello: "hello".into(),
            world: 42
        }
    );

    #[derive(Debug, PartialEq, Queryable)]
    struct Bar {
        foo: Foo,
        baz: i64,
    }

    let res = client
        .query_required_single::<Bar, _>(
            "select { baz := 3, foo := { world := 42, hello := 'hello' } }",
            &(),
        )
        .await
        .unwrap();

    assert_eq!(
        res,
        Bar {
            foo: Foo {
                hello: "hello".into(),
                world: 42
            },
            baz: 3
        }
    );

    Ok(())
}
