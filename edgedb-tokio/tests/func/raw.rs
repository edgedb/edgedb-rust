use std::sync::Arc;

use bytes::Bytes;

use edgedb_protocol::common::Capabilities;
use edgedb_protocol::common::{Cardinality, CompilationOptions, IoFormat, InputLanguage};
use edgedb_tokio::raw::{Pool, PoolState};

use crate::server::SERVER;

#[tokio::test]
async fn poll_connect() -> anyhow::Result<()> {
    let pool = Pool::new(&SERVER.config);
    let mut conn = pool.acquire().await?;
    assert!(conn.is_consistent());

    let state = Arc::new(PoolState::default());
    let options = CompilationOptions {
        implicit_limit: None,
        implicit_typenames: false,
        implicit_typeids: false,
        allow_capabilities: Capabilities::empty(),
        explicit_objectids: true,
        input_language: InputLanguage::EdgeQL,
        io_format: IoFormat::Binary,
        expected_cardinality: Cardinality::Many,
    };

    let desc = conn.parse(&options, "SELECT 7*8", &state).await?;
    assert!(conn.is_consistent());

    let data = conn
        .execute(&options, "SELECT 7*8", &state, &desc, &Bytes::new())
        .await?;
    assert!(conn.is_consistent());
    assert_eq!(data.len(), 1);
    assert_eq!(data[0].data.len(), 1);
    assert_eq!(&data[0].data[0][..], b"\0\0\0\0\0\0\0\x38");
    Ok(())
}
