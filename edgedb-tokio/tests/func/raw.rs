use bytes::Bytes;

use edgedb_tokio::raw::Pool;
use edgedb_protocol::common::{CompliationOptions, IoFormat, Cardinality};
use edgedb_protocol::common::{Capabilities};

use crate::server::SERVER;

#[tokio::test]
async fn poll_connect() -> anyhow::Result<()> {
    let pool = Pool::new(&SERVER.config);
    let mut conn = pool.acquire().await?;
    assert!(conn.is_consistent());
    let _prepare = conn.prepare(&CompliationOptions {
        implicit_limit: None,
        implicit_typenames: false,
        implicit_typeids: false,
        allow_capabilities: Capabilities::empty(),
        explicit_objectids: true,
        io_format: IoFormat::Binary,
        expected_cardinality: Cardinality::Many,
    }, "SELECT 7*8").await;
    assert!(conn.is_consistent());
    let _descr = conn.describe_data().await?;
    assert!(conn.is_consistent());
    let data = conn.execute(&Bytes::new()).await?;
    assert!(conn.is_consistent());
    assert_eq!(data.len(), 1);
    assert_eq!(data[0].data.len(), 1);
    assert_eq!(&data[0].data[0][..], b"\0\0\0\0\0\0\0\x38");
    Ok(())
}
