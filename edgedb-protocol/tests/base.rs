#[macro_export]
macro_rules! bconcat {
    ($($token: expr)*) => {
        &{
            let mut buf = ::bytes::BytesMut::new();
            $(
                buf.extend($token);
            )*
            buf
        }
    }
}
