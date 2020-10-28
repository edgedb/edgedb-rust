use crate::traits::{ErrorKind, Sealed};

macro_rules! define {
    ($id:ident, $mask:expr, $code:expr) => {
        pub struct $id;

        impl Sealed for $id {
            fn is_superclass_of(code: u64) -> bool {
                code & $mask == $code
            }
        }

        impl ErrorKind for $id {}
    }
}

define!(InternalServerError, 0xFF_00_00_00, 0x01_00_00_00);
define!(UnsupportedFeatureError, 0xFF_00_00_00, 0x02_00_00_00);
define!(ProtocolError, 0xFF_00_00_00, 0x03_00_00_00);
