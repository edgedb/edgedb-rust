#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProtocolVersion {
    pub(crate) major_ver: u16,
    pub(crate) minor_ver: u16,
}

impl ProtocolVersion {
    pub fn current() -> ProtocolVersion {
        ProtocolVersion {
            major_ver: 0,
            minor_ver: 10,
        }
    }
    pub fn new(major_ver: u16, minor_ver: u16) -> ProtocolVersion {
        ProtocolVersion {
            major_ver,
            minor_ver,
        }
    }
    pub fn version_tuple(&self) -> (u16, u16) {
        (self.major_ver, self.minor_ver)
    }
    pub fn supports_inline_typenames(&self) -> bool {
        self.version_tuple() >= (0, 9)
    }
    pub fn has_implicit_tid(&self) -> bool {
        self.version_tuple() <= (0, 8)
    }
}
