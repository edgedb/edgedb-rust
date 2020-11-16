#[derive(Clone)]
pub struct ProtocolVersion {
    pub(crate) major_ver: u16,
    pub(crate) minor_ver: u16,
}

impl ProtocolVersion {
    pub fn current() -> ProtocolVersion {
        ProtocolVersion {
            major_ver: 0,
            minor_ver: 9,
        }
    }
    fn ver(&self) -> (u16, u16) {
        (self.major_ver, self.minor_ver)
    }
    pub fn supports_inline_typenames(&self) -> bool {
        self.ver() >= (0, 9)
    }
    pub fn has_implicit_tid(&self) -> bool {
        self.ver() <= (0, 8)
    }
}
