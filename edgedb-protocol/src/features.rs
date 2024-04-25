#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProtocolVersion {
    pub(crate) major_ver: u16,
    pub(crate) minor_ver: u16,
}

impl ProtocolVersion {
    pub fn current() -> ProtocolVersion {
        ProtocolVersion {
            major_ver: 1,
            minor_ver: 0,
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
    pub fn is_1(&self) -> bool {
        self.major_ver >= 1
    }
    pub fn supports_inline_typenames(&self) -> bool {
        self.version_tuple() >= (0, 9)
    }
    pub fn has_implicit_tid(&self) -> bool {
        self.version_tuple() <= (0, 8)
    }
    pub fn has_implicit_id(&self) -> bool {
        // Some of pre 1.0 protocols required implicit id.
        // Later it was opt out. In 1.0 it's opt-in.
        // We never opt-in or opt-out so whether it's present only on pre 1.0
        // portocols.
        !self.is_1()
    }
    pub fn is_at_least(&self, major_ver: u16, minor_ver: u16) -> bool {
        self.major_ver > major_ver || self.major_ver == major_ver && self.minor_ver >= minor_ver
    }
    pub fn is_at_most(&self, major_ver: u16, minor_ver: u16) -> bool {
        self.major_ver < major_ver || self.major_ver == major_ver && self.minor_ver <= minor_ver
    }
}
