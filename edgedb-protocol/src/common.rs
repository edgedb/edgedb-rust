#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum Cardinality {
    NoResult = 0x6e,
    One = 0x6f,
    Many = 0x6d,
}

