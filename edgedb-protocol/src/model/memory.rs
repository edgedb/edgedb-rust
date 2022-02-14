use std::fmt::{Debug, Display};

/// A type for cfg::memory received from the database
#[derive(Copy, Debug, Clone, PartialEq)]
pub struct ConfigMemory(pub i64);

impl ConfigMemory {}

static KIB: i64 = 1024;
static MIB: i64 = 1024 * KIB;
static GIB: i64 = 1024 * MIB;
static TIB: i64 = 1024 * GIB;

impl Display for ConfigMemory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Use the same rendering logic we have in EdgeDB server
        // to cast cfg::memory to std::str.
        let v = self.0;
        if v >= TIB && v % TIB == 0 {
            write!(f, "{}TiB", v / TIB)
        } else if v >= GIB && v % GIB == 0 {
            write!(f, "{}GiB", v / GIB)
        } else if v >= MIB && v % MIB == 0 {
            write!(f, "{}MiB", v / MIB)
        } else if v >= KIB && v % KIB == 0 {
            write!(f, "{}KiB", v / KIB)
        } else {
            write!(f, "{}B", v)
        }
    }
}
