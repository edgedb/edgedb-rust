/// Trait that marks EdgeDB errors
///
/// Currently sealed, because edgedb errors will be changed in future
pub trait ErrorKind: Sealed {
}

pub trait Sealed {
    // TODO(tailhook) use uuids of errors instead
    fn is_superclass_of(code: u64) -> bool;
}
