use crate::client::Connection;


pub(crate) enum Command {
    Release(Connection),
    ConnectionCanceled,
    /// Connection is established but we got drop (e.g. at timeout)
    /// when trying to acquire lock to get statistics updated
    ConnectionEstablished,
    Close,
}
