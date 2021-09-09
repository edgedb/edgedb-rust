use crate::client::Connection;


pub(crate) enum Command {
    Release(Connection),
    Close,
}
