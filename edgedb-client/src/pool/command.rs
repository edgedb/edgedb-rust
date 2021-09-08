use crate::pool::PoolConn;


pub(crate) enum Command {
    Release(PoolConn),
    Close,
}
