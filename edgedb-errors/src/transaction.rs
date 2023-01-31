use crate::Error;
use crate::traits::AsEdgedbError;

pub enum TransactionError<E> {
    Edgedb(Error),
    User(E),
}

impl<E> From<Error> for TransactionError<E> {
    fn from(e: Error) -> TransactionError<E> {
        TransactionError::Edgedb(e)
    }
}

impl<E> AsEdgedbError for TransactionError<E> {
    fn as_edgedb_error(&self) -> Option<&Error> {
        match self {
            TransactionError::Edgedb(e) => Some(e),
            TransactionError::User(_) => None,
        }
    }
}
