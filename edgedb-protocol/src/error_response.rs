use edgedb_errors::Error;

use crate::server_message::ErrorResponse;

impl Into<Error> for ErrorResponse {
    fn into(self) -> Error {
        Error::from_code(self.code)
            .context(self.message)
            .with_headers(self.attributes)
    }
}
