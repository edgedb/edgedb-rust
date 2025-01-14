use gel_errors::Error;

use crate::server_message::ErrorResponse;

impl From<ErrorResponse> for Error {
    fn from(val: ErrorResponse) -> Self {
        Error::from_code(val.code)
            .context(val.message)
            .with_headers(val.attributes)
    }
}
