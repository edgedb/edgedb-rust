#[cfg(feature = "with-serde")]
use crate::encoding::Annotations;

/// CommandDataDescription1 may contain "warnings" annotations, whose value is
/// a JSON array of this [Warning] type.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "with-serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Warning {
    /// User-friendly explanation of the problem
    message: String,

    /// Name of the Python exception class
    r#type: String,

    /// Machine-friendly exception id
    code: u64,

    /// Name of the source file that caused the warning.
    filename: Option<String>,

    /// Additional user-friendly info
    hint: Option<String>,

    /// Developer-friendly explanation of why this problem occured
    details: Option<String>,

    /// Inclusive 0-based position within the source
    start: Option<i64>,

    /// Exclusive 0-based position within the source
    end: Option<i64>,

    /// 1-based index of the line of the start
    line: Option<i64>,

    /// 1-based index of the column of the start
    col: Option<i64>,
}

#[cfg(feature = "with-serde")]
pub fn decode_warnings(annotations: &Annotations) -> Result<Vec<Warning>, edgedb_errors::Error> {
    use edgedb_errors::{ErrorKind, ProtocolEncodingError};

    const ANN_NAME: &'static str = "warnings";

    if let Some(warnings) = annotations.get(ANN_NAME) {
        serde_json::from_str::<Vec<_>>(&warnings).map_err(|e| {
            ProtocolEncodingError::with_source(e)
                .context("Invalid JSON while decoding 'warnings' annotation")
                .into()
        })
    } else {
        Ok(vec![])
    }
}
