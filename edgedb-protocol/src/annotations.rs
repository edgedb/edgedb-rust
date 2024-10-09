#[cfg(feature = "with-serde")]
use crate::encoding::Annotations;

/// CommandDataDescription1 may contain "warnings" annotations, whose value is
/// a JSON array of this [Warning] type.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "with-serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Warning {
    /// User-friendly explanation of the problem
    pub message: String,

    /// Name of the Python exception class
    pub r#type: String,

    /// Machine-friendly exception id
    pub code: u64,

    /// Name of the source file that caused the warning.
    pub filename: Option<String>,

    /// Additional user-friendly info
    pub hint: Option<String>,

    /// Developer-friendly explanation of why this problem occured
    pub details: Option<String>,

    /// Inclusive 0-based position within the source
    #[cfg_attr(
        feature = "with-serde",
        serde(deserialize_with = "deserialize_i64_from_str")
    )]
    pub start: Option<i64>,

    /// Exclusive 0-based position within the source
    #[cfg_attr(
        feature = "with-serde",
        serde(deserialize_with = "deserialize_i64_from_str")
    )]
    pub end: Option<i64>,

    /// 1-based index of the line of the start
    #[cfg_attr(
        feature = "with-serde",
        serde(deserialize_with = "deserialize_i64_from_str")
    )]
    pub line: Option<i64>,

    /// 1-based index of the column of the start
    #[cfg_attr(
        feature = "with-serde",
        serde(deserialize_with = "deserialize_i64_from_str")
    )]
    pub col: Option<i64>,
}

impl std::fmt::Display for Warning {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Warning {
            filename,
            line,
            col,
            r#type,
            message,
            ..
        } = self;
        let filename = filename
            .as_ref()
            .map(|f| format!("{f}:"))
            .unwrap_or_default();
        let line = line.clone().unwrap_or(1);
        let col = col.clone().unwrap_or(1);

        write!(f, "{type} at {filename}{line}:{col} {message}")
    }
}

#[cfg(feature = "with-serde")]
pub fn decode_warnings(annotations: &Annotations) -> Result<Vec<Warning>, edgedb_errors::Error> {
    use edgedb_errors::{ErrorKind, ProtocolEncodingError};

    const ANN_NAME: &str = "warnings";

    if let Some(warnings) = annotations.get(ANN_NAME) {
        serde_json::from_str::<Vec<_>>(warnings).map_err(|e| {
            ProtocolEncodingError::with_source(e)
                .context("Invalid JSON while decoding 'warnings' annotation")
        })
    } else {
        Ok(vec![])
    }
}

#[cfg(feature = "with-serde")]
fn deserialize_i64_from_str<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<i64>, D::Error> {
    use serde::Deserialize;

    #[derive(Deserialize)]
    #[serde(untagged)]
    enum StringOrInt {
        String(String),
        Number(i64),
        None,
    }

    match StringOrInt::deserialize(deserializer)? {
        StringOrInt::String(s) => s.parse::<i64>().map_err(serde::de::Error::custom).map(Some),
        StringOrInt::Number(i) => Ok(Some(i)),
        StringOrInt::None => Ok(None),
    }
}
