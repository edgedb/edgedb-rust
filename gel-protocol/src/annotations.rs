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
    #[cfg_attr(feature = "with-serde", serde(default))]
    pub filename: Option<String>,

    /// Additional user-friendly info
    #[cfg_attr(feature = "with-serde", serde(default))]
    pub hint: Option<String>,

    /// Developer-friendly explanation of why this problem occured
    #[cfg_attr(feature = "with-serde", serde(default))]
    pub details: Option<String>,

    /// Inclusive 0-based position within the source
    #[cfg_attr(
        feature = "with-serde",
        serde(deserialize_with = "deserialize_usize_from_str", default)
    )]
    pub start: Option<usize>,

    /// Exclusive 0-based position within the source
    #[cfg_attr(
        feature = "with-serde",
        serde(deserialize_with = "deserialize_usize_from_str", default)
    )]
    pub end: Option<usize>,

    /// 1-based index of the line of the start
    #[cfg_attr(
        feature = "with-serde",
        serde(deserialize_with = "deserialize_usize_from_str", default)
    )]
    pub line: Option<usize>,

    /// 1-based index of the column of the start
    #[cfg_attr(
        feature = "with-serde",
        serde(deserialize_with = "deserialize_usize_from_str", default)
    )]
    pub col: Option<usize>,
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
        let line = (*line).unwrap_or(1);
        let col = (*col).unwrap_or(1);

        write!(f, "{type} at {filename}{line}:{col} {message}")
    }
}

#[cfg(feature = "with-serde")]
pub fn decode_warnings(annotations: &Annotations) -> Result<Vec<Warning>, gel_errors::Error> {
    use gel_errors::{ErrorKind, ProtocolEncodingError};

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
fn deserialize_usize_from_str<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<usize>, D::Error> {
    use serde::Deserialize;

    #[derive(Deserialize)]
    #[serde(untagged)]
    enum StringOrInt {
        String(String),
        Number(usize),
    }

    Option::<StringOrInt>::deserialize(deserializer)?
        .map(|x| match x {
            StringOrInt::String(s) => s.parse::<usize>().map_err(serde::de::Error::custom),
            StringOrInt::Number(i) => Ok(i),
        })
        .transpose()
}

#[test]
#[cfg(feature = "with-serde")]
fn deserialize_warning() {
    let a: Warning =
        serde_json::from_str(r#"{"message": "a", "type": "WarningException", "code": 1}"#).unwrap();
    assert_eq!(
        a,
        Warning {
            message: "a".to_string(),
            r#type: "WarningException".to_string(),
            code: 1,
            filename: None,
            hint: None,
            details: None,
            start: None,
            end: None,
            line: None,
            col: None
        }
    );

    let a: Warning = serde_json::from_str(
        r#"{"message": "a", "type": "WarningException", "code": 1, "start": null}"#,
    )
    .unwrap();
    assert_eq!(
        a,
        Warning {
            message: "a".to_string(),
            r#type: "WarningException".to_string(),
            code: 1,
            filename: None,
            hint: None,
            details: None,
            start: None,
            end: None,
            line: None,
            col: None
        }
    );

    let a: Warning = serde_json::from_str(
        r#"{"message": "a", "type": "WarningException", "code": 1, "start": 23}"#,
    )
    .unwrap();
    assert_eq!(
        a,
        Warning {
            message: "a".to_string(),
            r#type: "WarningException".to_string(),
            code: 1,
            filename: None,
            hint: None,
            details: None,
            start: Some(23),
            end: None,
            line: None,
            col: None
        }
    );

    let a: Warning = serde_json::from_str(
        r#"{"message": "a", "type": "WarningException", "code": 1, "start": "23"}"#,
    )
    .unwrap();
    assert_eq!(
        a,
        Warning {
            message: "a".to_string(),
            r#type: "WarningException".to_string(),
            code: 1,
            filename: None,
            hint: None,
            details: None,
            start: Some(23),
            end: None,
            line: None,
            col: None
        }
    );
}
