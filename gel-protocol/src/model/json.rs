/// A newtype for JSON received from the database
#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "with-serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Json(String);

impl Json {
    /// Create a JSON value without checking the contents.
    ///
    /// Two examples of use:
    ///
    /// 1) To construct values with the data received from the
    ///    database, because we trust database to produce valid JSON.
    ///
    /// 2) By client users who are using data that is guaranteed
    ///    to be valid JSON. If unsure, using a method such as serde_json's
    ///    [to_string](https://docs.rs/serde_json/latest/serde_json/ser/fn.to_string.html)
    ///    to construct a String is highly recommended.
    ///
    /// When used in a client query method, Gel itself will recognize if the
    /// String inside `Json` is invalid JSON by returning `InvalidValueError:
    /// invalid input syntax for type json`.
    pub fn new_unchecked(value: String) -> Json {
        Json(value)
    }
}

impl AsRef<str> for Json {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl std::ops::Deref for Json {
    type Target = str;
    fn deref(&self) -> &str {
        &self.0
    }
}

impl From<Json> for String {
    fn from(val: Json) -> Self {
        val.0
    }
}
