/// A newtype for JSON received from the database
#[derive(Debug, Clone)]
pub struct Json(String);

impl Json {
	pub(crate) fn _new_unchecked(value: String) -> Json {
		Json(value)
	}
    /// Create JSON value without checking the contents
    ///
    /// This is used to construct values from the data received from the
    /// database (we trust database to produce valid JSON).
	pub unsafe fn new_unchecked(value: String) -> Json {
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

impl Into<String> for Json {
    fn into(self) -> String {
        self.0
    }
}
