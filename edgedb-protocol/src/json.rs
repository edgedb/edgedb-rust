#[derive(Debug, Clone)]
pub struct Json(pub(crate) String);

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
