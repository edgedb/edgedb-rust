use std::borrow::Cow;
use std::collections::HashMap;

pub struct SystemEnvVars;

/// A trait for abstracting the reading of environment variables.
///
/// By default, uses `std::env::Vars` but can be re-implemented for other
/// sources.
pub trait EnvVar {
    fn default() -> impl EnvVar {
        SystemEnvVars
    }
    fn read(&self, name: &str) -> Result<Cow<str>, std::env::VarError>;
}

impl<K, V> EnvVar for HashMap<K, V>
where
    K: std::hash::Hash + Eq + std::borrow::Borrow<str>,
    V: std::borrow::Borrow<str>,
{
    fn read(&self, name: &str) -> Result<Cow<str>, std::env::VarError> {
        self.get(name)
            .map(|value| value.borrow().into())
            .ok_or(std::env::VarError::NotPresent)
    }
}

impl EnvVar for SystemEnvVars {
    fn read(&self, name: &str) -> Result<Cow<str>, std::env::VarError> {
        if let Ok(value) = std::env::var(name) {
            Ok(value.into())
        } else {
            Err(std::env::VarError::NotPresent)
        }
    }
}

impl EnvVar for &[(&str, &str)] {
    fn read(&self, name: &str) -> Result<Cow<str>, std::env::VarError> {
        for (key, value) in self.iter() {
            if *key == name {
                return Ok((*value).into());
            }
        }
        Err(std::env::VarError::NotPresent)
    }
}

impl EnvVar for () {
    fn read(&self, _: &str) -> Result<Cow<str>, std::env::VarError> {
        Err(std::env::VarError::NotPresent)
    }
}

impl<T> EnvVar for &T
where
    T: EnvVar,
{
    fn read(&self, name: &str) -> Result<Cow<str>, std::env::VarError> {
        (*self).read(name)
    }
}
