use std::{
    collections::HashMap,
    path::{Path, PathBuf},
};

pub struct SystemFileAccess;

/// A trait for abstracting the reading of files.
pub trait FileAccess {
    fn default() -> impl FileAccess {
        SystemFileAccess
    }

    fn read(&self, path: &Path) -> Result<String, std::io::Error>;

    fn cwd(&self) -> Option<PathBuf> {
        None
    }

    fn exists(&self, path: &Path) -> Result<bool, std::io::Error> {
        match self.read(path) {
            Ok(_) => Ok(true),
            Err(e) => {
                if e.kind() == std::io::ErrorKind::NotFound {
                    Ok(false)
                } else {
                    Err(e)
                }
            }
        }
    }

    fn exists_dir(&self, path: &Path) -> Result<bool, std::io::Error>;

    fn canonicalize(&self, path: &Path) -> Result<PathBuf, std::io::Error> {
        Ok(path.to_path_buf())
    }
}

impl FileAccess for &[(&Path, &str)] {
    fn read(&self, path: &Path) -> Result<String, std::io::Error> {
        self.iter()
            .find(|(key, _)| *key == path)
            .map(|(_, value)| value.to_string())
            .ok_or(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "File not found",
            ))
    }

    fn exists_dir(&self, path: &Path) -> Result<bool, std::io::Error> {
        Ok(self.iter().any(|(key, _)| key.starts_with(path)))
    }
}

impl<K, V> FileAccess for HashMap<K, V>
where
    K: std::hash::Hash + Eq + std::borrow::Borrow<Path>,
    V: std::borrow::Borrow<str>,
{
    fn read(&self, name: &Path) -> Result<String, std::io::Error> {
        self.get(name)
            .map(|value| value.borrow().into())
            .ok_or(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "File not found",
            ))
    }

    fn exists_dir(&self, path: &Path) -> Result<bool, std::io::Error> {
        Ok(self.iter().any(|(key, _)| key.borrow().starts_with(path)))
    }
}

impl FileAccess for () {
    fn read(&self, _: &Path) -> Result<String, std::io::Error> {
        Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "File not found",
        ))
    }

    fn exists_dir(&self, _: &Path) -> Result<bool, std::io::Error> {
        Ok(false)
    }
}

impl FileAccess for SystemFileAccess {
    fn read(&self, path: &Path) -> Result<String, std::io::Error> {
        use std::io::Read;
        let mut file = std::fs::File::open(path)?;
        let mut contents = String::new();
        file.read_to_string(&mut contents)?;
        Ok(contents)
    }

    fn cwd(&self) -> Option<PathBuf> {
        std::env::current_dir().ok()
    }

    fn exists(&self, path: &Path) -> Result<bool, std::io::Error> {
        std::fs::exists(path)
    }

    fn exists_dir(&self, path: &Path) -> Result<bool, std::io::Error> {
        std::fs::metadata(path).map(|metadata| metadata.is_dir())
    }

    fn canonicalize(&self, path: &Path) -> Result<PathBuf, std::io::Error> {
        std::fs::canonicalize(path)
    }
}
