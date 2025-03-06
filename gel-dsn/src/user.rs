use std::{borrow::Cow, path::Path};

pub struct SystemUserProfile;

/// A trait for abstracting user profiles.
pub trait UserProfile {
    fn username(&self) -> Option<Cow<str>>;
    fn homedir(&self) -> Option<Cow<Path>>;
    fn config_dir(&self) -> Option<Cow<Path>>;
    fn data_local_dir(&self) -> Option<Cow<Path>>;
}

impl UserProfile for () {
    fn username(&self) -> Option<Cow<str>> {
        None
    }

    fn homedir(&self) -> Option<Cow<Path>> {
        None
    }

    fn config_dir(&self) -> Option<Cow<Path>> {
        None
    }

    fn data_local_dir(&self) -> Option<Cow<Path>> {
        None
    }
}
