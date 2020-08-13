#[derive(Debug, thiserror::Error)]
#[error("Connection is inconsistent state. Please reconnect.")]
pub struct ConnectionDirty;

#[derive(Debug, thiserror::Error)]
#[error("Password required for the specified user/host")]
pub struct PasswordRequired;
