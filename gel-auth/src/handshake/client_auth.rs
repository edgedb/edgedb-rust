use crate::{
    md5::md5_password,
    scram::{
        generate_nonce, generate_salted_password, ClientEnvironment, ClientTransaction, SCRAMError,
        Sha256Out,
    },
    AuthType, CredentialData,
};
use tracing::error;

#[derive(Debug)]
pub enum ClientAuthResponse {
    Initial(AuthType, Vec<u8>),
    Continue(Vec<u8>),
    Complete,
    Waiting,
    Error(ClientAuthError),
}

#[derive(Debug, thiserror::Error)]
pub enum ClientAuthError {
    #[error("SCRAM protocol error: {0}")]
    ScramError(#[from] SCRAMError),
    #[error("Invalid authentication state")]
    InvalidState,
    #[error("Invalid credentials")]
    InvalidCredentials,
    #[error("Unexpected message during authentication")]
    UnexpectedMessage,
}

#[derive(Debug)]
enum ClientAuthState {
    Initial(String, CredentialData),
    Complete,
    Waiting,
    Sasl(ClientTransaction, ClientEnvironmentImpl),
}

#[derive(Debug)]
pub enum ClientAuthDrive<'a> {
    /// Authentication is successful.
    Ok,
    /// Server requested plain authentication.
    Plain,
    /// Server requested MD5 authentication (with salt).
    Md5([u8; 4]),
    /// Server requested SCRAM authentication.
    Scram,
    /// Server sent SCRAM message.
    ScramResponse(&'a [u8]),
}

#[derive(Debug)]
pub struct ClientAuth {
    state: ClientAuthState,
    auth_type: Option<AuthType>,
}

impl ClientAuth {
    /// Create a new client authentication state.
    pub fn new(username: String, credentials: CredentialData) -> Self {
        Self {
            state: ClientAuthState::Initial(username, credentials),
            auth_type: None,
        }
    }

    pub fn is_complete(&self) -> bool {
        matches!(self.state, ClientAuthState::Complete)
    }

    pub fn auth_type(&self) -> Option<AuthType> {
        self.auth_type
    }

    pub fn drive(&mut self, drive: ClientAuthDrive) -> Result<ClientAuthResponse, ClientAuthError> {
        match (&mut self.state, drive) {
            (ClientAuthState::Initial(username, credentials), drive) => {
                let username = std::mem::take(username);
                let credentials = std::mem::replace(credentials, CredentialData::Deny);
                self.handle_initial(username, credentials, drive)
            }
            // SCRAM authentication: Handle SCRAM protocol messages.
            (ClientAuthState::Sasl(tx, env), ClientAuthDrive::ScramResponse(message)) => {
                let response = tx.process_message(&message, env)?;
                match response {
                    Some(response) => Ok(ClientAuthResponse::Continue(response)),
                    None => {
                        self.state = ClientAuthState::Waiting;
                        Ok(ClientAuthResponse::Waiting)
                    }
                }
            }
            (ClientAuthState::Sasl(..), _) => Err(ClientAuthError::InvalidState),

            // Handle "Ok" drive (authentication successful).
            (_, ClientAuthDrive::Ok) => {
                self.state = ClientAuthState::Complete;
                Ok(ClientAuthResponse::Complete)
            }

            // Invalid state/drive combination.
            (_, drive) => {
                error!("Received invalid drive {drive:?} in state {:?}", self.state);
                Err(ClientAuthError::InvalidState)
            }
        }
    }

    fn handle_initial(
        &mut self,
        username: String,
        credentials: CredentialData,
        drive: ClientAuthDrive,
    ) -> Result<ClientAuthResponse, ClientAuthError> {
        let (auth_type, (state, response)) = match drive {
            ClientAuthDrive::Ok => (
                AuthType::Trust,
                match credentials {
                    CredentialData::Deny => (
                        ClientAuthState::Complete,
                        ClientAuthResponse::Error(ClientAuthError::InvalidCredentials),
                    ),
                    _ => (ClientAuthState::Complete, ClientAuthResponse::Complete),
                },
            ),
            ClientAuthDrive::Plain => (
                AuthType::Plain,
                match credentials {
                    CredentialData::Plain(credentials) => (
                        ClientAuthState::Waiting,
                        ClientAuthResponse::Initial(
                            AuthType::Plain,
                            credentials.clone().into_bytes(),
                        ),
                    ),
                    _ => (
                        ClientAuthState::Complete,
                        ClientAuthResponse::Error(ClientAuthError::InvalidCredentials),
                    ),
                },
            ),
            ClientAuthDrive::Md5(salt) => (
                AuthType::Md5,
                match credentials {
                    CredentialData::Md5(credentials) => (
                        ClientAuthState::Waiting,
                        ClientAuthResponse::Initial(
                            AuthType::Md5,
                            credentials.salted(salt).into_bytes(),
                        ),
                    ),
                    CredentialData::Plain(credentials) => (
                        ClientAuthState::Waiting,
                        ClientAuthResponse::Initial(
                            AuthType::Md5,
                            md5_password(&credentials, &username, salt).into_bytes(),
                        ),
                    ),
                    _ => (
                        ClientAuthState::Complete,
                        ClientAuthResponse::Error(ClientAuthError::InvalidCredentials),
                    ),
                },
            ),
            ClientAuthDrive::Scram => (
                AuthType::ScramSha256,
                match credentials {
                    CredentialData::Plain(credentials) => {
                        let env = ClientEnvironmentImpl {
                            password: credentials,
                        };
                        let mut tx = ClientTransaction::new(username.into());
                        let response = tx.process_message(&[], &env);
                        match response {
                            Ok(Some(response)) => (
                                ClientAuthState::Sasl(tx, env),
                                ClientAuthResponse::Initial(AuthType::ScramSha256, response),
                            ),
                            Ok(None) => (
                                ClientAuthState::Complete,
                                ClientAuthResponse::Error(ClientAuthError::InvalidCredentials),
                            ),
                            Err(e) => (
                                ClientAuthState::Complete,
                                ClientAuthResponse::Error(ClientAuthError::ScramError(e)),
                            ),
                        }
                    }
                    _ => (
                        ClientAuthState::Complete,
                        ClientAuthResponse::Error(ClientAuthError::InvalidCredentials),
                    ),
                },
            ),
            _ => {
                error!("Received invalid drive {drive:?} in state Initial");
                return Err(ClientAuthError::InvalidState);
            }
        };

        self.auth_type = Some(auth_type);
        self.state = state;
        Ok(response)
    }
}

#[derive(Debug)]
struct ClientEnvironmentImpl {
    password: String,
}

impl ClientEnvironment for ClientEnvironmentImpl {
    fn generate_nonce(&self) -> String {
        generate_nonce()
    }

    fn get_salted_password(&self, salt: &[u8], iterations: usize) -> Sha256Out {
        generate_salted_password(self.password.as_bytes(), salt, iterations)
    }
}
