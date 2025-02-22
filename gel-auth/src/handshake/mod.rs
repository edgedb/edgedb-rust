//! Handshake state machines for client/server authentication.

mod client_auth;
mod server_auth;

pub use client_auth::*;
pub use server_auth::*;

#[cfg(test)]
mod tests {
    use crate::{AuthType, CredentialData};

    use super::*;

    const USERNAME: &str = "username";
    const PASSWORD: &str = "password";

    #[test]
    fn test_client_server_trust() {
        let mut server = ServerAuth::new(USERNAME.into(), AuthType::Trust, CredentialData::Trust);
        let mut client = ClientAuth::new(USERNAME.into(), CredentialData::Trust);

        let ServerAuthResponse::Complete(message) = server.drive(ServerAuthDrive::Initial) else {
            panic!("Server auth should complete");
        };

        assert!(message.is_empty());

        let Ok(ClientAuthResponse::Complete) = client.drive(ClientAuthDrive::Ok) else {
            panic!("Client auth should complete");
        };

        assert!(server.is_complete());
        assert!(client.is_complete());
    }

    #[test]
    fn test_client_server_plain() {
        let mut server = ServerAuth::new(
            USERNAME.into(),
            AuthType::Plain,
            CredentialData::Plain(PASSWORD.into()),
        );
        let mut client = ClientAuth::new(USERNAME.into(), CredentialData::Plain(PASSWORD.into()));

        let ServerAuthResponse::Initial(AuthType::Plain, message) =
            server.drive(ServerAuthDrive::Initial)
        else {
            panic!("Server auth should ask for plain password");
        };

        assert!(message.is_empty());

        let Ok(ClientAuthResponse::Initial(AuthType::Plain, message)) =
            client.drive(ClientAuthDrive::Plain)
        else {
            panic!("Client auth should send plain password");
        };

        let ServerAuthResponse::Complete(message) =
            server.drive(ServerAuthDrive::Message(AuthType::Plain, &message))
        else {
            panic!("Server auth should complete");
        };

        assert!(message.is_empty());

        let Ok(ClientAuthResponse::Complete) = client.drive(ClientAuthDrive::Ok) else {
            panic!("Client auth should complete");
        };

        assert!(server.is_complete());
        assert!(client.is_complete());
    }

    #[test]
    fn test_client_server_md5() {
        let mut server = ServerAuth::new(
            USERNAME.into(),
            AuthType::Md5,
            CredentialData::Plain(PASSWORD.into()),
        );
        let mut client = ClientAuth::new(USERNAME.into(), CredentialData::Plain(PASSWORD.into()));

        let ServerAuthResponse::Initial(AuthType::Md5, salt) =
            server.drive(ServerAuthDrive::Initial)
        else {
            panic!("Server auth should ask for MD5 password");
        };

        let salt_array: [u8; 4] = salt.try_into().unwrap();

        let Ok(ClientAuthResponse::Initial(AuthType::Md5, message)) =
            client.drive(ClientAuthDrive::Md5(salt_array))
        else {
            panic!("Client auth should send MD5 password");
        };

        let ServerAuthResponse::Complete(message) =
            server.drive(ServerAuthDrive::Message(AuthType::Md5, &message))
        else {
            panic!("Server auth should complete");
        };

        assert!(message.is_empty());

        let Ok(ClientAuthResponse::Complete) = client.drive(ClientAuthDrive::Ok) else {
            panic!("Client auth should complete");
        };

        assert!(server.is_complete());
        assert!(client.is_complete());
    }

    #[test]
    fn test_client_server_scram() {
        let mut server = ServerAuth::new(
            USERNAME.into(),
            AuthType::ScramSha256,
            CredentialData::Plain(PASSWORD.into()),
        );
        let mut client = ClientAuth::new(USERNAME.into(), CredentialData::Plain(PASSWORD.into()));

        let ServerAuthResponse::Initial(AuthType::ScramSha256, message) =
            server.drive(ServerAuthDrive::Initial)
        else {
            panic!("Server auth should ask for SCRAM password");
        };

        assert!(message.is_empty());

        let Ok(ClientAuthResponse::Initial(AuthType::ScramSha256, message)) =
            client.drive(ClientAuthDrive::Scram)
        else {
            panic!("Client auth should send SCRAM password");
        };

        let ServerAuthResponse::Continue(message) =
            server.drive(ServerAuthDrive::Message(AuthType::ScramSha256, &message))
        else {
            panic!("Server auth should continue");
        };

        let Ok(ClientAuthResponse::Continue(message)) =
            client.drive(ClientAuthDrive::ScramResponse(&message))
        else {
            panic!("Client auth should continue");
        };

        let ServerAuthResponse::Complete(message) =
            server.drive(ServerAuthDrive::Message(AuthType::ScramSha256, &message))
        else {
            panic!("Server auth should complete");
        };

        let Ok(ClientAuthResponse::Waiting) =
            client.drive(ClientAuthDrive::ScramResponse(&message))
        else {
            panic!("Client auth should wait");
        };

        let Ok(ClientAuthResponse::Complete) = client.drive(ClientAuthDrive::Ok) else {
            panic!("Client auth should complete");
        };

        assert!(server.is_complete());
        assert!(client.is_complete());
    }
}
