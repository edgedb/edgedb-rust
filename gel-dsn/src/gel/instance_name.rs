use std::fmt;
use std::str::FromStr;

use super::error::*;

const DOMAIN_LABEL_MAX_LENGTH: usize = 63;
const CLOUD_INSTANCE_NAME_MAX_LENGTH: usize = DOMAIN_LABEL_MAX_LENGTH - 2 + 1; // "--" -> "/"

/// Parsed Gel instance name.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum InstanceName {
    /// Instance configured locally
    Local(String),
    /// Instance running on the Gel Cloud
    Cloud {
        /// Organization name
        org_slug: String,
        /// Instance name within the organization
        name: String,
    },
}

impl InstanceName {
    pub fn cloud_address(&self, secret_key: &str) -> Result<Option<String>, ParseError> {
        let InstanceName::Cloud { org_slug, name } = self else {
            return Ok(None);
        };

        #[derive(Debug, serde::Deserialize)]
        struct Claims {
            #[serde(rename = "iss", skip_serializing_if = "Option::is_none")]
            issuer: Option<String>,
        }

        use base64::Engine;
        let claims_b64 = secret_key
            .split('.')
            .nth(1)
            .ok_or(ParseError::InvalidSecretKey(
                InvalidSecretKeyError::InvalidJwt,
            ))?;
        let claims = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(claims_b64)
            .map_err(|_| ParseError::InvalidSecretKey(InvalidSecretKeyError::InvalidJwt))?;
        let claims: Claims = serde_json::from_slice(&claims)
            .map_err(|_| ParseError::InvalidSecretKey(InvalidSecretKeyError::InvalidJwt))?;
        let dns_zone = claims.issuer.ok_or(ParseError::InvalidSecretKey(
            InvalidSecretKeyError::MissingIssuer,
        ))?;
        let org_slug = org_slug.to_lowercase();
        let name = name.to_lowercase();
        let msg = format!("{}/{}", org_slug, name);
        let checksum = crc16::State::<crc16::XMODEM>::calculate(msg.as_bytes());
        let dns_bucket = format!("c-{:02}", checksum % 100);
        Ok(Some(format!(
            "{}--{}.{}.i.{}",
            name, org_slug, dns_bucket, dns_zone
        )))
    }
}

fn is_valid_local_instance_name(name: &str) -> bool {
    // For local instance names:
    //  1. Allow only letters, numbers, underscores and single dashes
    //  2. Must not start or end with a dash
    // regex: ^[a-zA-Z_0-9]+(-[a-zA-Z_0-9]+)*$
    let mut chars = name.chars();
    match chars.next() {
        Some(c) if c.is_ascii_alphanumeric() || c == '_' => {}
        _ => return false,
    }
    let mut was_dash = false;
    for c in chars {
        if c == '-' {
            if was_dash {
                return false;
            } else {
                was_dash = true;
            }
        } else {
            if !c.is_ascii_alphanumeric() && c != '_' {
                return false;
            }
            was_dash = false;
        }
    }
    !was_dash
}

fn is_valid_cloud_instance_name(name: &str) -> bool {
    // For cloud instance name part:
    //  1. Allow only letters, numbers and single dashes
    //  2. Must not start or end with a dash
    // regex: ^[a-zA-Z0-9]+(-[a-zA-Z0-9]+)*$
    let mut chars = name.chars();
    match chars.next() {
        Some(c) if c.is_ascii_alphanumeric() => {}
        _ => return false,
    }
    let mut was_dash = false;
    for c in chars {
        if c == '-' {
            if was_dash {
                return false;
            } else {
                was_dash = true;
            }
        } else {
            if !c.is_ascii_alphanumeric() {
                return false;
            }
            was_dash = false;
        }
    }
    !was_dash
}

fn is_valid_cloud_org_name(name: &str) -> bool {
    // For cloud organization slug part:
    //  1. Allow only letters, numbers, underscores and single dashes
    //  2. Must not end with a dash
    // regex: ^-?[a-zA-Z0-9_]+(-[a-zA-Z0-9]+)*$
    let mut chars = name.chars();
    match chars.next() {
        Some(c) if c.is_ascii_alphanumeric() || c == '-' || c == '_' => {}
        _ => return false,
    }
    let mut was_dash = false;
    for c in chars {
        if c == '-' {
            if was_dash {
                return false;
            } else {
                was_dash = true;
            }
        } else {
            if !(c.is_ascii_alphanumeric() || c == '_') {
                return false;
            }
            was_dash = false;
        }
    }
    !was_dash
}

impl fmt::Display for InstanceName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            InstanceName::Local(name) => name.fmt(f),
            InstanceName::Cloud { org_slug, name } => write!(f, "{}/{}", org_slug, name),
        }
    }
}

impl FromStr for InstanceName {
    type Err = ParseError;

    fn from_str(name: &str) -> Result<InstanceName, Self::Err> {
        if let Some((org_slug, instance_name)) = name.split_once('/') {
            if !is_valid_cloud_instance_name(instance_name) {
                return Err(ParseError::InvalidInstanceName(
                    InstanceNameError::InvalidCloudInstanceName,
                ));
            }
            if !is_valid_cloud_org_name(org_slug) {
                return Err(ParseError::InvalidInstanceName(
                    InstanceNameError::InvalidCloudOrgName,
                ));
            }
            if name.len() > CLOUD_INSTANCE_NAME_MAX_LENGTH {
                return Err(ParseError::InvalidInstanceName(
                    InstanceNameError::InvalidCloudInstanceName,
                ));
            }
            Ok(InstanceName::Cloud {
                org_slug: org_slug.into(),
                name: instance_name.into(),
            })
        } else {
            if !is_valid_local_instance_name(name) {
                return Err(ParseError::InvalidInstanceName(
                    InstanceNameError::InvalidInstanceName,
                ));
            }
            Ok(InstanceName::Local(name.into()))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_instance_name() {
        let instance_name = InstanceName::from_str("my-instance").unwrap();
        assert_eq!(instance_name.to_string(), "my-instance");
    }

    #[test]
    fn test_invalid_instance_name() {
        let instance_name = InstanceName::from_str(
            "123456789012345678901234567890123456789012345678901234567890/1234",
        )
        .unwrap_err();
        assert_eq!(
            instance_name,
            ParseError::InvalidInstanceName(InstanceNameError::InvalidCloudInstanceName)
        );
        let instance_name = InstanceName::from_str(
            "12345678901234567890123456789012/34567890123456789012345678901234",
        )
        .unwrap_err();
        assert_eq!(
            instance_name,
            ParseError::InvalidInstanceName(InstanceNameError::InvalidCloudInstanceName)
        );
    }

    #[test]
    fn test_instance_names() {
        for inst_name in [
            "abc",
            "_localdev",
            "123",
            "___",
            "12345678901234567890123456789012345678901234567890123456789012345678901234567890",
            "abc-123",
            "a-b-c_d-e-f",
            "_-_-_-_",
            "abc/def",
            "123/456",
            "abc-123/def-456",
            "123-abc/456-def",
            "a-b-c/1-2-3",
            "-leading-dash/abc",
            "_leading-underscore/abc",
            "under_score/abc",
            "-vicfg-hceTeOuz6iXr3vkXPf0Wsudd/test123",
        ] {
            match InstanceName::from_str(inst_name) {
                Ok(InstanceName::Local(name)) => assert_eq!(name, inst_name),
                Ok(InstanceName::Cloud { org_slug, name }) => {
                    let (o, i) = inst_name
                        .split_once('/')
                        .expect("test case must have one slash");
                    assert_eq!(org_slug, o);
                    assert_eq!(name, i);
                }
                Err(e) => panic!("{:#}", e),
            }
        }
        for name in [
            "",
            "-leading-dash",
            "trailing-dash-",
            "double--dash",
            "trailing-dash-/abc",
            "double--dash/abc",
            "abc/-leading-dash",
            "abc/trailing-dash-",
            "abc/double--dash",
            "abc/_localdev",
            "123/45678901234567890123456789012345678901234567890123456789012345678901234567890",
        ] {
            assert!(
                InstanceName::from_str(name).is_err(),
                "unexpected success: {}",
                name
            );
        }
    }
}
