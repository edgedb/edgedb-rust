use std::{
    borrow::Cow,
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::Duration,
};

use gel_dsn::{
    gel::{Builder, ConnectionOptions, Params, Traces, Warnings},
    EnvVar, FileAccess, UserProfile,
};
use serde::{Deserialize, Serialize};
use sha1::{Digest, Sha1};

const JSON: &str = include_str!("shared-client-testcases/connection_testcases.json");

#[derive(Debug, Serialize, Deserialize)]
struct ConnectionTestcase {
    name: String,
    #[serde(default)]
    opts: Option<ConnectionOptions>,
    #[serde(default)]
    env: Option<HashMap<String, String>>,
    #[serde(default)]
    fs: Option<Fs>,
    #[serde(flatten)]
    outcome: TestOutcome,
    platform: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, derive_more::Display)]
#[serde(untagged)]
enum StringOrNumber {
    #[display("{}", _0)]
    String(String),
    #[display("{}", _0)]
    Number(f64),
}

#[derive(Debug, Serialize, Deserialize)]
struct Fs {
    files: Option<HashMap<String, serde_json::Value>>,
    cwd: Option<String>,
    homedir: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq)]
struct TestResult {
    address: (String, usize),
    branch: String,
    database: String,
    password: Option<String>,
    #[serde(rename = "secretKey")]
    secret_key: Option<String>,
    #[serde(rename = "serverSettings")]
    server_settings: serde_json::Value,
    #[serde(rename = "tlsCAData")]
    tls_ca_data: Option<String>,
    #[serde(rename = "tlsSecurity")]
    tls_security: String,
    #[serde(rename = "tlsServerName")]
    tls_server_name: Option<String>,
    user: String,
    #[serde(
        rename = "waitUntilAvailable",
        deserialize_with = "deserialize_duration",
        serialize_with = "serialize_duration"
    )]
    wait_until_available: Duration,
}

fn deserialize_duration<'de, D>(deserializer: D) -> Result<Duration, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let duration_str: &str = serde::Deserialize::deserialize(deserializer)?;
    let duration = gel_dsn::gel::parse_duration(duration_str).map_err(serde::de::Error::custom)?;
    Ok(duration)
}

fn serialize_duration<S>(duration: &Duration, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    let duration = gel_dsn::gel::format_duration(duration);
    serializer.serialize_str(&duration)
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq)]
struct TestError {
    r#type: String,
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq)]
enum TestOutcome {
    #[serde(rename = "result")]
    Result(TestResult),
    #[serde(rename = "error")]
    Error(TestError),
}

impl FileAccess for &ConnectionTestcase {
    fn read(&self, path: &Path) -> Result<String, std::io::Error> {
        if let Some(fs) = &self.fs {
            if let Some(files) = &fs.files {
                if let Some(content) = files.get(path.to_str().unwrap()) {
                    if content.is_string() {
                        return Ok(content.as_str().unwrap().to_string());
                    }
                }
                if let Some(parent) = files.get(path.parent().unwrap().to_str().unwrap()) {
                    let parent = parent.as_object().unwrap();
                    if let Some(content) = parent.get(path.file_name().unwrap().to_str().unwrap()) {
                        if content.is_string() {
                            return Ok(content.as_str().unwrap().to_string());
                        }
                    }
                }
            }
        }
        Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "File not found",
        ))
    }

    fn exists_dir(&self, path: &Path) -> Result<bool, std::io::Error> {
        if let Some(fs) = &self.fs {
            if let Some(files) = &fs.files {
                return Ok(files
                    .iter()
                    .any(|(key, _)| Path::new(key).starts_with(path)));
            }
        }
        Ok(false)
    }

    fn cwd(&self) -> Option<PathBuf> {
        if let Some(fs) = &self.fs {
            if let Some(cwd) = &fs.cwd {
                return Some(PathBuf::from(cwd));
            }
        }
        None
    }
}

impl EnvVar for &ConnectionTestcase {
    fn read(&self, name: &str) -> Result<Cow<'static, str>, std::env::VarError> {
        if let Some(env) = &self.env {
            if let Some(value) = env.get(name) {
                return Ok(Cow::Owned(value.to_string()));
            }
        }
        Err(std::env::VarError::NotPresent)
    }
}

impl UserProfile for &ConnectionTestcase {
    fn config_dir(&self) -> Option<Cow<Path>> {
        if self.platform.as_deref() == Some("macos") {
            Some(Cow::Borrowed(Path::new(
                "/Users/edgedb/Library/Application Support",
            )))
        } else if self.platform.as_deref() == Some("windows") {
            Some(Cow::Borrowed(Path::new(r#"C:\Users\edgedb\AppData\Local"#)))
        } else {
            Some(Cow::Borrowed(Path::new("/home/edgedb/.config")))
        }
    }

    fn data_local_dir(&self) -> Option<Cow<Path>> {
        Some(Cow::Borrowed(Path::new(
            "/home/edgedb/Library/Application Support/edgedb",
        )))
    }

    fn homedir(&self) -> Option<Cow<Path>> {
        Some(Cow::Borrowed(Path::new("/home/edgedb")))
    }

    fn username(&self) -> Option<Cow<str>> {
        Some(Cow::Borrowed("edgedb"))
    }
}

fn main() {
    let testcases: Vec<ConnectionTestcase> = serde_json::from_str(JSON).unwrap();
    let mut failed = 0;
    let mut passed = 0;
    let mut skipped = 0;
    let filter = std::env::args().nth(1).unwrap_or_default();

    for mut testcase in testcases {
        if !testcase.name.contains(&filter) {
            skipped += 1;
            continue;
        }

        #[cfg(not(windows))]
        if testcase.platform.as_deref() == Some("windows") {
            println!("Skipping Windows-only testcase: {}", testcase.name);
            continue;
        }

        #[cfg(not(unix))]
        if testcase.platform.as_deref() == Some("macos") || testcast.platform.is_none() {
            println!("Skipping Unix-only testcase: {}", testcase.name);
            continue;
        }

        if let TestOutcome::Result(a) = &mut testcase.outcome {
            if a.address.0.contains("%") {
                if let Some(opts) = &testcase.opts {
                    if opts.dsn.as_ref().unwrap_or(&"".to_string()).contains("%") {
                        println!("Fuzzy match: {} omitting ipv6 scope", testcase.name);
                        a.address.0 = a.address.0.split_once('%').unwrap().0.to_string();
                        continue;
                    }
                }
            }
        }

        let expected = match &testcase.outcome {
            TestOutcome::Result(a) => serde_json::to_string_pretty(a).unwrap(),
            TestOutcome::Error(a) => serde_json::to_string_pretty(a).unwrap(),
        };

        let traces = Arc::new(Mutex::new(Vec::new()));

        let project = if let Some(fs) = &testcase.fs {
            if let Some(files) = &fs.files {
                files
                    .keys()
                    .find(|k| k.ends_with("edgedb.toml") || k.ends_with("gel.toml"))
                    .map(PathBuf::from)
            } else {
                None
            }
        } else {
            None
        };

        if let Some(fs) = &mut testcase.fs {
            if let Some(files) = fs.files.take() {
                let entries = files.into_iter().collect::<Vec<_>>();
                let mut files = HashMap::new();
                for (mut key, value) in entries.into_iter() {
                    if key.contains("${HASH}") {
                        if let Some(project) = &project {
                            fn hash_path(path: &Path) -> String {
                                let mut hasher = Sha1::new();
                                hasher.update(path.as_os_str().as_encoded_bytes());
                                format!("{:x}", hasher.finalize())
                            }
                            let project_dir = project.parent().unwrap();
                            let hash = hash_path(project_dir);
                            traces.lock().unwrap().push(format!(
                                "Hashing parent of {:?} ({:?}) to {}",
                                project_dir,
                                project_dir.parent(),
                                hash
                            ));
                            key = key.replace("${HASH}", &hash);
                            traces.lock().unwrap().push(format!("Hashed path: {}", key));
                        } else {
                            panic!("No project directory found for testcase but ${{HASH}} is present: {}", testcase.name);
                        }
                    }
                    files.insert(key, value);
                }
                fs.files = Some(files);
            }
        }

        let warnings = Warnings::default();
        let traces = Traces::default();

        let result = 'block: {
            let params = testcase.opts.clone().unwrap_or_default();
            let params: Params = match params.try_into() {
                Ok(params) => params,
                Err(e) => {
                    break 'block Err(TestError {
                        r#type: e.error_type().to_string(),
                    });
                }
            };
            let result = Builder::default()
                .params(params)
                .with_system_impl(&testcase)
                .with_auto_project_cwd()
                .with_tracing(traces.clone().trace_fn())
                .with_warning(warnings.clone().warn_fn())
                .build_parse_error()
                .map_err(|e| TestError {
                    r#type: e.error_type().to_string(),
                });
            result
        };

        let actual = match &result {
            Ok(config) => serde_json::to_string_pretty(&config.to_json()).unwrap(),
            Err(e) => serde_json::to_string_pretty(&e).unwrap(),
        };

        let mut fuzzy_match = false;
        if testcase.outcome
            == TestOutcome::Error(TestError {
                r#type: "invalid_dsn_or_instance_name".to_string(),
            })
            && (result
                == Err(TestError {
                    r#type: "invalid_dsn".to_string(),
                })
                || result
                    == Err(TestError {
                        r#type: "invalid_instance_name".to_string(),
                    }))
        {
            println!("Fuzzy match: {}", testcase.name);
            fuzzy_match = true;
        }

        if actual == expected || fuzzy_match {
            passed += 1;
            traces.trace(&format!("Passed: {}", testcase.name));
        } else {
            failed += 1;
            traces.trace(&format!("Failed: {}", testcase.name));

            println!("---------------------------------------------");
            for trace in traces.into_vec() {
                println!("{}", trace);
            }
            for warning in warnings.into_vec() {
                println!("{}", warning);
            }
            println!(
                "Failed: {}",
                pretty_assertions::StrComparison::new(&expected, &actual)
            );
        }
    }

    println!("Passed: {}", passed);
    println!("Failed: {}", failed);
    println!("Skipped: {}", skipped);

    if failed > 0 {
        std::process::exit(1);
    }
}
