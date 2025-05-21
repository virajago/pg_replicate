use std::fmt::Debug;

use thiserror::Error;

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SourceSettings {
    Postgres {
        /// Host on which Postgres is running
        host: String,

        /// Port on which Postgres is running
        port: u16,

        /// Postgres database name
        name: String,

        /// Postgres database user name
        username: String,

        /// Postgres database user password
        password: Option<String>,

        /// Postgres slot name
        slot_name: String,

        /// Postgres publication name
        publication: String,
    },
}

impl Debug for SourceSettings {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Postgres {
                host,
                port,
                name,
                username,
                password: _,
                slot_name,
                publication,
            } => f
                .debug_struct("Postgres")
                .field("host", host)
                .field("port", port)
                .field("name", name)
                .field("username", username)
                .field("password", &"REDACTED")
                .field("slot_name", slot_name)
                .field("publication", publication)
                .finish(),
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SinkSettings {
    BigQuery {
        /// BigQuery project id
        project_id: String,

        /// BigQuery dataset id
        dataset_id: String,

        /// BigQuery service account key
        service_account_key: String,

        /// The max_staleness parameter for BigQuery: https://cloud.google.com/bigquery/docs/change-data-capture#create-max-staleness
        #[serde(skip_serializing_if = "Option::is_none")]
        max_staleness_mins: Option<u16>,
    },
    Spanner {
        project_id: String,
        instance_id: String,
        database_id: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        service_account_key: Option<String>, // Path to SA key or ADC
        dataset_id: String, // Used for metadata tables in Spanner (e.g., _replicator_last_lsn)
        #[serde(skip_serializing_if = "Option::is_none")]
        max_staleness_mins: Option<u16>, // For table creation, if applicable
    },
}

impl Debug for SinkSettings {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BigQuery {
                project_id,
                dataset_id,
                service_account_key: _,
                max_staleness_mins,
            } => f
                .debug_struct("BigQuery")
                .field("project_id", project_id)
                .field("dataset_id", dataset_id)
                .field("service_account_key", &"REDACTED")
                .field("max_staleness_mins", max_staleness_mins)
                .finish(),
            Self::Spanner {
                project_id,
                instance_id,
                database_id,
                service_account_key: _,
                dataset_id,
                max_staleness_mins,
            } => f
                .debug_struct("Spanner")
                .field("project_id", project_id)
                .field("instance_id", instance_id)
                .field("database_id", database_id)
                .field("service_account_key", &"REDACTED_IF_PRESENT")
                .field("dataset_id", dataset_id)
                .field("max_staleness_mins", max_staleness_mins)
                .finish(),
        }
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct BatchSettings {
    /// maximum batch size in number of events
    pub max_size: usize,

    /// maximum duration, in seconds, to wait for a batch to fill
    pub max_fill_secs: u64,
}

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct TlsSettings {
    /// trusted root certificates in PEM format
    pub trusted_root_certs: String,

    /// true when TLS is enabled
    pub enabled: bool,
}

impl Debug for TlsSettings {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TlsSettings")
            .field("trusted_root_certs", &"OMITTED")
            .field("enabled", &self.enabled)
            .finish()
    }
}

#[derive(Debug, Error)]
pub enum TlsSettingsError {
    #[error("Invalid TLS settings: `trusted_root_certs` must be set when `enabled` is true")]
    MissingTrustedRootCerts,
}

impl TlsSettings {
    pub fn validate(&self) -> Result<(), TlsSettingsError> {
        if self.enabled && self.trusted_root_certs.is_empty() {
            return Err(TlsSettingsError::MissingTrustedRootCerts);
        }
        Ok(())
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct Settings {
    pub source: SourceSettings,
    pub sink: SinkSettings,
    pub batch: BatchSettings,
    pub tls: TlsSettings,
    pub project: String,
}

pub fn get_configuration() -> Result<Settings, config::ConfigError> {
    let base_path = std::env::current_dir().expect("Failed to determine the current directory");
    let configuration_directory = base_path.join("configuration");

    // Detect the running environment.
    // Default to `dev` if unspecified.
    let environment: Environment = std::env::var("APP_ENVIRONMENT")
        .unwrap_or_else(|_| DEV_ENV_NAME.into())
        .try_into()
        .expect("Failed to parse APP_ENVIRONMENT.");

    let environment_filename = format!("{}.yaml", environment.as_str());
    let settings = config::Config::builder()
        .add_source(config::File::from(
            configuration_directory.join("base.yaml"),
        ))
        .add_source(config::File::from(
            configuration_directory.join(environment_filename),
        ))
        // Add in settings from environment variables (with a prefix of APP and '__' as separator)
        // E.g. `APP_SINK__BIG_QUERY__PROJECT_ID=my-project-id would set `Settings { sink: BigQuery { project_id }}` to my-project-id
        .add_source(
            config::Environment::with_prefix("APP")
                .prefix_separator("_")
                .separator("__"),
        )
        .build()?;

    settings.try_deserialize::<Settings>()
}

pub const DEV_ENV_NAME: &str = "dev";
pub const PROD_ENV_NAME: &str = "prod";

/// The possible runtime environment for our application.
pub enum Environment {
    Dev,
    Prod,
}

impl Environment {
    pub fn as_str(&self) -> &'static str {
        match self {
            Environment::Dev => DEV_ENV_NAME,
            Environment::Prod => PROD_ENV_NAME,
        }
    }
}

impl TryFrom<String> for Environment {
    type Error = String;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        match s.to_lowercase().as_str() {
            "dev" => Ok(Self::Dev),
            "prod" => Ok(Self::Prod),
            other => Err(format!(
                "{other} is not a supported environment. Use either `{DEV_ENV_NAME}` or `{PROD_ENV_NAME}`.",
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        configuration::{Settings, TlsSettings},
        BatchSettings, SinkSettings, SourceSettings,
    };

    #[test]
    pub fn deserialize_settings_test() {
        let settings = r#"{
            "source": {
                "postgres": {
                    "host": "localhost",
                    "port": 5432,
                    "name": "postgres",
                    "username": "postgres",
                    "password": "postgres",
                    "slot_name": "replicator_slot",
                    "publication": "replicator_publication"
                }
            },
            "sink": {
                "big_query": {
                    "project_id": "project-id",
                    "dataset_id": "dataset-id",
                    "service_account_key": "key"
                }
            },
            "batch": {
                "max_size": 1000,
                "max_fill_secs": 10
            },
            "tls": {
                "trusted_root_certs": "",
                "enabled": false
            },
            "project": "abcdefghijklmnopqrst"
        }"#;
        let actual = serde_json::from_str::<Settings>(settings);
        let expected = Settings {
            source: SourceSettings::Postgres {
                host: "localhost".to_string(),
                port: 5432,
                name: "postgres".to_string(),
                username: "postgres".to_string(),
                password: Some("postgres".to_string()),
                slot_name: "replicator_slot".to_string(),
                publication: "replicator_publication".to_string(),
            },
            sink: SinkSettings::BigQuery {
                project_id: "project-id".to_string(),
                dataset_id: "dataset-id".to_string(),
                service_account_key: "key".to_string(),
                max_staleness_mins: None,
            },
            batch: BatchSettings {
                max_size: 1000,
                max_fill_secs: 10,
            },
            tls: TlsSettings {
                trusted_root_certs: String::new(),
                enabled: false,
            },
            project: "abcdefghijklmnopqrst".to_string(),
        };
        assert!(actual.is_ok());
        assert_eq!(expected, actual.unwrap());
    }

    #[test]
    pub fn serialize_settings_test() {
        let actual = Settings {
            source: SourceSettings::Postgres {
                host: "localhost".to_string(),
                port: 5432,
                name: "postgres".to_string(),
                username: "postgres".to_string(),
                password: Some("postgres".to_string()),
                slot_name: "replicator_slot".to_string(),
                publication: "replicator_publication".to_string(),
            },
            sink: SinkSettings::BigQuery {
                project_id: "project-id".to_string(),
                dataset_id: "dataset-id".to_string(),
                service_account_key: "key".to_string(),
                max_staleness_mins: None,
            },
            batch: BatchSettings {
                max_size: 1000,
                max_fill_secs: 10,
            },
            tls: TlsSettings {
                trusted_root_certs: String::new(),
                enabled: false,
            },
            project: "abcdefghijklmnopqrst".to_string(),
        };
        let expected = r#"{"source":{"postgres":{"host":"localhost","port":5432,"name":"postgres","username":"postgres","password":"postgres","slot_name":"replicator_slot","publication":"replicator_publication"}},"sink":{"big_query":{"project_id":"project-id","dataset_id":"dataset-id","service_account_key":"key"}},"batch":{"max_size":1000,"max_fill_secs":10},"tls":{"trusted_root_certs":"","enabled":false},"project":"abcdefghijklmnopqrst"}"#;
        let actual = serde_json::to_string(&actual);
        assert!(actual.is_ok());
        assert_eq!(expected, actual.unwrap());
    }
}
