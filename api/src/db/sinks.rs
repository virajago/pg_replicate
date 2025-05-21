use aws_lc_rs::{aead::Nonce, error::Unspecified};
use base64::{prelude::BASE64_STANDARD, DecodeError, Engine};
use serde_json::Value;
use sqlx::{PgPool, Postgres, Transaction};
use std::{
    fmt::{Debug, Formatter},
    str::{from_utf8, Utf8Error},
};
use thiserror::Error;

use crate::encryption::{decrypt, encrypt, EncryptedValue, EncryptionKey};

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SinkConfig {
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
        service_account_key: Option<String>, // Spanner might use default ADC
        // max_staleness_mins is not typically configured directly for Spanner tables in the same way as BQ CDC.
        // Data retention and point-in-time recovery are handled differently.
    },
}

impl SinkConfig {
    pub fn into_db_config(
        self,
        encryption_key: &EncryptionKey,
    ) -> Result<SinkConfigInDb, Unspecified> {
        let SinkConfig::BigQuery {
            project_id,
            dataset_id,
            service_account_key,
            project_id,
            dataset_id,
            service_account_key,
            max_staleness_mins,
        } => {
            let (encrypted_sa_key, nonce) =
                encrypt(service_account_key.as_bytes(), &encryption_key.key)?;
            let encrypted_encoded_sa_key = BASE64_STANDARD.encode(encrypted_sa_key);
            let encoded_nonce = BASE64_STANDARD.encode(nonce.as_ref());
            let encrypted_sa_key_value = EncryptedValue {
                id: encryption_key.id,
                nonce: encoded_nonce,
                value: encrypted_encoded_sa_key,
            };

            Ok(SinkConfigInDb::BigQuery {
                project_id,
                dataset_id,
                service_account_key: encrypted_sa_key_value,
                max_staleness_mins,
            })
        }
        SinkConfig::Spanner {
            project_id,
            instance_id,
            database_id,
            service_account_key,
        } => {
            let encrypted_sa_key_opt = if let Some(sa_key) = service_account_key {
                let (encrypted_sa_key, nonce) = encrypt(sa_key.as_bytes(), &encryption_key.key)?;
                let encrypted_encoded_sa_key = BASE64_STANDARD.encode(encrypted_sa_key);
                let encoded_nonce = BASE64_STANDARD.encode(nonce.as_ref());
                Some(EncryptedValue {
                    id: encryption_key.id,
                    nonce: encoded_nonce,
                    value: encrypted_encoded_sa_key,
                })
            } else {
                None
            };

            Ok(SinkConfigInDb::Spanner {
                project_id,
                instance_id,
                database_id,
                service_account_key: encrypted_sa_key_opt,
            })
        }
    }
}

impl Debug for SinkConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
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
            } => f
                .debug_struct("Spanner")
                .field("project_id", project_id)
                .field("instance_id", instance_id)
                .field("database_id", database_id)
                .field("service_account_key", &"REDACTED_IF_PRESENT")
                .finish(),
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SinkConfigInDb {
    BigQuery {
        /// BigQuery project id
        project_id: String,

        /// BigQuery dataset id
        dataset_id: String,

        /// BigQuery service account key
        service_account_key: EncryptedValue,

        /// The max_staleness parameter for BigQuery: https://cloud.google.com/bigquery/docs/change-data-capture#create-max-staleness
        #[serde(skip_serializing_if = "Option::is_none")]
        max_staleness_mins: Option<u16>,
    },
    Spanner {
        project_id: String,
        instance_id: String,
        database_id: String,
        service_account_key: Option<EncryptedValue>,
    },
}

impl SinkConfigInDb {
    fn into_config(self, encryption_key: &EncryptionKey) -> Result<SinkConfig, SinksDbError> {
        match self {
            SinkConfigInDb::BigQuery {
                project_id,
                dataset_id,
                service_account_key: encrypted_sa_key_value,
                max_staleness_mins,
            } => {
                if encrypted_sa_key_value.id != encryption_key.id {
                    return Err(SinksDbError::MismatchedKeyId(
                        encrypted_sa_key_value.id,
                        encryption_key.id,
                    ));
                }

                let encrypted_sa_key_bytes = BASE64_STANDARD.decode(encrypted_sa_key_value.value)?;
                let nonce = Nonce::try_assume_unique_for_key(
                    &BASE64_STANDARD.decode(encrypted_sa_key_value.nonce)?,
                )?;
                let decrypted_sa_key = from_utf8(&decrypt(
                    encrypted_sa_key_bytes,
                    nonce,
                    &encryption_key.key,
                )?)?
                .to_string();

                Ok(SinkConfig::BigQuery {
                    project_id,
                    dataset_id,
                    service_account_key: decrypted_sa_key,
                    max_staleness_mins,
                })
            }
            SinkConfigInDb::Spanner {
                project_id,
                instance_id,
                database_id,
                service_account_key: encrypted_sa_key_opt,
            } => {
                let decrypted_sa_key_opt = if let Some(encrypted_sa_key) = encrypted_sa_key_opt {
                    if encrypted_sa_key.id != encryption_key.id {
                        return Err(SinksDbError::MismatchedKeyId(
                            encrypted_sa_key.id,
                            encryption_key.id,
                        ));
                    }
                    let encrypted_sa_key_bytes = BASE64_STANDARD.decode(encrypted_sa_key.value)?;
                    let nonce = Nonce::try_assume_unique_for_key(
                        &BASE64_STANDARD.decode(encrypted_sa_key.nonce)?,
                    )?;
                    Some(
                        from_utf8(&decrypt(
                            encrypted_sa_key_bytes,
                            nonce,
                            &encryption_key.key,
                        )?)?
                        .to_string(),
                    )
                } else {
                    None
                };

                Ok(SinkConfig::Spanner {
                    project_id,
                    instance_id,
                    database_id,
                    service_account_key: decrypted_sa_key_opt,
                })
            }
        }
    }
}

#[derive(Debug, Error)]
pub enum SinksDbError {
    #[error("sqlx error: {0}")]
    Sqlx(#[from] sqlx::Error),

    #[error("encryption error: {0}")]
    Encryption(#[from] Unspecified),

    #[error("invalid source config in db")]
    InvalidConfig(#[from] serde_json::Error),

    #[error("mismatched key id. Expected: {0}, actual: {1}")]
    MismatchedKeyId(u32, u32),

    #[error("base64 decode error: {0}")]
    Base64Decode(#[from] DecodeError),

    #[error("utf8 error: {0}")]
    Utf8(#[from] Utf8Error),
}

pub struct Sink {
    pub id: i64,
    pub tenant_id: String,
    pub name: String,
    pub config: SinkConfig,
}

pub async fn create_sink(
    pool: &PgPool,
    tenant_id: &str,
    name: &str,
    config: SinkConfig,
    encryption_key: &EncryptionKey,
) -> Result<i64, SinksDbError> {
    let db_config = config.into_db_config(encryption_key)?;
    let db_config = serde_json::to_value(db_config).expect("failed to serialize config");
    let mut txn = pool.begin().await?;
    let res = create_sink_txn(&mut txn, tenant_id, name, db_config).await;
    txn.commit().await?;
    res
}

pub async fn create_sink_txn(
    txn: &mut Transaction<'_, Postgres>,
    tenant_id: &str,
    name: &str,
    sink_config: Value,
) -> Result<i64, SinksDbError> {
    let record = sqlx::query!(
        r#"
        insert into app.sinks (tenant_id, name, config)
        values ($1, $2, $3)
        returning id
        "#,
        tenant_id,
        name,
        sink_config
    )
    .fetch_one(&mut **txn)
    .await?;

    Ok(record.id)
}

pub async fn read_sink(
    pool: &PgPool,
    tenant_id: &str,
    sink_id: i64,
    encryption_key: &EncryptionKey,
) -> Result<Option<Sink>, SinksDbError> {
    let record = sqlx::query!(
        r#"
        select id, tenant_id, name, config
        from app.sinks
        where tenant_id = $1 and id = $2
        "#,
        tenant_id,
        sink_id,
    )
    .fetch_optional(pool)
    .await?;

    let sink = record
        .map(|r| {
            let config: SinkConfigInDb = serde_json::from_value(r.config)?;
            let config = config.into_config(encryption_key)?;
            let source = Sink {
                id: r.id,
                tenant_id: r.tenant_id,
                name: r.name,
                config,
            };
            Ok::<Sink, SinksDbError>(source)
        })
        .transpose()?;
    Ok(sink)
}

pub async fn update_sink(
    pool: &PgPool,
    tenant_id: &str,
    name: &str,
    sink_id: i64,
    sink_config: SinkConfig,
    encryption_key: &EncryptionKey,
) -> Result<Option<i64>, SinksDbError> {
    let sink_config = sink_config.into_db_config(encryption_key)?;
    let sink_config = serde_json::to_value(sink_config).expect("failed to serialize config");
    let mut txn = pool.begin().await?;
    let res = update_sink_txn(&mut txn, tenant_id, name, sink_id, sink_config).await;
    txn.commit().await?;
    res
}

pub async fn update_sink_txn(
    txn: &mut Transaction<'_, Postgres>,
    tenant_id: &str,
    name: &str,
    sink_id: i64,
    sink_config: Value,
) -> Result<Option<i64>, SinksDbError> {
    let record = sqlx::query!(
        r#"
        update app.sinks
        set config = $1, name = $2
        where tenant_id = $3 and id = $4
        returning id
        "#,
        sink_config,
        name,
        tenant_id,
        sink_id
    )
    .fetch_optional(&mut **txn)
    .await?;

    Ok(record.map(|r| r.id))
}

pub async fn delete_sink(
    pool: &PgPool,
    tenant_id: &str,
    sink_id: i64,
) -> Result<Option<i64>, sqlx::Error> {
    let record = sqlx::query!(
        r#"
        delete from app.sinks
        where tenant_id = $1 and id = $2
        returning id
        "#,
        tenant_id,
        sink_id
    )
    .fetch_optional(pool)
    .await?;

    Ok(record.map(|r| r.id))
}

pub async fn read_all_sinks(
    pool: &PgPool,
    tenant_id: &str,
    encryption_key: &EncryptionKey,
) -> Result<Vec<Sink>, SinksDbError> {
    let records = sqlx::query!(
        r#"
        select id, tenant_id, name, config
        from app.sinks
        where tenant_id = $1
        "#,
        tenant_id,
    )
    .fetch_all(pool)
    .await?;

    let mut sinks = Vec::with_capacity(records.len());
    for record in records {
        let config: SinkConfigInDb = serde_json::from_value(record.config)?;
        let config = config.into_config(encryption_key)?;
        let source = Sink {
            id: record.id,
            tenant_id: record.tenant_id,
            name: record.name,
            config,
        };
        sinks.push(source);
    }

    Ok(sinks)
}

pub async fn sink_exists(
    pool: &PgPool,
    tenant_id: &str,
    sink_id: i64,
) -> Result<bool, sqlx::Error> {
    let record = sqlx::query!(
        r#"
        select exists (select id
        from app.sinks
        where tenant_id = $1 and id = $2) as "exists!"
        "#,
        tenant_id,
        sink_id,
    )
    .fetch_one(pool)
    .await?;

    Ok(record.exists)
}

#[cfg(test)]
mod tests {
    use crate::db::sinks::SinkConfig;

    #[test]
    pub fn deserialize_settings_test() {
        let settings_bq = r#"{
            "big_query": {
                "project_id": "project-id",
                "dataset_id": "dataset-id",
                "service_account_key": "service-account-key"
            }
        }"#;
        let actual_bq = serde_json::from_str::<SinkConfig>(settings_bq);
        let expected_bq = SinkConfig::BigQuery {
            project_id: "project-id".to_string(),
            dataset_id: "dataset-id".to_string(),
            service_account_key: "service-account-key".to_string(),
            max_staleness_mins: None,
        };
        assert!(actual_bq.is_ok());
        assert_eq!(expected_bq, actual_bq.unwrap());

        let settings_spanner_full = r#"{
            "spanner": {
                "project_id": "sp-project",
                "instance_id": "sp-instance",
                "database_id": "sp-db",
                "service_account_key": "sp-sa-key"
            }
        }"#;
        let actual_spanner_full = serde_json::from_str::<SinkConfig>(settings_spanner_full);
        let expected_spanner_full = SinkConfig::Spanner {
            project_id: "sp-project".to_string(),
            instance_id: "sp-instance".to_string(),
            database_id: "sp-db".to_string(),
            service_account_key: Some("sp-sa-key".to_string()),
        };
        assert!(actual_spanner_full.is_ok());
        assert_eq!(expected_spanner_full, actual_spanner_full.unwrap());

        let settings_spanner_no_sa = r#"{
            "spanner": {
                "project_id": "sp-project2",
                "instance_id": "sp-instance2",
                "database_id": "sp-db2",
                "service_account_key": null
            }
        }"#;
        let actual_spanner_no_sa = serde_json::from_str::<SinkConfig>(settings_spanner_no_sa);
        let expected_spanner_no_sa = SinkConfig::Spanner {
            project_id: "sp-project2".to_string(),
            instance_id: "sp-instance2".to_string(),
            database_id: "sp-db2".to_string(),
            service_account_key: None,
        };
        assert!(actual_spanner_no_sa.is_ok());
        assert_eq!(expected_spanner_no_sa, actual_spanner_no_sa.unwrap());
    }

    #[test]
    pub fn serialize_settings_test() {
        let actual_bq = SinkConfig::BigQuery {
            project_id: "project-id".to_string(),
            dataset_id: "dataset-id".to_string(),
            service_account_key: "service-account-key".to_string(),
            max_staleness_mins: None,
        };
        let expected_bq = r#"{"big_query":{"project_id":"project-id","dataset_id":"dataset-id","service_account_key":"service-account-key"}}"#;
        let actual_bq_json = serde_json::to_string(&actual_bq);
        assert!(actual_bq_json.is_ok());
        assert_eq!(expected_bq, actual_bq_json.unwrap());

        let actual_spanner_full = SinkConfig::Spanner {
            project_id: "sp-project".to_string(),
            instance_id: "sp-instance".to_string(),
            database_id: "sp-db".to_string(),
            service_account_key: Some("sp-sa-key".to_string()),
        };
        let expected_spanner_full = r#"{"spanner":{"project_id":"sp-project","instance_id":"sp-instance","database_id":"sp-db","service_account_key":"sp-sa-key"}}"#;
        let actual_spanner_full_json = serde_json::to_string(&actual_spanner_full);
        assert!(actual_spanner_full_json.is_ok());
        assert_eq!(expected_spanner_full, actual_spanner_full_json.unwrap());

        let actual_spanner_no_sa = SinkConfig::Spanner {
            project_id: "sp-project2".to_string(),
            instance_id: "sp-instance2".to_string(),
            database_id: "sp-db2".to_string(),
            service_account_key: None,
        };
        // Note: serde serializes Option<String>: None as `null` if not skipped,
        // and skips it if `skip_serializing_if = "Option::is_none"` is on the field.
        // For `service_account_key: Option<String>` without skip_serializing_if on Spanner variant:
        let expected_spanner_no_sa = r#"{"spanner":{"project_id":"sp-project2","instance_id":"sp-instance2","database_id":"sp-db2","service_account_key":null}}"#;
        let actual_spanner_no_sa_json = serde_json::to_string(&actual_spanner_no_sa);
        assert!(actual_spanner_no_sa_json.is_ok());
        assert_eq!(expected_spanner_no_sa, actual_spanner_no_sa_json.unwrap());
    }
}
