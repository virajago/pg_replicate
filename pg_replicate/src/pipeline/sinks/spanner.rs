use async_trait::async_trait;
use std::collections::HashMap;
use thiserror::Error;
use chrono::Utc;
use tokio_postgres::types::PgLsn;

use crate::clients::spanner::{SpannerClient, SpannerClientError};
use crate::conversions::table_row::{TableRow, TableCell, CellValue};
use crate::table::{TableId, TableSchema, ColumnSchema, ColumnType};
use crate::pipeline::cdc_event::CdcEvent;
use crate::pipeline::resumption::PipelineResumptionState;
use super::{BatchSink, SinkError as GenericSinkError}; // Renamed to avoid conflict

#[derive(Error, Debug)]
pub enum SpannerSinkError {
    #[error("Spanner client error: {0}")]
    ClientError(#[from] SpannerClientError),
    #[error("Table schema not found for table ID: {0:?}")]
    SchemaNotFound(TableId),
    #[error("LSN mismatch during commit: expected {expected}, got {got}")]
    LsnMismatch { expected: PgLsn, got: PgLsn },
    #[error("Begin event not found before commit")]
    BeginEventNotFound,
    #[error("Other error: {0}")]
    Other(String),
}

impl From<SpannerSinkError> for GenericSinkError<SpannerSinkError> {
    fn from(e: SpannerSinkError) -> Self {
        GenericSinkError::SinkSpecificError(e)
    }
}

pub struct SpannerBatchSink {
    client: SpannerClient,
    dataset_id: String, // Using dataset_id as a general term for the Spanner "namespace"
    table_schemas: Option<HashMap<TableId, TableSchema>>,
    committed_lsn: Option<PgLsn>,
    max_staleness_mins: u16, // Or however Spanner handles this concept.
    // For CDC processing, to hold LSN from Begin event
    current_transaction_final_lsn: Option<PgLsn>,
}

impl SpannerBatchSink {
    pub async fn new(
        project_id: String,
        instance_id: String,
        database_id: String,
        gcp_sa_key_path: Option<String>,
        dataset_id: String, // This could be derived from project/instance/database or be a separate concept
        max_staleness_mins: u16,
    ) -> Result<Self, SpannerSinkError> {
        let client =
            SpannerClient::new(project_id, instance_id, database_id, gcp_sa_key_path).await?;
        Ok(Self {
            client,
            dataset_id,
            table_schemas: None,
            committed_lsn: None,
            max_staleness_mins,
            current_transaction_final_lsn: None,
        })
    }

    // Helper to add CDC metadata columns
    fn add_cdc_metadata(table_row: &mut TableRow, change_type: &str) {
        let timestamp_cell = TableCell {
            column_name: "_CHANGE_TIMESTAMP".to_string(),
            value: CellValue::Timestamp(Utc::now()), // Using chrono Utc for timestamp
        };
        let change_type_cell = TableCell {
            column_name: "_CHANGE_TYPE".to_string(),
            value: CellValue::String(change_type.to_string()),
        };
        table_row.cells.push(timestamp_cell);
        table_row.cells.push(change_type_cell);
    }
}

#[async_trait]
impl BatchSink for SpannerBatchSink {
    type Error = SpannerSinkError;

    async fn get_resumption_state(&mut self) -> Result<PipelineResumptionState, Self::Error> {
        // These are conceptual names for metadata tables in Spanner.
        // The actual implementation in SpannerClient will handle their creation.
        // For now, we assume SpannerClient::new or a dedicated setup method handles this.

        // Example: ensure metadata tables for LSN and copied tables exist.
        // These calls might be part of SpannerClient::new or a separate setup method.
        // For now, we assume they are implicitly handled or not strictly needed for this method's purpose
        // beyond fetching the data.

        let last_lsn = self.client.get_last_lsn(&self.dataset_id).await?;
        let copied_tables = self.client.get_copied_table_ids(&self.dataset_id).await?;

        self.committed_lsn = Some(last_lsn);

        Ok(PipelineResumptionState {
            last_committed_lsn: last_lsn,
            copied_table_ids: copied_tables,
        })
    }

    async fn write_table_schemas(
        &mut self,
        table_schemas: HashMap<TableId, TableSchema>,
    ) -> Result<(), Self::Error> {
        for (table_id, schema) in &table_schemas {
            let spanner_table_name = format!("{}.{}", schema.schema_name, schema.table_name);
            // Ensure _CHANGE_TYPE and _CHANGE_TIMESTAMP columns are part of the schema for Spanner
            let mut spanner_columns = schema.columns.clone();
            spanner_columns.push(ColumnSchema {
                name: "_CHANGE_TYPE".to_string(),
                column_type: ColumnType::SpannerType("STRING".to_string()), // Assuming direct Spanner type
                is_primary_key: false,
                is_nullable: false, // Typically not nullable
                ordinal_position: spanner_columns.len() as i32 + 1,
            });
            spanner_columns.push(ColumnSchema {
                name: "_CHANGE_TIMESTAMP".to_string(),
                column_type: ColumnType::SpannerType("TIMESTAMP".to_string()), // Assuming direct Spanner type
                is_primary_key: false,
                is_nullable: false, // Typically not nullable
                ordinal_position: spanner_columns.len() as i32 + 1,
            });

            self.client
                .create_table_if_missing(&spanner_table_name, &spanner_columns, self.max_staleness_mins)
                .await?;
            println!("Ensured table {} exists in Spanner for TableId {:?}", spanner_table_name, table_id);
        }
        self.table_schemas = Some(table_schemas);
        Ok(())
    }

    async fn write_table_rows(
        &mut self,
        table_id: TableId,
        rows: Vec<TableRow>,
    ) -> Result<(), Self::Error> {
        let schemas = self.table_schemas.as_ref().ok_or_else(|| {
            SpannerSinkError::Other("Table schemas not initialized".to_string())
        })?;
        let table_schema = schemas
            .get(&table_id)
            .ok_or(SpannerSinkError::SchemaNotFound(table_id))?;

        let spanner_table_name = format!("{}.{}", table_schema.schema_name, table_schema.table_name);

        // Add CDC metadata for initial data load as well, marking as "INSERT"
        let mut processed_rows = Vec::new();
        for mut row in rows {
            Self::add_cdc_metadata(&mut row, "INSERT");
            processed_rows.push(row);
        }

        self.client.insert_rows(&spanner_table_name, &processed_rows).await?;
        Ok(())
    }

    async fn write_cdc_events(
        &mut self,
        events: Vec<CdcEvent>,
    ) -> Result<PgLsn, Self::Error> {
        let mut inserts: HashMap<TableId, Vec<TableRow>> = HashMap::new();
        let mut updates: HashMap<TableId, Vec<TableRow>> = HashMap::new();
        let mut deletes: HashMap<TableId, Vec<TableRow>> = HashMap::new();
        let mut new_last_lsn: Option<PgLsn> = None;

        let schemas = self.table_schemas.as_ref().ok_or_else(|| {
            SpannerSinkError::Other("Table schemas not initialized before writing CDC events".to_string())
        })?;

        for event in events {
            match event {
                CdcEvent::Begin { final_lsn, .. } => {
                    self.current_transaction_final_lsn = Some(final_lsn);
                }
                CdcEvent::Commit { commit_lsn, .. } => {
                    let expected_lsn = self.current_transaction_final_lsn.take()
                        .ok_or(SpannerSinkError::BeginEventNotFound)?;
                    if commit_lsn != expected_lsn {
                        return Err(SpannerSinkError::LsnMismatch { expected: expected_lsn, got: commit_lsn });
                    }
                    new_last_lsn = Some(commit_lsn);
                }
                CdcEvent::Insert { table_id, mut after_row } => {
                    Self::add_cdc_metadata(&mut after_row, "INSERT");
                    inserts.entry(table_id).or_default().push(after_row);
                }
                CdcEvent::Update { table_id, mut after_row, .. } => {
                    Self::add_cdc_metadata(&mut after_row, "UPDATE");
                    updates.entry(table_id).or_default().push(after_row);
                }
                CdcEvent::Delete { table_id, mut old_row, .. } => {
                    // For delete, we use the old_row to identify the record,
                    // but the actual row written to Spanner might just be PKs + metadata
                    // depending on how SpannerClient::delete_rows is implemented.
                    // Here, we pass the full old_row with added metadata.
                    Self::add_cdc_metadata(&mut old_row, "DELETE");
                    deletes.entry(table_id).or_default().push(old_row);
                }
            }
        }

        for (table_id, rows) in inserts {
            if !rows.is_empty() {
                let table_schema = schemas.get(&table_id).ok_or(SpannerSinkError::SchemaNotFound(table_id))?;
                let spanner_table_name = format!("{}.{}", table_schema.schema_name, table_schema.table_name);
                self.client.insert_rows(&spanner_table_name, &rows).await?;
            }
        }

        for (table_id, rows) in updates {
            if !rows.is_empty() {
                let table_schema = schemas.get(&table_id).ok_or(SpannerSinkError::SchemaNotFound(table_id))?;
                let spanner_table_name = format!("{}.{}", table_schema.schema_name, table_schema.table_name);
                self.client.update_rows(&spanner_table_name, &table_schema.columns, &rows).await?;
            }
        }

        for (table_id, rows) in deletes {
            if !rows.is_empty() {
                let table_schema = schemas.get(&table_id).ok_or(SpannerSinkError::SchemaNotFound(table_id))?;
                let spanner_table_name = format!("{}.{}", table_schema.schema_name, table_schema.table_name);
                self.client.delete_rows(&spanner_table_name, &table_schema.columns, &rows).await?;
            }
        }

        if let Some(lsn) = new_last_lsn {
            self.client.set_last_lsn(&self.dataset_id, lsn).await?;
            self.committed_lsn = Some(lsn);
        }

        Ok(self.committed_lsn.unwrap_or_else(|| PgLsn::from(0u64))) // Should always be Some if events were processed
    }

    async fn table_copied(&mut self, table_id: TableId) -> Result<(), Self::Error> {
        self.client
            .insert_into_copied_tables(&self.dataset_id, table_id)
            .await?;
        Ok(())
    }

    async fn truncate_table(&mut self, table_id: TableId) -> Result<(), Self::Error> {
        let schemas = self.table_schemas.as_ref().ok_or_else(|| {
            SpannerSinkError::Other("Table schemas not initialized".to_string())
        })?;
        let table_schema = schemas
            .get(&table_id)
            .ok_or(SpannerSinkError::SchemaNotFound(table_id))?;
        let spanner_table_name = format!("{}.{}", table_schema.schema_name, table_schema.table_name);
        
        // Spanner does not have a direct TRUNCATE TABLE DML command like PostgreSQL.
        // Deleting all rows is the closest equivalent.
        // However, for high-volume tables, this can be slow and resource-intensive.
        // A common pattern is to drop and recreate the table, or use batch delete.
        // For now, log a warning as this might need a more sophisticated implementation.
        println!(
            "Warning: SpannerBatchSink::truncate_table for table {} (ID: {:?}) is a no-op or requires careful implementation (e.g., delete all rows).",
            spanner_table_name, table_id
        );
        // If a "delete all rows" functionality is added to SpannerClient, it could be called here.
        // Example: self.client.delete_all_rows(&spanner_table_name).await?;
        Ok(())
    }
}
