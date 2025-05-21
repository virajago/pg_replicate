#[cfg(feature = "spanner")]
mod spanner_tests {
    use std::collections::{HashMap, HashSet};
    use tokio_postgres::types::{PgLsn, Type as PgNativeType}; // Renamed to avoid conflict with ColumnType::PgType
    use pg_replicate::{
        clients::spanner::SpannerClient,
        conversions::{
            cdc_event::CdcEvent,
            table_row::{TableRow, TableCell, CellValue},
        },
        pipeline::{
            sinks::{spanner::SpannerBatchSink, BatchSink},
            resumption::PipelineResumptionState, // Corrected path
        },
        table::{ColumnSchema, ColumnType, TableId, TableName, TableSchema},
    };
    use chrono::Utc;

    // Potentially use gcloud_sdk for test setup/teardown or direct validation if needed
    // use gcloud_sdk::google::spanner::v1::*;

    // Helper function to get Spanner configuration (from env vars for testing)
    fn get_test_spanner_config() -> (String, String, String, Option<String>, String, u16) {
        let project_id = std::env::var("SPANNER_TEST_PROJECT_ID").expect("SPANNER_TEST_PROJECT_ID not set");
        let instance_id = std::env::var("SPANNER_TEST_INSTANCE_ID").expect("SPANNER_TEST_INSTANCE_ID not set");
        let database_id = std::env::var("SPANNER_TEST_DATABASE_ID").expect("SPANNER_TEST_DATABASE_ID not set");
        let sa_key_path = std::env::var("SPANNER_TEST_SA_KEY_PATH").ok(); // Optional
        // Construct a dataset_id for metadata tables, could be specific or derived.
        // Using a fixed name for testing or deriving from instance/database.
        let dataset_id = std::env::var("SPANNER_TEST_DATASET_ID")
            .unwrap_or_else(|_| format!("test_replicator_metadata_{}", database_id));
        let max_staleness_mins = std::env::var("SPANNER_TEST_MAX_STALENESS_MINS")
            .map(|s| s.parse().expect("Failed to parse SPANNER_TEST_MAX_STALENESS_MINS"))
            .unwrap_or(0); // Default for tests
        (project_id, instance_id, database_id, sa_key_path, dataset_id, max_staleness_mins)
    }

    async fn setup_spanner_sink() -> SpannerBatchSink {
        let (project_id, instance_id, database_id, sa_key_path, dataset_id, max_staleness_mins) = get_test_spanner_config();
        SpannerBatchSink::new(
            project_id,
            instance_id,
            database_id,
            sa_key_path, // Pass Option<String> directly
            dataset_id,
            max_staleness_mins,
        )
        .await
        .expect("Failed to create SpannerBatchSink for testing")
    }

    async fn setup_spanner_client() -> SpannerClient {
        let (project_id, instance_id, database_id, sa_key_path, _dataset_id, _max_staleness_mins) = get_test_spanner_config();
        SpannerClient::new(
            project_id,
            instance_id,
            database_id,
            sa_key_path, // Pass Option<String> directly
        )
        .await
        .expect("Failed to create SpannerClient for testing")
    }
    
    // Helper to cleanup Spanner tables (implementation will be complex)
    async fn cleanup_spanner_tables(_client: &mut SpannerClient, table_names: Vec<String>, _dataset_id: &str) {
        // TODO: Implement logic to drop tables or delete all rows.
        // This might involve listing all tables in the test dataset and dropping them.
        // For now, can be a placeholder.
        // Example: for table_name in table_names { client.execute_ddl(&format!("DROP TABLE {}", table_name)).await.ok(); }
        eprintln!("TODO: Cleanup Spanner tables: {:?}", table_names);
    }

    #[tokio::test]
    #[ignore = "Spanner emulator/instance required and test not fully implemented"]
    async fn test_spanner_get_resumption_state_initial() {
        let mut sink = setup_spanner_sink().await;
        // TODO: Potentially clean up metadata tables (_replicator_last_lsn, _replicator_copied_tables) before test
        // using SpannerClient and dataset_id from get_test_spanner_config()
        
        let state = sink.get_resumption_state().await.expect("get_resumption_state failed");
        
        assert_eq!(state.last_committed_lsn, PgLsn::from(0u64)); // Corrected field name
        assert!(state.copied_table_ids.is_empty()); // Corrected field name
        
        // TODO: Cleanup metadata tables after test
    }

    #[tokio::test]
    #[ignore = "Spanner emulator/instance required and test not fully implemented"]
    async fn test_spanner_write_table_schemas_and_copy() {
        let mut sink = setup_spanner_sink().await;
        let mut client = setup_spanner_client().await;
        let (_project_id, _instance_id, _database_id, _sa_key_path, _dataset_id, _max_staleness_mins) = get_test_spanner_config();

        let table_id_val = 1u32; // TableId is u32
        let table_id = TableId::from(table_id_val);
        let table_name_str = "test_table_copy";
        let schema_name_str = "public";

        let table_name_obj = TableName { schema: schema_name_str.to_string(), name: table_name_str.to_string() };
        
        let column_schemas = vec![
            ColumnSchema { name: "id".to_string(), column_type: ColumnType::PgType(PgNativeType::INT4), is_primary_key: true, is_nullable: false, ordinal_position: 1 },
            ColumnSchema { name: "data".to_string(), column_type: ColumnType::PgType(PgNativeType::TEXT), is_primary_key: false, is_nullable: true, ordinal_position: 2 },
        ];
        let table_schema = TableSchema { table_id, table_name: table_name_obj.clone(), columns: column_schemas.clone() }; // Corrected field name
        let mut schemas = HashMap::new();
        schemas.insert(table_id, table_schema.clone());

        sink.write_table_schemas(schemas).await.expect("write_table_schemas failed");

        let spanner_table_name = format!("{}.{}", schema_name_str, table_name_str);
        assert!(client.table_exists(&spanner_table_name).await.unwrap_or_else(|e| panic!("table_exists check failed for {}: {}", spanner_table_name, e)));
        
        // TODO: Verify columns and types in Spanner if possible with client (e.g., query INFORMATION_SCHEMA.COLUMNS)

        let rows = vec![
            TableRow {
                table_id,
                cells: vec![
                    TableCell { column_name: "id".to_string(), value: CellValue::I32(Some(1)) },
                    TableCell { column_name: "data".to_string(), value: CellValue::String(Some("row1".to_string())) },
                ],
            },
            TableRow {
                table_id,
                cells: vec![
                    TableCell { column_name: "id".to_string(), value: CellValue::I32(Some(2)) },
                    TableCell { column_name: "data".to_string(), value: CellValue::String(Some("row2".to_string())) },
                ],
            },
        ];
        // write_table_rows in BatchSink trait expects table_id and rows.
        // The SpannerBatchSink implementation takes table_id and Vec<TableRow>
        sink.write_table_rows(table_id, rows).await.expect("write_table_rows failed");
        
        // TODO: Verify rows exist in Spanner (using SpannerClient to query `SELECT * FROM public.test_table_copy`)

        sink.table_copied(table_id).await.expect("table_copied failed");
        let state = sink.get_resumption_state().await.expect("get_resumption_state failed");
        assert!(state.copied_table_ids.contains(&table_id)); // Corrected field name
            
        cleanup_spanner_tables(&mut client, vec![spanner_table_name], &_dataset_id).await;
    }

    #[tokio::test]
    #[ignore = "Spanner emulator/instance required and test not fully implemented"]
    async fn test_spanner_cdc_events_insert_update_delete() {
        let mut sink = setup_spanner_sink().await;
        let mut client = setup_spanner_client().await;
        let (_project_id, _instance_id, _database_id, _sa_key_path, dataset_id_meta, _max_staleness_mins) = get_test_spanner_config();

        let table_id_val = 2u32; // Use a different table ID or ensure cleanup
        let table_id = TableId::from(table_id_val);
        let table_name_str = "test_table_cdc";
        let schema_name_str = "public";
        let spanner_table_name = format!("{}.{}", schema_name_str, table_name_str);

        let table_name_obj = TableName { schema: schema_name_str.to_string(), name: table_name_str.to_string() };
        let column_schemas = vec![
            ColumnSchema { name: "id".to_string(), column_type: ColumnType::PgType(PgNativeType::INT4), is_primary_key: true, is_nullable: false, ordinal_position: 1 },
            ColumnSchema { name: "value".to_string(), column_type: ColumnType::PgType(PgNativeType::TEXT), is_primary_key: false, is_nullable: true, ordinal_position: 2 },
        ];
        let table_schema_for_cdc = TableSchema { table_id, table_name: table_name_obj.clone(), columns: column_schemas.clone() };
        let mut schemas_for_cdc = HashMap::new();
        schemas_for_cdc.insert(table_id, table_schema_for_cdc.clone());

        // Write schema before CDC events
        sink.write_table_schemas(schemas_for_cdc).await.expect("write_table_schemas for CDC failed");


        let lsn_begin = PgLsn::from(90u64); // Not used directly by sink, but good for context
        let lsn_commit = PgLsn::from(120u64);
        let commit_timestamp = Utc::now();

        let cdc_events = vec![
            CdcEvent::Begin { final_lsn: lsn_commit, commit_timestamp },
            CdcEvent::Insert { 
                table_id, 
                after_row: TableRow {
                    table_id,
                    cells: vec![
                        TableCell { column_name: "id".to_string(), value: CellValue::I32(Some(10)) },
                        TableCell { column_name: "value".to_string(), value: CellValue::String(Some("cdc_insert".to_string())) },
                    ],
                },
            },
            CdcEvent::Update { 
                table_id, 
                // old_row might be None or Some depending on replica identity
                old_row: Some(TableRow { // Assuming replica identity FULL for test
                    table_id,
                    cells: vec![
                        TableCell { column_name: "id".to_string(), value: CellValue::I32(Some(10)) },
                        TableCell { column_name: "value".to_string(), value: CellValue::String(Some("cdc_insert".to_string())) },
                    ],
                }),
                after_row: TableRow {
                    table_id,
                    cells: vec![
                        TableCell { column_name: "id".to_string(), value: CellValue::I32(Some(10)) },
                        TableCell { column_name: "value".to_string(), value: CellValue::String(Some("cdc_update".to_string())) },
                    ],
                },
            },
            CdcEvent::Delete { 
                table_id, 
                old_row: TableRow { // PK is sufficient for delete, but replica identity might provide full row
                    table_id,
                    cells: vec![
                        TableCell { column_name: "id".to_string(), value: CellValue::I32(Some(10)) },
                        // Value might be the updated one or original depending on how delete event is captured
                        TableCell { column_name: "value".to_string(), value: CellValue::String(Some("cdc_update".to_string())) }, 
                    ],
                },
            },
            CdcEvent::Commit {commit_lsn: lsn_commit, end_lsn: lsn_commit, commit_timestamp },
        ];
            
        let returned_lsn = sink.write_cdc_events(cdc_events).await.expect("write_cdc_events failed");
        assert_eq!(returned_lsn, lsn_commit);

        // TODO: Verify data in Spanner using client:
        // - Row with ID 10 was inserted, then updated, then deleted. So it should not exist if queried by PK.
        // - Check _CHANGE_TYPE and _CHANGE_TIMESTAMP columns for the history (would require more specific queries and known timestamps)

        let state = sink.get_resumption_state().await.expect("get_resumption_state failed");
        assert_eq!(state.last_committed_lsn, lsn_commit); // Corrected field name

        cleanup_spanner_tables(&mut client, vec![spanner_table_name], &dataset_id_meta).await;
    }
    
    // TODO: Add test for data type conversions (write various PG types, read from Spanner, verify)
    // TODO: Add test for replication resumption (write some data, get LSN, create new sink, ensure it resumes)
}
