use async_trait::async_trait;
use pg_replicate::conversions::cdc_event::CdcEvent;
use pg_replicate::conversions::table_row::TableRow;
use pg_replicate::pipeline::sinks::{BatchSink, InfallibleSinkError};
use pg_replicate::pipeline::PipelineResumptionState;
use postgres::schema::{TableId, TableSchema};
use std::cmp::max;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use tokio_postgres::types::PgLsn;

/// A test sink that captures replication events and data for verification.
///
/// This sink is designed to be shared across multiple pipelines, simulating
/// persistent storage while maintaining thread safety through interior mutability.
#[derive(Debug, Clone)]
pub struct TestSink {
    inner: Arc<Mutex<TestSinkInner>>,
}

/// Internal state of the test sink.
///
/// This struct maintains the sink's state including table schemas, rows,
/// CDC events, and tracking information for copied and truncated tables.
#[derive(Debug)]
struct TestSinkInner {
    // We have a Vec to store all the changes of the schema that we receive over time.
    tables_schemas: Vec<HashMap<TableId, TableSchema>>,
    tables_rows: HashMap<TableId, Vec<TableRow>>,
    events: Vec<Arc<CdcEvent>>,
    copied_tables: HashSet<TableId>,
    truncated_tables: HashSet<TableId>,
    last_lsn: u64,
}

impl TestSink {
    /// Creates a new test sink with an empty state.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(TestSinkInner {
                tables_schemas: Vec::new(),
                tables_rows: HashMap::new(),
                events: Vec::new(),
                copied_tables: HashSet::new(),
                truncated_tables: HashSet::new(),
                last_lsn: 0,
            })),
        }
    }

    /// Updates the last LSN based on received events.
    ///
    /// This method ensures that the last LSN is monotonically increasing,
    /// taking the maximum between the current LSN and the maximum LSN from
    /// the received events.
    fn receive_events(&mut self, events: &[CdcEvent]) {
        let mut max_lsn = 0;
        for event in events {
            if let CdcEvent::Commit(commit_body) = event {
                max_lsn = max(max_lsn, commit_body.commit_lsn());
            }
        }

        // We update the last lsn taking the maximum between the maximum of the event stream and
        // the current lsn, since we assume that lsns are guaranteed to be monotonically increasing,
        // so if we see a max lsn, we can be sure that all events before that point have been received.
        let mut inner = self.inner.lock().unwrap();
        inner.last_lsn = max(inner.last_lsn, max_lsn);
    }

    /// Returns a copy of all table schemas received by the sink.
    pub fn get_tables_schemas(&self) -> Vec<HashMap<TableId, TableSchema>> {
        self.inner.lock().unwrap().tables_schemas.clone()
    }

    /// Returns a copy of all table rows received by the sink.
    pub fn get_tables_rows(&self) -> HashMap<TableId, Vec<TableRow>> {
        self.inner.lock().unwrap().tables_rows.clone()
    }

    /// Returns a copy of all CDC events received by the sink.
    pub fn get_events(&self) -> Vec<Arc<CdcEvent>> {
        self.inner.lock().unwrap().events.clone()
    }

    /// Returns a copy of the set of tables that have been copied.
    pub fn get_copied_tables(&self) -> HashSet<TableId> {
        self.inner.lock().unwrap().copied_tables.clone()
    }

    /// Returns the number of tables that have been copied.
    pub fn get_tables_copied(&self) -> u8 {
        self.inner.lock().unwrap().copied_tables.len() as u8
    }

    /// Returns the number of tables that have been truncated.
    pub fn get_tables_truncated(&self) -> u8 {
        self.inner.lock().unwrap().truncated_tables.len() as u8
    }

    /// Returns the last LSN processed by the sink.
    pub fn get_last_lsn(&self) -> u64 {
        self.inner.lock().unwrap().last_lsn
    }
}

#[async_trait]
impl BatchSink for TestSink {
    type Error = InfallibleSinkError;

    async fn get_resumption_state(&mut self) -> Result<PipelineResumptionState, Self::Error> {
        Ok(PipelineResumptionState {
            copied_tables: self.get_copied_tables(),
            last_lsn: PgLsn::from(self.get_last_lsn()),
        })
    }

    async fn write_table_schemas(
        &mut self,
        table_schemas: HashMap<TableId, TableSchema>,
    ) -> Result<(), Self::Error> {
        self.inner
            .lock()
            .unwrap()
            .tables_schemas
            .push(table_schemas);

        Ok(())
    }

    async fn write_table_rows(
        &mut self,
        rows: Vec<TableRow>,
        table_id: TableId,
    ) -> Result<(), Self::Error> {
        self.inner
            .lock()
            .unwrap()
            .tables_rows
            .entry(table_id)
            .or_default()
            .extend(rows);

        Ok(())
    }

    async fn write_cdc_events(&mut self, events: Vec<CdcEvent>) -> Result<PgLsn, Self::Error> {
        self.receive_events(&events);

        // Since CdcEvent is not Clone, we have to wrap it in an Arc, and we are fine with this
        // since it's not mutable, so we don't even have to use mutexes.
        let arc_events = events.into_iter().map(Arc::new).collect::<Vec<_>>();
        self.inner.lock().unwrap().events.extend(arc_events);

        Ok(PgLsn::from(self.inner.lock().unwrap().last_lsn))
    }

    async fn table_copied(&mut self, table_id: TableId) -> Result<(), Self::Error> {
        self.inner.lock().unwrap().copied_tables.insert(table_id);

        Ok(())
    }

    async fn truncate_table(&mut self, table_id: TableId) -> Result<(), Self::Error> {
        self.inner.lock().unwrap().truncated_tables.insert(table_id);

        Ok(())
    }
}
