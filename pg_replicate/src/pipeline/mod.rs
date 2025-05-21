use std::collections::HashSet;

use postgres::schema::TableId;
use sinks::SinkError;
use sources::SourceError;
use thiserror::Error;
use tokio_postgres::types::PgLsn;

pub mod batching;
pub mod sinks;
pub mod sources;

#[derive(Debug)]
pub enum PipelineAction {
    TableCopiesOnly,
    CdcOnly,
    Both,
}

#[derive(Debug)]
pub struct PipelineResumptionState {
    pub copied_tables: HashSet<TableId>,
    pub last_lsn: PgLsn,
}

#[derive(Debug, Error)]
pub enum PipelineError<SrcErr: SourceError, SnkErr: SinkError> {
    #[error("source error: {0}")]
    Source(#[source] SrcErr),

    #[error("sink error: {0}")]
    Sink(#[source] SnkErr),

    #[error("source error: {0}")]
    CommonSource(#[from] sources::CommonSourceError),
}
