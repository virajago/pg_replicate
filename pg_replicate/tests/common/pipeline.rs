use pg_replicate::pipeline::batching::data_pipeline::{BatchDataPipeline, BatchDataPipelineHandle};
use pg_replicate::pipeline::batching::BatchConfig;
use pg_replicate::pipeline::sinks::BatchSink;
use pg_replicate::pipeline::sources::postgres::{PostgresSource, TableNamesFrom};
use pg_replicate::pipeline::PipelineAction;
use postgres::schema::TableName;
use postgres::tokio::options::PgDatabaseOptions;
use std::time::Duration;
use tokio::task::JoinHandle;

/// Defines the operational mode for a PostgreSQL replication pipeline.
pub enum PipelineMode {
    /// Initializes a pipeline to copy specified tables.
    CopyTable { table_names: Vec<TableName> },
    /// Initializes a pipeline to consume changes from a publication and replication slot.
    ///
    /// If no slot name is provided, a new slot will be created on the specified publication.
    Cdc {
        publication: String,
        slot_name: String,
    },
}

/// Generates a test-specific replication slot name.
///
/// This function prefixes the provided slot name with "test_" to avoid conflicts
/// with other replication slots.
pub fn test_slot_name(slot_name: &str) -> String {
    format!("test_{}", slot_name)
}

/// Creates a new PostgreSQL replication pipeline.
///
/// This function initializes a pipeline with a batch size of 1000 records and
/// a maximum batch duration of 10 seconds.
///
/// # Panics
///
/// Panics if the PostgreSQL source cannot be created.
pub async fn spawn_pg_pipeline<Snk: BatchSink>(
    options: &PgDatabaseOptions,
    mode: PipelineMode,
    sink: Snk,
) -> BatchDataPipeline<PostgresSource, Snk> {
    let batch_config = BatchConfig::new(1000, Duration::from_secs(10));

    let pipeline = match mode {
        PipelineMode::CopyTable { table_names } => {
            let source = PostgresSource::new(
                options.clone(),
                vec![],
                None,
                TableNamesFrom::Vec(table_names),
            )
            .await
            .expect("Failure when creating the Postgres source for copying tables");
            let action = PipelineAction::TableCopiesOnly;
            BatchDataPipeline::new(source, sink, action, batch_config)
        }
        PipelineMode::Cdc {
            publication,
            slot_name,
        } => {
            let source = PostgresSource::new(
                options.clone(),
                vec![],
                Some(test_slot_name(&slot_name)),
                TableNamesFrom::Publication(publication),
            )
            .await
            .expect("Failure when creating the Postgres source for cdc");
            let action = PipelineAction::CdcOnly;
            BatchDataPipeline::new(source, sink, action, batch_config)
        }
    };

    pipeline
}

/// Creates and spawns a new asynchronous PostgreSQL replication pipeline.
///
/// This function creates a pipeline and wraps it in a [`PipelineRunner`] for
/// easier management of the pipeline lifecycle.
pub async fn spawn_async_pg_pipeline<Snk: BatchSink + Send + 'static>(
    options: &PgDatabaseOptions,
    mode: PipelineMode,
    sink: Snk,
) -> PipelineRunner<Snk> {
    let pipeline = spawn_pg_pipeline(options, mode, sink).await;
    PipelineRunner::new(pipeline)
}

/// Manages the lifecycle of a PostgreSQL replication pipeline.
///
/// This struct provides methods to run and stop a pipeline, handling the
/// pipeline's state and ensuring proper cleanup.
pub struct PipelineRunner<Snk: BatchSink> {
    pipeline: Option<BatchDataPipeline<PostgresSource, Snk>>,
    pipeline_handle: BatchDataPipelineHandle,
}

impl<Snk: BatchSink + Send + 'static> PipelineRunner<Snk> {
    /// Creates a new pipeline runner with the specified pipeline.
    pub fn new(pipeline: BatchDataPipeline<PostgresSource, Snk>) -> Self {
        let pipeline_handle = pipeline.handle();
        Self {
            pipeline: Some(pipeline),
            pipeline_handle,
        }
    }

    /// Starts the pipeline asynchronously.
    ///
    /// # Panics
    ///
    /// Panics if the pipeline has already been run.
    pub async fn run(&mut self) -> JoinHandle<BatchDataPipeline<PostgresSource, Snk>> {
        if let Some(mut pipeline) = self.pipeline.take() {
            return tokio::spawn(async move {
                pipeline
                    .start()
                    .await
                    .expect("The pipeline experienced an error");

                pipeline
            });
        }

        panic!("The pipeline has already been run");
    }

    /// Stops the pipeline and waits for it to complete.
    ///
    /// This method signals the pipeline to stop and waits for it to finish
    /// before returning. The pipeline is then restored to its initial state
    /// for potential reuse.
    ///
    /// # Panics
    ///
    /// Panics if the pipeline task fails.
    pub async fn stop_and_wait(
        &mut self,
        pipeline_task_handle: JoinHandle<BatchDataPipeline<PostgresSource, Snk>>,
    ) {
        // We signal the existing pipeline to stop.
        self.pipeline_handle.stop();

        // We wait for the pipeline to finish, and we put it back for the next run.
        let pipeline = pipeline_task_handle
            .await
            .expect("The pipeline task has failed");
        // We recreate the handle just to make sure the pipeline handle and pipelines connected.
        self.pipeline_handle = pipeline.handle();
        self.pipeline = Some(pipeline);
    }
}
