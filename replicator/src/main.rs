use std::{io::BufReader, time::Duration, vec};

use configuration::{
    get_configuration, BatchSettings, Settings, SinkSettings, SourceSettings, TlsSettings,
};
use pg_replicate::{
    pipeline::{
        batching::{data_pipeline::BatchDataPipeline, BatchConfig},
        sinks::{
            bigquery::BigQueryBatchSink,
            #[cfg(feature = "spanner")]
            spanner::SpannerBatchSink,
            BatchSink, // Added to use Box<dyn BatchSink>
        },
        sources::postgres::{PostgresSource, TableNamesFrom},
        PipelineAction,
    },
    SslMode,
};
use telemetry::init_tracing;
use tracing::{info, instrument};

mod configuration;

// APP_SOURCE__POSTGRES__PASSWORD and APP_SINK__BIG_QUERY__PROJECT_ID environment variables must be set
// before running because these are sensitive values which can't be configured in the config files
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let app_name = env!("CARGO_BIN_NAME");
    // We pass emit_on_span_close = false to avoid emitting logs on span close
    // for replicator because it is not a web server and we don't need to emit logs
    // for every closing span.
    let _log_flusher = init_tracing(app_name, false)?;
    let settings = get_configuration()?;
    start_replication(settings).await
}

#[instrument(name = "replication", skip(settings), fields(project = settings.project))]
async fn start_replication(settings: Settings) -> anyhow::Result<()> {
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .expect("failed to install default crypto provider");

    let SourceSettings::Postgres {
        host,
        port,
        name,
        username,
        password: _,
        slot_name,
        publication,
    } = &settings.source;
    info!(
        host,
        port,
        dbname = name,
        username,
        slot_name,
        publication,
        "source settings"
    );

    let SinkSettings::BigQuery {
        project_id,
        dataset_id,
        service_account_key: _,
        max_staleness_mins,
    } = match &settings.sink {
        SinkSettings::BigQuery { project_id, dataset_id, service_account_key: _, max_staleness_mins } => {
            info!(project_id, dataset_id, max_staleness_mins, "bigquery sink settings");
            (project_id, dataset_id, max_staleness_mins)
        }
        #[cfg(feature = "spanner")]
        SinkSettings::Spanner { project_id, instance_id, database_id, service_account_key: _, dataset_id, max_staleness_mins } => {
            info!(project_id, instance_id, database_id, dataset_id, max_staleness_mins, "spanner sink settings");
            // The destructuring above is for BigQuery, Spanner has different fields.
            // We'll access them directly when creating the sink.
            // For the info log, we just show them.
            // This part of the code seems to be for generic logging, might need adjustment if we want to log specific fields for each sink type.
            // For now, just logging the Spanner specific fields.
            (&String::new(), &String::new(), max_staleness_mins) // Placeholder for common logging, actual values used later
        }
    };


    let BatchSettings {
        max_size,
        max_fill_secs,
    } = &settings.batch;
    info!(max_size, max_fill_secs, "batch settings");

    let TlsSettings {
        trusted_root_certs: _,
        enabled,
    } = &settings.tls;
    info!(tls_enabled = enabled, "tls settings");

    settings.tls.validate()?;

    let SourceSettings::Postgres {
        host,
        port,
        name,
        username,
        password,
        slot_name,
        publication,
    } = settings.source;

    let TlsSettings {
        trusted_root_certs,
        enabled,
    } = settings.tls;

    let mut trusted_root_certs_vec = vec![];
    let ssl_mode = if enabled {
        let mut root_certs_reader = BufReader::new(trusted_root_certs.as_bytes());
        for cert in rustls_pemfile::certs(&mut root_certs_reader) {
            let cert = cert?;
            trusted_root_certs_vec.push(cert);
        }

        SslMode::VerifyFull
    } else {
        SslMode::Disable
    };

    let postgres_source = PostgresSource::new(
        &host,
        port,
        &name,
        &username,
        password,
        ssl_mode,
        trusted_root_certs_vec,
        Some(slot_name),
        TableNamesFrom::Publication(publication),
    )
    .await?;

    // Determine the sink based on configuration
    let sink: Box<dyn BatchSink<Error = anyhow::Error>> = match settings.sink {
        SinkSettings::BigQuery {
            project_id,
            dataset_id,
            service_account_key,
            max_staleness_mins,
        } => {
            let bq_sink = BigQueryBatchSink::new_with_key(
                project_id,
                dataset_id,
                &service_account_key,
                max_staleness_mins.unwrap_or(5), // Default to 5 mins if not set
            )
            .await
            .map_err(anyhow::Error::from)?; // Convert error type
            Box::new(bq_sink)
        }
        #[cfg(feature = "spanner")]
        SinkSettings::Spanner {
            project_id,
            instance_id,
            database_id,
            service_account_key,
            dataset_id, // This is the one for metadata tables
            max_staleness_mins,
        } => {
            // The SpannerBatchSink::new expects max_staleness_mins as u16, not Option<u16>.
            // Using 0 as default if not specified in config.
            let staleness = max_staleness_mins.unwrap_or(0);
            let spanner_sink = SpannerBatchSink::new(
                project_id,
                instance_id,
                database_id,
                service_account_key, // This is Option<String>, matching gcp_sa_key_path
                dataset_id,          // dataset_id for metadata
                staleness,           // max_staleness_mins for table creation
            )
            .await
            .map_err(anyhow::Error::from)?; // Convert error type
            Box::new(spanner_sink)
        }
        #[cfg(not(feature = "spanner"))]
        SinkSettings::Spanner { .. } => {
            // This case should ideally not be hit if configuration and features are aligned.
            // If Spanner config is present but feature not compiled, it's an issue.
            return Err(anyhow::anyhow!(
                "Spanner sink configured but 'spanner' feature is not enabled in replicator."
            ));
        }
    };

    let BatchSettings {
        max_size,
        max_fill_secs,
    } = settings.batch;

    let batch_config = BatchConfig::new(max_size, Duration::from_secs(max_fill_secs));
    let mut pipeline = BatchDataPipeline::new(
        postgres_source,
        sink, // Use the dynamically created sink
        PipelineAction::Both,
        batch_config,
    );

    pipeline.start().await?;

    Ok(())
}
