mod config;
mod fhir_client;
mod processor;

use crate::processor::{Context, Processor};
use config::AppConfig;
use log::{error, info};
use rdkafka::ClientConfig;
use std::{env, process};
use tokio::signal::unix::{SignalKind, signal};
use tokio_util::sync::CancellationToken;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() {
    // app config
    let config = match AppConfig::new() {
        Ok(config) => config,
        Err(e) => {
            println!("Failed to parse app settings: {e}");
            process::exit(1)
        }
    };

    // logging / tracing
    let filter = format!(
        "{}={level},reqwest_retry={level}",
        env!("CARGO_CRATE_NAME"),
        level = config.app.log_level
    );
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| filter.into()))
        .init();

    // cancellation
    let cancel = CancellationToken::new();
    let cloned_token = cancel.clone();
    tokio::spawn(async move {
        let mut sigterm = signal(SignalKind::terminate()).unwrap();
        sigterm.recv().await;
        info!("🛑 SIGTERM received. Shutting down consumers..");
        cloned_token.cancel();
    });

    let ctx = Context {
        cancel,
        on_commit: None,
    };
    match Processor::new(config, ctx).await {
        Ok(processor) => {
            processor.start().await;
        }
        Err(e) => {
            error!("Failed to create Processor: {e}");
            process::exit(1)
        }
    }
}
