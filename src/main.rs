mod config;
mod fhir_client;

use crate::config::Kafka;
use crate::fhir_client::FhirClient;
use anyhow::anyhow;
use config::AppConfig;
use futures::TryStreamExt;
use futures::future::join_all;
use futures::stream::FuturesUnordered;
use log::{debug, error, info, trace, warn};
use rdkafka::ClientConfig;
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::{BorrowedMessage, Headers, Message};
use serde_derive::Deserialize;
use std::sync::Arc;
use std::time::Duration;
use std::{env, process};
use tokio::select;
use tokio::signal::unix::{SignalKind, signal};
use tokio_util::sync::CancellationToken;
use tracing_subscriber::EnvFilter;

async fn run(config: Kafka, topic: String, client: Arc<FhirClient>, token: CancellationToken) {
    loop {
        // create consumer
        let consumer: StreamConsumer = create_consumer(config.clone());
        match consumer.subscribe(&[&topic]) {
            Ok(()) => {
                info!("Successfully subscribed to topic {topic}");
            }
            Err(e) => {
                error!("Failed to subscribe to specified topic: {e}");
                break;
            }
        }
        let consumer = Arc::new(consumer);

        let stream = consumer.stream().map_err(|e| anyhow!(e)).try_for_each(|m| {
            process_message(
                m,
                consumer.clone(),
                client.clone(),
                topic.clone(),
                token.clone(),
            )
        });

        info!("Starting consumer for topic: {topic}");
        match stream.await {
            Err(e) => error!("Consumer for topic {topic} terminated: {e}"),
            Ok(()) => {
                warn!("Consumer stream for topic {topic} ended");
                break;
            }
        }

        info!("Restarting consumer for topic {topic} in 10 seconds...");
        if !do_retry(token.clone(), Duration::from_secs(10)).await {
            // The token was cancelled
            consumer.unsubscribe();
            trace!("Consumer for topic {topic} was stopped by cancellation");
            break;
        }
    }
}

async fn process_message(
    m: BorrowedMessage<'_>,
    consumer: Arc<StreamConsumer>,
    client: Arc<FhirClient>,
    topic: String,
    token: CancellationToken,
) -> anyhow::Result<()> {
    if token.is_cancelled() {
        consumer.unsubscribe();
        return Err(anyhow!("Consumer for topic {topic} stopped"));
    }

    let (key, payload) = deserialize_message(&m);

    debug!("[Received] message from {topic}, key: {key}");
    trace!(
        "Message key: '{}', payload: '{}', topic: {}, partition: {}, offset: {}, timestamp: {:?}",
        key,
        payload.as_deref().unwrap_or("[null]"),
        m.topic(),
        m.partition(),
        m.offset(),
        m.timestamp()
    );

    if let Some(headers) = m.headers() {
        for header in headers.iter() {
            trace!(
                "Header {}:{}",
                header.key,
                header
                    .value
                    .map(String::from_utf8_lossy)
                    .unwrap_or_default()
            );
        }
    }

    // filter tombstone records
    if payload.is_none() {
        return Ok(());
    }

    // send payload to FHIR server
    let res = client.send(&payload.unwrap()).await;
    match res {
        Ok(b) if b.status().is_success() => {
            let status = b.status();
            let id = b.json::<ResponseBundle>().await.map(|b| b.id)?;
            debug!("[Sent] bundle to FHIR server, id: {id}");
            trace!("[Response] {}", status);

            // store offset
            consumer
                .store_offset_from_message(&m)
                .expect("Failed to store offset for message");
            Ok(())
        }
        Ok(b) => {
            error!("Error response from server: {}", b.status());
            // stop processing
            consumer.unsubscribe();
            Err(anyhow!(
                "Failed to send payload to the FHIR server (status: {}). Stopping consumer for {topic}",
                b.status()
            ))
        }
        Err(e) => {
            // stop processing
            error!("Failed to send request to server: {e}");
            consumer.unsubscribe();
            Err(anyhow!(e))
        }
    }
}

async fn do_retry(token: CancellationToken, wait: Duration) -> bool {
    select! {
        _ =  token.cancelled() => {
            false
        }
        _ = tokio::time::sleep(wait) => {
        true
        }
    }
}

#[derive(Deserialize)]
struct ResponseBundle {
    id: String,
}

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
        "{}={level},tower_http={level}",
        env!("CARGO_CRATE_NAME"),
        level = config.app.log_level
    );
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| filter.into()))
        .init();

    let client = match FhirClient::new(&config).await {
        Ok(c) => Arc::new(c),
        Err(e) => {
            error!("Failed to create HTTP client: {e}");
            process::exit(1)
        }
    };

    // cancellation
    let cancel = CancellationToken::new();
    let cloned_token = cancel.clone();
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.unwrap();
        signal(SignalKind::terminate()).unwrap().recv().await;
        info!("🛑 SIGTERM received. Shutting down consumers..");
        cloned_token.cancel();
    });

    let topics = config
        .kafka
        .input_topics
        .split(',')
        .map(String::from)
        .collect::<Vec<String>>();

    let tasks = topics
        .into_iter()
        .map(|t| tokio::spawn(run(config.kafka.clone(), t, client.clone(), cancel.clone())))
        .collect::<FuturesUnordered<_>>();

    join_all(tasks).await;
}

fn create_consumer(config: Kafka) -> StreamConsumer {
    let mut c = ClientConfig::new();
    c.set("bootstrap.servers", config.brokers)
        .set("security.protocol", config.security_protocol)
        .set("enable.partition.eof", "false")
        .set("group.id", config.consumer_group)
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "true")
        .set("enable.auto.offset.store", "false")
        .set("auto.offset.reset", config.offset_reset)
        .set_log_level(RDKafkaLogLevel::Debug);

    if let Some(ssl) = config.ssl {
        if let Some(value) = ssl.ca_location {
            c.set("ssl.ca.location", value);
        }
        if let Some(value) = ssl.key_location {
            c.set("ssl.key.location", value);
        }
        if let Some(value) = ssl.certificate_location {
            c.set("ssl.certificate.location", value);
        }
        if let Some(value) = ssl.key_password {
            c.set("ssl.key.password", value);
        }
    }

    c.create().expect("Failed to create Kafka consumer")
}

fn deserialize_message(m: &BorrowedMessage) -> (String, Option<String>) {
    let key = match m.key_view::<str>() {
        None => "",
        Some(Ok(k)) => k,
        Some(Err(e)) => {
            error!("Error while deserializing message key: {:?}", e);
            ""
        }
    };
    let payload = match m.payload_view::<str>() {
        None => None,
        Some(Ok(s)) => Some(s),
        Some(Err(e)) => {
            error!("Error while deserializing message payload: {:?}", e);
            None
        }
    };

    (key.to_owned(), payload.map(str::to_string).to_owned())
}

#[cfg(test)]
mod tests {
    use crate::fhir_client::FhirClient;
    use crate::fhir_client::tests::setup_config;
    use crate::run;
    use httpmock::Method::{GET, POST};
    use httpmock::{HttpMockRequest, HttpMockResponse, MockServer};
    use rdkafka::mocking::MockCluster;
    use rdkafka::producer::future_producer::OwnedDeliveryResult;
    use rdkafka::producer::{FutureProducer, FutureRecord};
    use serde_json::json;
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};
    use tokio::select;
    use tokio_util::sync::CancellationToken;

    #[tokio::test]
    async fn test_run() {
        const TOPIC: &str = "test_topic";

        // create mock cluster
        let mock_cluster = MockCluster::new(3).unwrap();
        mock_cluster
            .create_topic(TOPIC, 3, 3)
            .expect("Failed to create topic");

        let mock_producer: FutureProducer = rdkafka::ClientConfig::new()
            .set("bootstrap.servers", mock_cluster.bootstrap_servers())
            .create()
            .expect("Producer creation error");

        send_record(mock_producer.clone(), TOPIC, "test")
            .await
            .unwrap();

        // task cancellation
        let token = CancellationToken::new();
        let cloned_token = token.clone();

        // create mock fhir server
        let server = MockServer::start();
        // metadata mock
        let metadata_mock = server.mock(|when, then| {
            when.method(GET).path("/metadata");
            then.status(200).body("OK");
        });
        // post data mock
        let post_mock = server.mock(|when, then| {
            let token = token.clone();
            when.method(POST).path("/").body("test");
            then.respond_with(move |_: &HttpMockRequest| {
                println!("cancelling token");
                token.cancel();

                HttpMockResponse::builder()
                    .status(200)
                    .body(json!({"id":"ok"}).to_string())
                    .build()
            });
        });

        // setup config
        let mut config = setup_config(server.base_url());
        config.kafka.brokers = mock_cluster.bootstrap_servers();
        config.kafka.offset_reset = "earliest".to_string();
        config.kafka.security_protocol = "plaintext".to_string();
        config.kafka.consumer_group = "test".to_string();

        // create new client
        let client = FhirClient::new(&config).await.unwrap();

        tokio::spawn(
            async move { run(config.kafka, TOPIC.to_string(), Arc::new(client), token).await },
        );
        select! {
            _ = cloned_token.cancelled() => {
                // The token was canceled
                println!("task canceled");
            }
            _ = tokio::time::sleep(std::time::Duration::from_secs(30)) => {
                // timeout
                println!("timeout waiting for task");
            }
        }

        // mocks were called once
        metadata_mock.assert();
        post_mock.assert();
    }

    async fn send_record(
        producer: FutureProducer,
        topic: &str,
        payload: &str,
    ) -> OwnedDeliveryResult {
        producer
            .send_result(
                FutureRecord::to(topic)
                    .key("test")
                    .payload(payload)
                    .timestamp(
                        SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap()
                            .as_millis()
                            .try_into()
                            .unwrap(),
                    ),
            )
            .unwrap()
            .await
            .unwrap()
    }
}
