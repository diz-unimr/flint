mod config;
mod fhir_client;

use crate::config::Kafka;
use crate::fhir_client::FhirClient;
use config::AppConfig;
use futures::stream::FuturesUnordered;
use futures::{StreamExt, TryStreamExt};
use log::{error, info, warn};
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::message::{BorrowedMessage, Headers, Message, OwnedMessage};
use rdkafka::ClientConfig;
use reqwest::{header, Client};
use std::collections::HashMap;
use std::env;
use std::sync::Arc;

async fn run(config: AppConfig, topic: String, client: FhirClient) {
    // create consumer
    let consumer: StreamConsumer = create_consumer(&config.kafka(), config.app().name());
    consumer
        .subscribe(&[&topic])
        .expect("Can't subscribe to specified topics");
    let consumer = Arc::new(consumer);

    let stream_processor = consumer.stream().try_for_each(|m| {
        let consumer = consumer.clone();
        let client = client.clone();

        async move {
            let (key, payload) = deserialize_message(&m);
            info!("key: '{}', payload: '{}', topic: {}, partition: {}, offset: {}, timestamp: {:?}",
                    key,payload, m.topic(), m.partition(), m.offset(), m.timestamp());
            if let Some(headers) = m.headers() {
                for header in headers.iter() {
                    info!("Header {:#?}: {:?}", header.key, header.value);
                }
            }

            let res = client.send(&payload).await;
            match res {
                Ok(b) => {
                    if b.status().is_success() {
                        info!("Response indicates success: {}", b.text().await.unwrap());
                    } else {
                        error!("Error response: {}", b.status());
                    }
                }
                Err(e) => // TODO skip processing
                    error!("Got an error: {}", e),
            }

            consumer.store_offset_from_message(&m).expect("Failed to store offset for message");

            Ok(())
        }
    });

    info!("Starting consumers");
    stream_processor.await.expect("stream processing failed");
    info!("Processing terminated");
}

#[tokio::main]
async fn main() {
    // TODO
    env::set_var("RUST_LOG", "info");
    env_logger::init();

    let config = match AppConfig::new() {
        Ok(s) => s,
        Err(e) => panic!("Failed to parse app settings: {e:?}"),
    };

    let client = match FhirClient::new(&config) {
        Ok(c) => c,
        Err(e) => panic!("Failed create HTTP FHIR client: {e:?}"),
    };

    let topics = config.kafka().input_topics().split(',').map(String::from).collect::<Vec<String>>();
    topics.iter()
        .map(|t| {
            tokio::spawn(run(
                config.clone(),
                t.clone(),
                client.clone(),
            ))
        })
        .collect::<FuturesUnordered<_>>()
        .for_each(|_| async { () })
        .await
}

fn create_consumer(config: &Kafka, group_id: &str) -> StreamConsumer {
    let mut c = ClientConfig::new();
    c.set("bootstrap.servers", config.brokers())
        .set("security.protocol", config.security_protocol())
        .set("enable.partition.eof", "false")
        .set("group.id", &*group_id)
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "true")
        .set("enable.auto.offset.store", "false")
        .set("auto.offset.reset", "earliest")
        .set_log_level(RDKafkaLogLevel::Debug);

    if let Some(ssl) = config.ssl() {
        if let Some(value) = ssl.ca_location() {
            c.set("ssl.ca.location", value);
        }
        if let Some(value) = ssl.key_location() {
            c.set("ssl.key.location", value);
        }
        if let Some(value) = ssl.certificate_location() {
            c.set("ssl.certificate.location", value);
        }
        if let Some(value) = ssl.key_password() {
            c.set("ssl.key.password", value);
        }
    }

    c.create()
        .expect("Failed to create Kafka consumer")
}

fn deserialize_message(m: &BorrowedMessage) -> (String, String) {
    let key = match m.key_view::<str>() {
        None => "",
        Some(Ok(k)) => k,
        Some(Err(e)) => {
            error!("Error while deserializing message key: {:?}", e);
            ""
        }
    };
    let payload = match m.payload_view::<str>() {
        None => "",
        Some(Ok(s)) => s,
        Some(Err(e)) => {
            error!("Error while deserializing message payload: {:?}", e);
            ""
        }
    };

    (key.to_owned(), payload.to_owned())
}