use futures::stream::FuturesUnordered;
use futures::{StreamExt, TryStreamExt};
use log::{error, info, warn};
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::message::{BorrowedMessage, Headers, Message, OwnedMessage};
use rdkafka::ClientConfig;
use reqwest::Client;
use std::env;
use std::sync::Arc;

#[tokio::main]
async fn main() {
    env::set_var("RUST_LOG", "info");
    env_logger::init();

    let brokers = "localhost:9092";
    let group_id = "kafka-rust";
    let topics = ["person-fhir", "lab-fhir"];

    topics.iter()
        .map(|t| {
            tokio::spawn(run(
                brokers,
                group_id,
                t,
            ))
        })
        .collect::<FuturesUnordered<_>>()
        .for_each(|_| async { () })
        .await
}

fn create_consumer(brokers: &str, group_id: &str) -> StreamConsumer {
    ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("enable.partition.eof", "false")
        .set("group.id", &*group_id)
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "true")
        .set("enable.auto.offset.store", "false")
        .set("auto.offset.reset", "earliest")
        .set_log_level(RDKafkaLogLevel::Debug)
        .create()
        .expect("Failed to create consumer")
}

async fn run(brokers: &str, group_id: &str, topic: &str) {
    // create consumer(s)
    let consumer: StreamConsumer = create_consumer(brokers, group_id);
    consumer
        .subscribe(&[&topic])
        .expect("Can't subscribe to specified topics");
    let consumer = Arc::new(consumer);

    let stream_processor = consumer.stream().try_for_each(|m| {
        let client = Client::new();
        let consumer = consumer.clone();


        async move {
            let (key, payload) = deserialize_message(&m);
            info!("key: '{}', payload: '{}', topic: {}, partition: {}, offset: {}, timestamp: {:?}",
                    key,payload, m.topic(), m.partition(), m.offset(), m.timestamp());
            if let Some(headers) = m.headers() {
                for header in headers.iter() {
                    info!("Header {:#?}: {:?}", header.key, header.value);
                }
            }

            // tokio::spawn(async move {
            let url = "http://localhost/fhir";

            let res = client.post(url)
                .body("the exact body that is sent")
                .send()
                .await;

            match res {
                Ok(b) => info!("Response indicates success: {}", b.text().await.unwrap().to_string()),
                Err(e) => error!("Got an error: {}", e),
            }

            // if res.status().is_success() {
            //     info!("Response indicates success: {}", res.text().await?);
            // } else {
            //     info!("Error response: {}", res.status());
            // }

            consumer.store_offset_from_message(&m).expect("Failed to store offset for message");

            Ok(())
        }
    });

    info!("Starting event loop");
    stream_processor.await.expect("stream processing failed");
    info!("Stream processing terminated");
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