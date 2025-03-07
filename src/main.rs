mod config;
mod fhir_client;

use crate::config::Kafka;
use crate::fhir_client::FhirClient;
use config::AppConfig;
use futures::stream::FuturesUnordered;
use futures::{StreamExt, TryStreamExt};
use log::{debug, error, info};
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::{BorrowedMessage, Headers, Message};
use rdkafka::ClientConfig;
use std::env;
use std::sync::Arc;

async fn run(config: AppConfig, topic: String, client: FhirClient) {
    // create consumer
    let consumer: StreamConsumer = create_consumer(config.kafka);
    match consumer.subscribe(&[&topic]) {
        Ok(_) => {
            info!("Successfully subscribed to topic: {:?}", topic);
        }
        Err(error) => error!("Failed to subscribe to specified topic: {}", error),
    }
    let consumer = Arc::new(consumer);

    let stream = consumer
        .stream()
        .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)
        .try_for_each(|m| {
            let consumer = consumer.clone();
            let client = client.clone();

            {
                let topic = topic.clone();
                async move {
                    let (key, payload) = deserialize_message(&m);

                    debug!("key: '{}', payload: '{}', topic: {}, partition: {}, offset: {}, timestamp: {:?}",
                        key,payload.as_deref().unwrap_or("[null]"),m.topic(),m.partition(),m.offset(),m.timestamp());

                    if let Some(headers) = m.headers() {
                        for header in headers.iter() {
                            debug!("Header {:#?}: {:?}", header.key, header.value);
                        }
                    }

                    // filter tombstone records
                    if payload.is_none() {
                        return Ok(())
                    }


                    // send payload to FHIR server
                    let res = client.send(&payload.unwrap()).await;
                    match res {
                        Ok(b) => {
                            if b.status().is_success() {
                                debug!("Response indicates success: {}", b.text().await.unwrap());

                                // store offset
                                consumer
                                    .store_offset_from_message(&m)
                                    .expect("Failed to store offset for message");
                                Ok(())
                            } else {
                                error!("Error response: {}", b.status());
                                // stop processing
                                consumer.unsubscribe();
                                Err(format!("Failed to send payload to the FHIR server (status: {}). Stopping consumer for {}", b.status(), topic).into())
                            }
                        }
                        Err(e) => {
                            // stop processing
                            error!("Got an error: {}", e);
                            consumer.unsubscribe();
                            Err(Box::new(e) as Box<dyn std::error::Error>)
                        }
                    }
                }
            }
        });

    info!("Starting consumers");
    let error = stream.await;
    info!("Consumers terminated: {:?}", error);
}

#[tokio::main]
async fn main() {
    let config = match AppConfig::new() {
        Ok(s) => s,
        Err(e) => panic!("Failed to parse app settings: {e:?}"),
    };
    env::set_var("RUST_LOG", config.app.log_level.clone());
    env_logger::init();

    let client = match FhirClient::new(&config).await {
        Ok(c) => c,
        Err(e) => panic!("Failed create HTTP FHIR client: {e:?}"),
    };

    let topics = config
        .kafka
        .input_topics
        .split(',')
        .map(String::from)
        .collect::<Vec<String>>();
    topics
        .iter()
        .map(|t| tokio::spawn(run(config.clone(), t.clone(), client.clone())))
        .collect::<FuturesUnordered<_>>()
        .for_each(|_| async { () })
        .await
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
    use crate::fhir_client::tests::setup_config;
    use crate::fhir_client::FhirClient;
    use crate::run;
    use httpmock::Method::{GET, POST};
    use httpmock::MockServer;
    use rdkafka::mocking::MockCluster;
    use rdkafka::producer::future_producer::OwnedDeliveryResult;
    use rdkafka::producer::{FutureProducer, FutureRecord};
    use std::time::{SystemTime, UNIX_EPOCH};

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
        send_record(mock_producer.clone(), TOPIC, "done")
            .await
            .unwrap();

        // create mock fhir server
        let server = MockServer::start();
        server.mock(|when, then| {
            when.method(GET).path("/metadata");
            then.status(200).body("OK");
        });
        let post_mock = server.mock(|when, then| {
            when.method(POST).path("/").body("test");
            then.status(200).body("OK");
        });
        let done = server.mock(|when, then| {
            when.method(POST).path("/").body("done");
            then.status(500).body("done");
        });
        // setup config
        let mut config = setup_config(server.base_url());
        config.kafka.brokers = mock_cluster.bootstrap_servers();
        config.kafka.offset_reset = String::from("earliest");
        config.kafka.security_protocol = String::from("plaintext");
        config.kafka.consumer_group = String::from("test");

        // create new client
        let client = FhirClient::new(&config).await.unwrap();

        // run
        run(config, TOPIC.to_string(), client.clone()).await;

        // mock was called once
        post_mock.assert();
        done.assert();
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
