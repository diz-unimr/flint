use crate::ClientConfig;
use crate::config::AppConfig;
use crate::fhir_client::{FhirClient, FhirClientError};
use anyhow::anyhow;
use futures::TryStreamExt;
use futures::future::join_all;
use futures::stream::FuturesUnordered;
use log::{debug, error, info, trace, warn};
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::consumer::{BaseConsumer, Consumer, ConsumerContext, Rebalance, StreamConsumer};
use rdkafka::error::{KafkaError, KafkaResult};
use rdkafka::message::{BorrowedMessage, Headers};
use rdkafka::{ClientContext, Message, TopicPartitionList};
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::select;
use tokio::sync::mpsc::Sender;
use tokio_util::sync::CancellationToken;

#[derive(Error, Debug)]
pub(crate) enum ProcessingError {
    #[error(transparent)]
    FhirClient(#[from] FhirClientError),
    #[error("consumer for topic {0} was cancelled")]
    Cancelled(String),
    #[error(transparent)]
    Kafka(#[from] KafkaError),
}

pub(crate) struct Processor {
    config: AppConfig,
    ctx: Context,
    topics: Vec<String>,
    client: Arc<FhirClient>,
}

#[derive(Clone)]
pub(crate) struct Context {
    pub(crate) on_commit: Option<Sender<TopicPartitionList>>,
    pub(crate) cancel: CancellationToken,
}
type ProcessingConsumer = StreamConsumer<Context>;
impl ClientContext for Context {}
impl ConsumerContext for Context {
    fn pre_rebalance(&self, _: &BaseConsumer<Self>, rebalance: &Rebalance) {
        debug!("Pre rebalance {:?}", rebalance);
    }

    fn post_rebalance(&self, _: &BaseConsumer<Self>, rebalance: &Rebalance) {
        debug!("Post rebalance {:?}", rebalance);
    }

    fn commit_callback(&self, result: KafkaResult<()>, _offsets: &TopicPartitionList) {
        debug!("Committed offsets: {:?}", _offsets);

        if let Some(hook) = &self.on_commit {
            match result {
                Ok(_) => {
                    let sender = hook.clone();
                    let offsets = _offsets.clone();
                    tokio::spawn(async move {
                        if let Err(e) = sender.send(offsets).await {
                            error!("Failed to send commit_callback result: {e}");
                        }
                    });
                }
                Err(e) => {
                    warn!("Offset commit returned error: {e}");
                }
            }
        }
    }
}

impl Processor {
    pub(crate) async fn new(config: AppConfig, ctx: Context) -> anyhow::Result<Self> {
        let client = FhirClient::new(&config)
            .await
            .map(Arc::new)
            .map_err(|e| anyhow!("Failed to create HTTP client: {e}"))?;

        let topics = config
            .kafka
            .input_topics
            .split(',')
            .map(String::from)
            .collect::<Vec<String>>();

        Ok(Self {
            config,
            ctx,
            client,
            topics,
        })
    }

    pub(crate) async fn start(self) {
        let this = Arc::new(self);

        let tasks = this
            .topics
            .iter()
            .map(|topic| {
                let this = this.clone();
                tokio::spawn(this.run(topic.clone()))
            })
            .collect::<FuturesUnordered<_>>();

        join_all(tasks).await;
    }

    async fn run(self: Arc<Self>, topic: String) {
        loop {
            // create consumer
            let consumer = self.create_consumer();
            match consumer.subscribe(&[&topic]) {
                Ok(()) => {
                    info!("Successfully subscribed to topic {topic}");
                }
                Err(e) => {
                    error!("Failed to subscribe to specified topic: {e}");
                    // exit
                    break;
                }
            }
            let consumer = Arc::new(consumer);

            let stream = consumer
                .stream()
                .map_err(ProcessingError::from)
                .try_for_each(|m| self.process_message(m, consumer.clone()));

            info!("Starting consumer for topic: {topic}");
            match stream.await {
                // exit
                Err(ProcessingError::Cancelled(e)) => {
                    consumer.unsubscribe();
                    error!("{e}. Exiting.");
                    // exit loop
                    break;
                }
                Err(ProcessingError::FhirClient(FhirClientError::ResponseError(e))) => {
                    consumer.unsubscribe();
                    error!("Failed to process message: {e}. Exiting.");
                    // exit loop
                    break;
                }
                // continue
                Err(ProcessingError::Kafka(e)) => {
                    consumer.unsubscribe();
                    error!("Failed to process message: {e}. Retrying..");
                }
                // continue
                Err(ProcessingError::FhirClient(FhirClientError::ClientError(e))) => {
                    consumer.unsubscribe();
                    error!("Failed to process message: {e}. Retrying..");
                }
                // exit
                Ok(()) => {
                    warn!("Consumer stream for topic {topic} unexpectedly ended");
                    break;
                }
            };

            info!("Restarting consumer for topic {topic} in 10 seconds...");
            if self.should_continue(Duration::from_secs(10)).await {
                // The token was cancelled
                trace!("Consumer for topic {topic} was stopped by cancellation");
                break;
            }
        }
    }

    async fn process_message(
        &self,
        m: BorrowedMessage<'_>,
        consumer: Arc<ProcessingConsumer>,
    ) -> Result<(), ProcessingError> {
        let topic = m.topic();
        if self.ctx.cancel.is_cancelled() {
            return Err(ProcessingError::Cancelled(format!(
                "Consumer for topic {topic} cancelled"
            )));
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
        let b = self.client.send(&payload.unwrap()).await?;
        debug!("[Sent] bundle to FHIR server, id: {}", b.id);
        trace!("[Response] success");

        // store offset
        consumer.store_offset_from_message(&m)?;

        match self.ctx.cancel.is_cancelled() {
            true => Err(ProcessingError::Cancelled(format!(
                "Consumer for topic {topic} cancelled"
            ))),
            false => Ok(()),
        }
    }

    async fn should_continue(&self, wait: Duration) -> bool {
        select! {
            _ =  self.ctx.cancel.cancelled() => {
                true
            }
            _ = tokio::time::sleep(wait) => {
                false
            }
        }
    }

    fn create_consumer(&self) -> ProcessingConsumer {
        let config = self.config.kafka.clone();
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

        c.create_with_context(self.ctx.clone())
            .expect("Failed to create Kafka consumer")
    }
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
    use crate::fhir_client::FhirClientError;
    use crate::fhir_client::tests::setup_config;
    use crate::processor::{Context, ProcessingError, Processor};
    use futures::StreamExt;
    use httpmock::Method::{GET, POST};
    use httpmock::{HttpMockRequest, HttpMockResponse, MockServer};
    use rdkafka::consumer::Consumer;
    use rdkafka::mocking::MockCluster;
    use rdkafka::producer::future_producer::OwnedDeliveryResult;
    use rdkafka::producer::{DefaultProducerContext, FutureProducer, FutureRecord};
    use rdkafka::{ClientConfig, TopicPartitionList};
    use serde_json::json;
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};
    use tokio::sync::mpsc;
    use tokio_util::sync::CancellationToken;

    const TOPIC: &str = "test_topic";

    fn init_logger() {
        let _ = env_logger::try_init();
    }

    #[tokio::test]
    async fn start_test() {
        init_logger();

        let mock_cluster = setup_kafka(vec![(TOPIC, "test")]).await;

        // create mock fhir server
        let server = MockServer::start();
        // metadata mock
        let metadata_mock = server.mock(|when, then| {
            when.method(GET).path("/metadata");
            then.status(200).body("OK");
        });
        // test mock
        let test_mock = server.mock(|when, then| {
            when.method(POST).path("/").body("test");
            then.status(200)
                .json_body(json!({"id":"ok","entry":[{"response":{"status":"201"}}]}));
        });

        // setup config
        let config = setup_config(
            server.base_url(),
            mock_cluster.bootstrap_servers(),
            TOPIC.to_string(),
        );

        // consumer commit callback
        let (tx, mut rx) = mpsc::channel::<TopicPartitionList>(1);

        // processor
        let p = Processor::new(
            config,
            Context {
                cancel: CancellationToken::new(),
                on_commit: Some(tx.clone()),
            },
        )
        .await
        .unwrap();

        // run
        tokio::spawn(p.start());

        // wait for commit confirmation
        let committed = rx.recv().await;

        // fhir server mocks were called
        metadata_mock.assert();
        test_mock.assert();
        // offset committed for test topic
        assert_eq!(
            committed
                .unwrap()
                .to_topic_map()
                .into_keys()
                .map(|k| k.0)
                .next()
                .unwrap(),
            TOPIC
        );
    }

    #[tokio::test]
    async fn cancellation_test() {
        init_logger();

        let mock_cluster = setup_kafka(vec![(TOPIC, "first"), (TOPIC, "second")]).await;

        let token = CancellationToken::new();

        // create mock fhir server
        let server = MockServer::start();
        // metadata mock
        let metadata_mock = server.mock(|when, then| {
            when.method(GET).path("/metadata");
            then.status(200).body("OK");
        });
        // test mock
        let first_mock = server.mock(|when, then| {
            let token = token.clone();
            when.method(POST).path("/").body("first");
            then.respond_with(move |_: &HttpMockRequest| {
                // cancel consumer at next checkpoint
                token.cancel();

                // fhir server will return success
                HttpMockResponse::builder()
                    .status(200)
                    .body(json!({"id":"test","entry":[{"response":{"status":"201"}}]}).to_string())
                    .build()
            });
        });
        // cancelled mock
        let second_mock = server.mock(|when, then| {
            when.method(POST).path("/").body("second");
            then.json_body(
                json!({"id":"cancelled","entry":[{"response":{"status":"201"}}]}).to_string(),
            );
        });
        // setup config
        let config = setup_config(
            server.base_url(),
            mock_cluster.bootstrap_servers(),
            TOPIC.to_string(),
        );

        // processor
        let p = Processor::new(
            config,
            Context {
                cancel: token,
                on_commit: None,
            },
        )
        .await
        .unwrap();

        // run
        p.start().await;

        metadata_mock.assert();
        first_mock.assert();
        // second record was not processed
        assert_eq!(second_mock.calls(), 0);
    }

    #[tokio::test]
    async fn process_message_fails() {
        init_logger();

        let mock_cluster = setup_kafka(vec![(TOPIC, "ok")]).await;

        // create mock fhir server
        let server = MockServer::start();
        // metadata mock
        server.mock(|when, then| {
            when.method(GET).path("/metadata");
            then.status(200).body("OK");
        });
        server.mock(|when, then| {
            when.method(POST).path("/").body("ok");
            // should fail when deserializing to json body
            then.status(200).body("invalid json!");
        });
        // setup config
        let config = setup_config(
            server.base_url(),
            mock_cluster.bootstrap_servers(),
            TOPIC.to_string(),
        );

        // processor
        let token = CancellationToken::new();
        let p = Processor::new(
            config,
            Context {
                on_commit: None,
                cancel: token.clone(),
            },
        )
        .await
        .unwrap();
        let consumer = Arc::new(p.create_consumer());
        consumer.subscribe(&[TOPIC]).unwrap();

        let msg = consumer.stream().next().await.unwrap().unwrap();

        // act
        let result = p.process_message(msg, consumer.clone()).await;

        assert!(matches!(
            result,
            Err(ProcessingError::FhirClient(FhirClientError::ResponseError(
                _
            )))
        ));
    }

    async fn setup_kafka<'a>(
        records: Vec<(&str, &str)>,
    ) -> MockCluster<'a, DefaultProducerContext> {
        // create mock cluster
        let mock_cluster = MockCluster::new(3).unwrap();
        let mock_producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", mock_cluster.bootstrap_servers())
            .create()
            .expect("Producer creation error");

        for record in records {
            let _ = mock_cluster.create_topic(record.0, 3, 3);

            send_record(mock_producer.clone(), record.0, record.1)
                .await
                .unwrap();
        }

        mock_cluster
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
