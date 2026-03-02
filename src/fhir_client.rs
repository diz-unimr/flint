use crate::config::AppConfig;
use crate::fhir_client::FhirClientError::{ClientError, ResponseError};
use anyhow::anyhow;
use log::{error, info, trace};
use reqwest::header::{AUTHORIZATION, HeaderMap, HeaderValue};
use reqwest::{Client, StatusCode, header};
use reqwest_middleware::{ClientBuilder, ClientWithMiddleware};
use reqwest_retry::{RetryTransientMiddleware, policies::ExponentialBackoff};
use serde_derive::Deserialize;
use std::str::FromStr;
use std::time::Duration;
use thiserror::Error;

#[derive(Error, Debug)]
pub(crate) enum FhirClientError {
    #[error("client error: {0}")]
    ClientError(#[from] anyhow::Error),
    #[error("response error: {0}")]
    ResponseError(String),
}

#[derive(Debug, Clone)]
pub(crate) struct FhirClient {
    client: ClientWithMiddleware,
    url: String,
}
#[derive(Deserialize)]
pub(crate) struct ResponseBundle {
    pub(crate) id: String,
    entry: Vec<ResponseBundleEntry>,
}
#[derive(Deserialize)]
struct ResponseBundleEntry {
    response: EntryResponse,
}
#[derive(Deserialize)]
struct EntryResponse {
    status: String,
}

impl ResponseBundle {
    fn success(self) -> Result<Self, FhirClientError> {
        match self.entry.iter().all(|b| {
            StatusCode::from_str(b.response.status.as_str())
                .map(|status| status.is_success())
                .unwrap_or(false)
        }) {
            true => Ok(self),
            false => Err(ResponseError(
                "Status code of at least one entry did not indicate success".into(),
            )),
        }
    }
}

impl FhirClient {
    pub(crate) async fn new(config: &AppConfig) -> anyhow::Result<Self> {
        // default headers
        let mut headers = HeaderMap::new();
        headers.insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/fhir+json"),
        );
        // set auth header as default
        if let Some(auth) = config
            .fhir
            .server
            .auth
            .as_ref()
            .and_then(|a| a.basic.as_ref())
            && let (Some(user), Some(password)) = (auth.user.clone(), auth.password.clone())
        {
            // auth header
            let auth_value = create_auth_header(user, Some(password));
            headers.insert(header::AUTHORIZATION, auth_value);
        }

        // retry
        let retry = ExponentialBackoff::builder()
            .retry_bounds(
                Duration::from_secs(config.fhir.retry.wait),
                Duration::from_secs(config.fhir.retry.max_wait),
            )
            .build_with_max_retries(config.fhir.retry.count);

        // client with retry middleware
        let client = ClientBuilder::new(
            Client::builder()
                .default_headers(headers.clone())
                .timeout(Duration::from_secs(config.fhir.retry.timeout))
                .build()?,
        )
        .with(RetryTransientMiddleware::new_with_policy(retry))
        .build();

        // test connection
        info!("Connecting to FHIR server..");
        match client
            .get(config.fhir.server.base_url.clone() + "/metadata")
            .send()
            .await
        {
            Ok(resp) if resp.status().is_success() => {
                info!("Connection successful");
                Ok(FhirClient {
                    client,
                    url: config.fhir.server.base_url.clone(),
                })
            }
            Ok(resp) => {
                error!("Metadata response returned error code: {}", resp.status());
                Err(anyhow!("Unsuccessful metadata request"))
            }
            Err(e) => {
                error!("Connection to FHIR server failed, {e}");
                Err(e.into())
            }
        }
    }

    pub(crate) async fn send(&self, payload: &str) -> Result<ResponseBundle, FhirClientError> {
        trace!("Sending bundle to: {}", self.url);
        let response = self
            .client
            .post(&self.url)
            .body(payload.to_owned())
            .send()
            .await
            .map_err(|e| ClientError(e.into()))?;

        let bundle = response.json::<ResponseBundle>().await.map_err(|e| {
            ResponseError(format!("Failed to parse response from FHIR server: {e}"))
        })?;

        bundle.success()
    }
}

fn create_auth_header(user: String, password: Option<String>) -> HeaderValue {
    let builder = Client::new()
        .get("http://localhost")
        .basic_auth(user, password);

    builder
        .build()
        .unwrap()
        .headers()
        .get(AUTHORIZATION)
        .unwrap()
        .clone()
}

#[cfg(test)]
pub(crate) mod tests {
    use crate::config::{App, AppConfig, Auth, Basic, Fhir, Kafka, Retry, Server};
    use crate::fhir_client::FhirClient;
    use httpmock::Method::GET;
    use httpmock::MockServer;

    fn init() {
        let _ = env_logger::builder().is_test(true).try_init();
    }

    pub(crate) fn setup_config(
        fhir_base_url: String,
        kafka_bootstrap_servers: String,
        kafka_topics: String,
    ) -> AppConfig {
        AppConfig {
            app: App::default(),
            kafka: Kafka {
                brokers: kafka_bootstrap_servers,
                security_protocol: "plaintext".to_string(),
                ssl: None,
                consumer_group: "test".to_string(),
                input_topics: kafka_topics,
                offset_reset: "earliest".to_string(),
            },
            fhir: Fhir {
                server: Server {
                    base_url: fhir_base_url,
                    auth: Some(Auth {
                        basic: Some(Basic {
                            user: Some(String::from("foo")),
                            password: Some(String::from("bar")),
                        }),
                    }),
                },
                retry: Retry {
                    timeout: 1,
                    ..Default::default()
                },
            },
        }
    }

    #[tokio::test]
    async fn test_new_ok() {
        init();
        let server = MockServer::start();
        let metadata_mock = server.mock(|when, then| {
            when.method(GET)
                .path("/metadata")
                .header_exists("Authorization");
            then.status(200).body("OK");
        });

        let config = setup_config(server.base_url(), String::new(), String::new());
        // create new client
        let client = FhirClient::new(&config).await;

        // mock was called once
        metadata_mock.assert();

        // assert client is created
        assert!(client.is_ok());
    }

    #[tokio::test]
    async fn test_new_error() {
        init();

        let server = MockServer::start();
        let metadata_mock = server.mock(|when, then| {
            when.method(GET)
                .path("/metadata")
                .header_exists("Authorization");
            then.status(404);
        });

        let config = setup_config(server.base_url(), String::new(), String::new());
        // create new client
        let client = FhirClient::new(&config).await;

        // mock was called once
        metadata_mock.assert();

        // assert client is created
        assert!(client.is_err());
    }
}
