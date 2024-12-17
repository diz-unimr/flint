use crate::config::{AppConfig, Fhir};
use log::{debug, error, info};
use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION};
use reqwest::{header, Client, Response};
use reqwest_middleware::{ClientBuilder, ClientWithMiddleware, Error};
use reqwest_retry::{policies::ExponentialBackoff, RetryTransientMiddleware};
use std::future::Future;
use std::time::Duration;

#[derive(Debug, Clone)]
pub(crate) struct FhirClient {
    client: ClientWithMiddleware,
    url: String,
}

impl FhirClient {
    pub(crate) async fn new(config: &AppConfig) -> Result<Self, Box<dyn std::error::Error>> {
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
        {
            if let (Some(user), Some(password)) = (auth.user.clone(), auth.password.clone()) {
                // auth header
                let auth_value = create_auth_header(user, Some(password));
                headers.insert(header::AUTHORIZATION, auth_value);
            }
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
        match client
            .get(config.fhir.server.base_url.clone() + "/metadata")
            .send()
            .await
        {
            Ok(resp) => {
                info!("Connection successful");
                if resp.status().is_success() {
                    Ok(FhirClient {
                        client,
                        url: config.fhir.server.base_url.clone(),
                    })
                } else {
                    error!("Metadata response returned error code: {}", resp.status());
                    Err("Unsuccessful metadata request".into())
                }
            }
            Err(e) => {
                error!("Connection failed, {e}");
                Err(e.into())
            }
        }
    }

    pub(crate) fn send(self, payload: &str) -> impl Future<Output = Result<Response, Error>> {
        debug!("Sending bundle to: {}", self.url);
        self.client.post(self.url).body(payload.to_owned()).send()
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
    use crate::config::{App, AppConfig, Fhir, Kafka, Retry, Server};
    use crate::fhir_client::FhirClient;

    fn init() {
        let _ = env_logger::builder().is_test(true).try_init();
    }

    pub(crate) fn setup_config(base_url: String) -> AppConfig {
        AppConfig {
            app: App::default(),
            kafka: Kafka::default(),
            fhir: Fhir {
                server: Server {
                    base_url,
                    auth: None,
                },
                retry: Retry {
                    timeout: 5,
                    ..Default::default()
                },
            },
        }
    }

    #[tokio::test]
    async fn test_new_ok() {
        use httpmock::prelude::*;

        init();
        let server = MockServer::start();
        let metadata_mock = server.mock(|when, then| {
            when.method(GET).path("/metadata");
            then.status(200).body("OK");
        });

        let config = setup_config(server.base_url());
        // create new client
        let client = FhirClient::new(&config).await;

        // mock was called once
        metadata_mock.assert();

        // assert client is created
        assert!(client.is_ok());
    }

    #[tokio::test]
    async fn test_new_error() {
        use httpmock::prelude::*;

        let server = MockServer::start();
        let metadata_mock = server.mock(|when, then| {
            when.method(GET).path("/metadata");
            then.status(404);
        });

        let config = setup_config(server.base_url());
        // create new client
        let client = FhirClient::new(&config).await;

        // mock was called once
        metadata_mock.assert();

        // assert client is created
        assert!(client.is_err());
    }
}
