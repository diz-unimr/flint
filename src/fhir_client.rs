use crate::config::AppConfig;
use log::{debug, error, info};
use reqwest::header::HeaderMap;
use reqwest::{header, Client, Response};
use reqwest_middleware::{ClientBuilder, ClientWithMiddleware, Error};
use reqwest_retry::{policies::ExponentialBackoff, RetryTransientMiddleware};
use std::future::Future;
use std::time::Duration;

#[derive(Debug, Clone)]
pub(crate) struct FhirClient {
    client: ClientWithMiddleware,
    headers: HeaderMap,
    url: String,
}

impl FhirClient {
    pub(crate) async fn new(config: &AppConfig) -> Result<Self, Box<dyn std::error::Error>> {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::CONTENT_TYPE,
            header::HeaderValue::from_static("application/fhir+json"),
        );

        let retry = ExponentialBackoff::builder()
            .retry_bounds(
                Duration::from_secs(config.fhir.retry.wait),
                Duration::from_secs(config.fhir.retry.max_wait),
            )
            .build_with_max_retries(config.fhir.retry.count);

        let client = Client::builder()
            .timeout(Duration::from_secs(config.fhir.retry.timeout))
            .default_headers(headers.clone())
            .build();
        match client {
            Ok(client) => {
                let mut req_builder = client.get(config.fhir.server.base_url.clone() + "/metadata");
                if let Some(auth) = config
                    .fhir
                    .server
                    .auth
                    .as_ref()
                    .and_then(|a| a.basic.as_ref())
                {
                    if let (Some(user), Some(password)) = (auth.user.clone(), auth.password.clone())
                    {
                        // auth header
                        req_builder = req_builder.basic_auth(user, Some(password));
                    }
                }

                // retry
                let client =
                    ClientBuilder::new(Client::builder().default_headers(headers.clone()).build()?)
                        .with(RetryTransientMiddleware::new_with_policy(retry))
                        .build();

                // test connection
                match req_builder.send().await {
                    Ok(resp) => {
                        info!("Connection successful");
                        if resp.status().is_success() {
                            Ok(FhirClient {
                                client,
                                headers,
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
            Err(e) => Err(e.into()),
        }
    }

    pub(crate) fn send(self, payload: &str) -> impl Future<Output = Result<Response, Error>> {
        debug!("Sending bundle to: {}", self.url);
        self.client
            .post(self.url)
            .headers(self.headers)
            .body(payload.to_owned())
            .send()
    }
}

#[cfg(test)]
mod tests {
    use crate::config::{App, AppConfig, Fhir, Kafka, Retry, Server};
    use crate::fhir_client::FhirClient;

    fn init() {
        let _ = env_logger::builder().is_test(true).try_init();
    }

    fn setup_config(base_url: String) -> AppConfig {
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

        let mut config = setup_config(server.base_url());
        config.fhir.server.base_url = server.base_url();
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
