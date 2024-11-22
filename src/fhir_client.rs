use crate::config::AppConfig;
use log::{debug, error, info};
use reqwest::header::HeaderMap;
use reqwest::{header, Client, Response};
use reqwest_middleware::{ClientBuilder, ClientWithMiddleware, Error};
use reqwest_retry::{policies::ExponentialBackoff, RetryTransientMiddleware};
use std::future::Future;
use std::time::Duration;

#[derive(Clone)]
pub(crate) struct FhirClient {
    client: ClientWithMiddleware,
    headers: HeaderMap,
    url: String,
}

impl FhirClient {
    pub(crate) async fn new(config: &AppConfig) -> Result<Self, Error> {
        let mut headers = HeaderMap::new();
        headers.insert(header::CONTENT_TYPE, header::HeaderValue::from_static("application/fhir+json"));

        let retry = ExponentialBackoff::builder()
            .retry_bounds(
                Duration::from_secs(config.fhir.retry.wait),
                Duration::from_secs(config.fhir.retry.max_wait))
            .build_with_max_retries(config.fhir.retry.count);

        let client = Client::builder().timeout(Duration::from_secs(config.fhir.retry.timeout))
            .default_headers(headers.clone()).build();
        match client {
            Ok(client) =>
                {
                    let mut req_builder = client.get(config.fhir.server.base_url.clone() + "/metadata");
                    if let Some(auth) = config.fhir.server.auth.as_ref()
                        .and_then(|a| a.basic.as_ref()) {
                        if let (Some(user), Some(password)) = (auth.user.clone(), auth.password.clone()) {
                            // auth header
                            req_builder = req_builder.basic_auth(user, Some(password));
                        }
                    }

                    // retry
                    let client = ClientBuilder::new(Client::builder().default_headers(headers.clone()).build()?)
                        .with(RetryTransientMiddleware::new_with_policy(retry))
                        .build();

                    // test connection
                    info!("Testing connection to the FHIR server...");
                    match req_builder.send().await {
                        Ok(_) => {
                            info!("Connection successful!");
                            Ok(FhirClient {
                                client,
                                headers,
                                url: config.fhir.server.base_url.clone(),
                            })
                        }
                        Err(e) => {
                            error!("Connection failed!");
                            Err(e.into())
                        }
                    }
                }
            Err(e) => Err(e.into())
        }
    }


    pub(crate) fn send(self, payload: &str) -> impl Future<Output=Result<Response, Error>> {
        debug!("Sending bundle to: {}",self.url);
        self.client.post(self.url).headers(self.headers).body(payload.to_owned()).send()
    }
}
