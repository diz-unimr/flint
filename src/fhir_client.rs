use crate::config::AppConfig;
use log::info;
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
    pub(crate) fn new(config: &AppConfig) -> Result<Self, Error> {
        let mut headers = HeaderMap::new();
        headers.insert(header::CONTENT_TYPE, header::HeaderValue::from_static("application/fhir+json"));

        let retry_config = config.fhir().retry();
        let retry = ExponentialBackoff::builder()
            .retry_bounds(
                Duration::from_secs(u64::from(*retry_config.wait())),
                Duration::from_secs(u64::from(*retry_config.max_wait())))
            .build_with_max_retries(*retry_config.count());

        let client = Client::builder().default_headers(headers.clone());
        match client.build() {
            Ok(client) =>
                Ok(
                    {
                        // test connection
                        let req = client.get(config.fhir().server().base_url().clone() + "/metadata");
                        // auth header
                        if let Some(auth) = config.fhir().server().auth().as_ref()
                            .and_then(|a| a.basic().as_ref()) {
                            if let (Some(user), Some(password)) = (auth.user(), auth.password()) {
                                headers = req.basic_auth(user, Some(password)).build()?.headers().clone();
                            }
                        }
                        // retry
                        let client = ClientBuilder::new(Client::builder().default_headers(headers.clone()).build()?)
                            .with(RetryTransientMiddleware::new_with_policy(retry))
                            .build();

                        FhirClient {
                            client,
                            headers,
                            url: config.fhir().server().base_url().clone(),
                        }
                    }),
            Err(e) => Err(e.into()),
        }
    }

    pub(crate) fn send(self, payload: &str) -> impl Future<Output=Result<Response, Error>> {
        info!("Sending bundle to: {}",self.url);
        self.client.post(self.url).headers(self.headers).body(payload.to_owned()).send()
    }
}
