use crate::config::AppConfig;
use log::info;
use reqwest::header::HeaderMap;
use reqwest::{header, Client, Error, Response};
use std::future::Future;

#[derive(Clone)]
pub(crate) struct FhirClient {
    client: Client,
    headers: HeaderMap,
    url: String,
}

impl FhirClient {
    pub(crate) fn new(config: &AppConfig) -> Result<Self, Error> {
        let mut headers = HeaderMap::new();
        headers.insert(header::CONTENT_TYPE, header::HeaderValue::from_static("application/fhir+json"));

        let client = Client::builder().default_headers(headers.clone());
        match client.build() {
            Ok(client) =>
                Ok(
                    {
                        // save auth header
                        // TODO test
                        let req = client.get(config.fhir().server().base_url().to_owned() + "/metadata");
                        if let Some(auth) = config.fhir().server().auth().as_ref()
                            .and_then(|a| a.basic().as_ref()) {
                            if let (Some(user), Some(password)) = (auth.user(), auth.password()) {
                                headers = req.basic_auth(user, Some(password)).build()?.headers().clone();
                            }
                        }

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
