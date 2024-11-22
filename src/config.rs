use config::{Config, ConfigError, Environment, File};
use derive_getters::Getters;
use serde_derive::Deserialize;

#[derive(Getters, Debug, Deserialize, Clone)]
#[allow(unused)]
pub(crate) struct App {
    log_level: String,
}

#[derive(Getters, Debug, Deserialize, Clone)]
#[allow(unused)]
pub(crate) struct Kafka {
    brokers: String,
    security_protocol: String,
    ssl: Option<Ssl>,
    consumer_group: String,
    input_topics: String,
    offset_reset: String,
}

#[derive(Getters, Debug, Deserialize, Clone)]
#[allow(unused)]
pub(crate) struct Ssl {
    ca_location: Option<String>,
    certificate_location: Option<String>,
    key_location: Option<String>,
    key_password: Option<String>,
}

#[derive(Getters, Debug, Deserialize, Clone)]
#[allow(unused)]
pub(crate) struct Fhir {
    server: Server,
    retry: Retry,
}

#[derive(Getters, Debug, Deserialize, Clone)]
#[allow(unused)]
pub(crate) struct Server {
    base_url: String,
    auth: Option<Auth>,
}

#[derive(Getters, Debug, Deserialize, Clone)]
#[allow(unused)]
pub(crate) struct Auth {
    basic: Option<Basic>,
}

#[derive(Getters, Debug, Deserialize, Clone)]
#[allow(unused)]
pub(crate) struct Basic {
    user: Option<String>,
    password: Option<String>,
}

#[derive(Getters, Debug, Deserialize, Clone)]
#[allow(unused)]
pub(crate) struct Retry {
    count: u32,
    timeout: u32,
    wait: u32,
    max_wait: u32,
}

#[derive(Getters, Debug, Deserialize, Clone)]
#[allow(unused)]
pub(crate) struct AppConfig {
    app: App,
    kafka: Kafka,
    fhir: Fhir,
}

impl AppConfig {
    pub(crate) fn new() -> Result<Self, ConfigError> {
        Config::builder()
            // default config from file
            .add_source(File::with_name("app.yaml")
            )

            // override values from environment variables
            .add_source(Environment::with_prefix("APP")
                .separator("__"))
            .build()?.try_deserialize()
    }
}