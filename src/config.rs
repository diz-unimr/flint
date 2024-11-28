use config::{Config, ConfigError, Environment, File};
use serde_derive::Deserialize;

#[derive(Default, Debug, Deserialize, Clone)]
pub(crate) struct App {
    pub(crate) log_level: String,
}

#[derive(Default, Debug, Deserialize, Clone)]
pub(crate) struct Kafka {
    pub(crate) brokers: String,
    pub(crate) security_protocol: String,
    pub(crate) ssl: Option<Ssl>,
    pub(crate) consumer_group: String,
    pub(crate) input_topics: String,
    pub(crate) offset_reset: String,
}

#[derive(Default, Debug, Deserialize, Clone)]
pub(crate) struct Ssl {
    pub(crate) ca_location: Option<String>,
    pub(crate) certificate_location: Option<String>,
    pub(crate) key_location: Option<String>,
    pub(crate) key_password: Option<String>,
}

#[derive(Default, Debug, Deserialize, Clone)]
pub(crate) struct Fhir {
    pub(crate) server: Server,
    pub(crate) retry: Retry,
}

#[derive(Default, Debug, Deserialize, Clone)]
pub(crate) struct Server {
    pub(crate) base_url: String,
    pub(crate) auth: Option<Auth>,
}

#[derive(Default, Debug, Deserialize, Clone)]
pub(crate) struct Auth {
    pub(crate) basic: Option<Basic>,
}

#[derive(Default, Debug, Deserialize, Clone)]
pub(crate) struct Basic {
    pub(crate) user: Option<String>,
    pub(crate) password: Option<String>,
}

#[derive(Default, Debug, Deserialize, Clone)]
pub(crate) struct Retry {
    pub(crate) count: u32,
    pub(crate) timeout: u64,
    pub(crate) wait: u64,
    pub(crate) max_wait: u64,
}

#[derive(Default, Debug, Deserialize, Clone)]
pub(crate) struct AppConfig {
    pub(crate) app: App,
    pub(crate) kafka: Kafka,
    pub(crate) fhir: Fhir,
}

impl AppConfig {
    pub(crate) fn new() -> Result<Self, ConfigError> {
        Config::builder()
            // default config from file
            .add_source(File::with_name("app.yaml"))
            // override values from environment variables
            .add_source(Environment::with_prefix("FLINT").separator("__"))
            .build()?
            .try_deserialize()
    }
}
