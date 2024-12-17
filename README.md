# flint 💥

[![MegaLinter](https://github.com/diz-unimr/flint/actions/workflows/mega-linter.yml/badge.svg)](https://github.com/diz-unimr/flint/actions/workflows/mega-linter.yml)
[![build](https://github.com/diz-unimr/flint/actions/workflows/build.yaml/badge.svg)](https://github.com/diz-unimr/flint/actions/workflows/build.yaml)
[![docker](https://github.com/diz-unimr/flint/actions/workflows/release.yaml/badge.svg)](https://github.com/diz-unimr/flint/actions/workflows/release.yaml)
[![codecov](https://codecov.io/gh/diz-unimr/flint/graph/badge.svg?token=ClPe13QC4b)](https://codecov.io/gh/diz-unimr/flint)


> Send FHIR 🔥 data from Kafka to a server

## Concurrency

In order to enable Multi-threaded message consumption while ensuring global topic message order
**one consumer per input topic** is used.

## Offset handling

By default, the consumers are configured to auto-commit offsets, in order to improve performance.

However, offsets are only committed for messages which were successfully sent to the FHIR server.

## Retry capabilities

The HTTP client supports retrying requests to the FHIR server in case the target endpoint is unavailable
or runs into a timeout. See [configuration properties](#configuration-properties) below.

## Validation

FHIR resource types are currently not validated. Processing requires valid FHIR bundles as input data and sends
them to the server without validation.

## Configuration properties

Application properties are read from a properties file ([app.yaml](./app.yaml)) with default values.

| Name                              | Default                      | Description                                   |
|-----------------------------------|------------------------------|-----------------------------------------------|
| `app.log_level`                   | info                         | Log level (error,warn,info,debug,trace)       |
| `kafka.brokers`                   | localhost:9092               | Kafka brokers                                 |
| `kafka.security_protocol`         | plaintext                    | Kafka communication protocol                  |
| `kafka.ssl.ca_location`           | /app/cert/kafka_ca.pem       | Kafka CA certificate location                 |
| `kafka.ssl.certificate_location`  | /app/cert/app_cert.pem       | Client certificate location                   |
| `kafka.ssl.key_location`          | /app/cert/app_key.pem        | Client key location                           |
| `kafka.ssl.key_password`          |                              | Client key password                           |
| `kafka.consumer_group`            | fhir-to-diz                  | Consumer group name                           |
| `kafka.input_topics`              |                              | Kafka topics to consume (comma separated)     |
| `kafka.offset_reset`              | earliest                     | Kafka consumer reset (`earliest` or `latest`) |
| `fhir.server.base_url`            | <http://localhost:8080/fhir> | FHIR server base URL                          |
| `fhir.server.auth.basic.user`     |                              | FHIR server BasicAuth username                |
| `fhir.server.auth.basic.password` |                              | FHIR server BasicAuth password                |
| `fhir.retry.count`                | 10                           | Retry count                                   |
| `fhir.retry.timeout`              | 10                           | Retry timeout                                 |
| `fhir.retry.wait`                 | 5                            | Retry wait between retries                    |
| `fhir.retry.max_wait`             | 20                           | Retry maximum wait                            |

### Environment variables

Override configuration properties by providing environment variables with their respective property names.

## License

[AGPL-3.0](https://www.gnu.org/licenses/agpl-3.0.en.html)
