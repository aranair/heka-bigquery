# heka-bigquery
Heka output plugin for persisting messages from the data pipeline to BigQuery. It buffers into a variable + file locally and uploads periodically to BigQuery based on a file size limit. Used in Wego. 

Consumes data from a Kafka topic and stores into variable + file.

Uploads periodically up to BigQuery from file size.

Contains a ticker that checks for midnight and creates new tables daily in BigQuery. The intervals are in the code as constants.

## Configuration
Uses toml (heka plugin default) for configuration. See `heka_config.toml.sample` for reference.

Bigquery schema file is specified by a json file. See `realtime_log.schema.sample` for reference.

Private key for BigQuery is specified by a pkcs12 format PEM file that was converted (password removed) from the p12 file originally obtained from developer's console: `https://console.developers.google.com/project/{project_id}/apiui/credential`. More information here: https://www.openssl.org/docs/apps/pkcs12.html.

## Sample TOML file (with Kafka as input source):

```
[realtime-log-input-kafka]
type = "KafkaInput"
topic = "realtime-log"
addrs = ["kafka-a-1.bezurk.org:9092", "kafka-b-1.bezurk.org:9092"]

[realtime-log-output-bq]
type = "BqOutput"
message_matcher = "Logger == 'realtime-log-input-kafka'"
project_id = "org-project"
dataset_id = "go_realtime_log"
table_id = "log"
schema_file_path = "/var/apps/shared/config/realtime_log.schema"
pem_file_path = "/var/apps/shared/config/big_query.pem"
buffer_path = "/var/buffer/bq"
buffer_file = "realtime_log"
ticker_interval = 5
```

## Installation

Refer to: http://hekad.readthedocs.org/en/v0.9.2/installing.html#building-hekad-with-external-plugins

Simply add this line in _{heka root}/cmake/plugin_loader.cmake_:

```
add_external_plugin(git https://github.com/aranair/heka-bigquery master)

Then run build.sh as per the documentation: `source build.sh`
```
