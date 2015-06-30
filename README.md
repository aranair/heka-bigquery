# heka-bigquery
- Heka output plugin for persisting messages from the data pipeline to BigQuery. It buffers into a variable + file locally and uploads periodically to BigQuery based on a file size limit. Used in Wego. 
- Consumes data from a Kafka topic and stores into variable + file.
- Uploads periodically up to BigQuery from file size.
- Checks clock for midnight and creates new tables in BigQuery. // TODO: Change to ticker for more reliability?

## Configuration
- Uses toml (heka defaults) for configuration. See `heka_config.toml.sample` for reference.
- Bigquery schema file is specified by a json file. See `analytics_visits.schema` for reference.
- Private key for BigQuery is specified by a pkcs12 format PEM file that was converted (password removed) from the p12 file originally obtained from developer's console: `https://console.developers.google.com/project/wego-cloud/apiui/credential`. More information here: https://www.openssl.org/docs/apps/pkcs12.html.

## Sample TOML file (with Kafka as input source):

```
[analytics-visits-input-kafka]
type = "KafkaInput"
topic = "analytics-visits"
addrs = ["kafka-a-1.bezurk.org:9092", "kafka-b-1.bezurk.org:9092"]

[analytics-visits-output-bq]
type = "BqOutput"
message_matcher = "Logger == 'analytics-visits-input-kafka'"
project_id = "wego-cloud"
dataset_id = "go_analytics_visits"
table_id = "visits"
buffer_path = "/var/buffer/bq"
buffer_file = "analytics_visits"
ticker_interval = 5
```

## Installation

Refer to: http://hekad.readthedocs.org/en/v0.9.2/installing.html#building-hekad-with-external-plugins

Simply add this line in _{heka root}/cmake/plugin_loader.cmake_:

    add_external_plugin(git https://github.com/aranair/heka-bigquery master)

    Then run build.sh as per the documentation: `source build.sh`
