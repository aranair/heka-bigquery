# heka-bigquery
Heka output plugin for persisting messages from the data pipeline to BigQuery. 

It consumes data from a Kafka Topic into a buffer variable + local file(for backup) and uploads periodically to BigQuery when the buffer is of a certain file size. 

Contains a ticker that checks for midnight and creates new tables daily in BigQuery. The intervals are in the code as constants.

This plugin is currently used in Wego.com


## Configuration
Uses toml (heka plugin default) for configuration. See `heka_config.toml.sample` for reference.

Bigquery schema file is specified by a json file. See `realtime_log.schema.sample` for reference.

Private key for BigQuery is specified by a pkcs12 format PEM file that was converted (password removed) from the p12 file originally obtained from developer's console: `https://console.developers.google.com/project/{project_id}/apiui/credential`. More information here: https://www.openssl.org/docs/apps/pkcs12.html.

Table names in BigQuery will {dataset_id}/{table_id}{date_stamp}. date_stamp formats as such: 20151230

If no Encoder is specified in TOML, then the message Payload is extracted and sent.

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
## Credits

Developed in collaboration, with Alex when he built https://github.com/uohzxela/heka-s3 the heka plugin to upload to S3 for Wego.

## Contributing

1. Fork it
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Added some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create new Pull Request

## Copyright

Copyright 2015 Boa Ho Man

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
