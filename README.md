# heka-bigquery
Heka output plugin for persisting messages from the data pipeline to BigQuery. It buffers into a variable + file locally and uploads periodically to BigQuery based on a file size limit. Used in Wego. 

## Configuration

- Uses toml (heka defaults) for configuration. See `heka_config.toml.sample` for reference.
- Bigquery schema file is specified by a json file. See `analytics_visits.schema` for reference.
- Private key for BigQuery is specified by a pkcs12 format PEM file. More information here: https://www.openssl.org/docs/apps/pkcs12.html. The original file was obtained from developer's console: `https://console.developers.google.com/project/wego-cloud/apiui/credential`


Sample TOML file (with Kafka as input source):

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

## Sample Code (service account)

```
// Pem file that is converted from the .p12 file.
pkey, _ := ioutil.ReadFile("big_query.pem")
schema, _ := ioutil.ReadFile("test_schema.json")
projectId := "wego-cloud"
datasetId := "go_analytics_visits"
tableId   := "visits"

uploader := bq.NewBqUploader(pkey, projectId, datasetId)

err := uploader.CreateTable(tableId, schema)
if err != nil {
  fmt.Println(err)
}

list := make([]map[string]bigquery.JsonValue, 0)

f, err := os.Open("test_data.json")
buf := bufio.NewReader(f)

var data []byte
data, _ = buf.ReadBytes('\n')

for len(data) > 0 {
  data, _ = buf.ReadBytes('\n')
  list = append(list, bq.BytesToBqJsonRow(data))
}

insertErr := uploader.InsertRows("test2", list)
fmt.Println("Done, errors: ", insertErr)

```
