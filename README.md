# heka-bigquery
Heka Output Plugin to BigQuery

### Sample Code (service account)

```
// Pem file that is converted from the .p12 file.
pkey, _ := ioutil.ReadFile("big_query.pem")
schema, _ := ioutil.ReadFile("test_schema.json")
projectId := "wego-cloud"
datasetId := "analytics_golang"
tableId   := "test"

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
