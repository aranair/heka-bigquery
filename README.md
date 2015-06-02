# heka-bigquery
Heka Output Plugin to BigQuery

```
pkey, _ := ioutil.ReadFile("big_query.pem")
schema, _ := ioutil.ReadFile("test_schema.json")

uploader := bq.NewBqUploader(pkey, "wego-cloud", "analytics_golang")
err := uploader.CreateTable("test2", schema)
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
fmt.Println("Done")

insertErr := uploader.InsertRows("test2", list)
fmt.Println(insertErr)

```
