package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"

	"google.golang.org/api/bigquery/v2"

	"./bq"
)

func main() {
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
		list = append(list, byteToBqRow(data))
	}
	fmt.Println("Done")

	insertErr := uploader.InsertRows("test2", list)
	fmt.Println(insertErr)
}

func byteToBqRow(data []byte) map[string]bigquery.JsonValue {
	rowData := make(map[string]bigquery.JsonValue)
	json.Unmarshal(data, &rowData)
	return rowData
}
