package bq

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"golang.org/x/oauth2/jwt"
	bigquery "google.golang.org/api/bigquery/v2"
)

type BqUploader struct {
	bq        *bigquery.Service
	projectId string
	datasetId string
}

func NewBqUploader(pkey []byte, projectId string, datasetId string) *BqUploader {
	conf := &jwt.Config{
		Email:      "1022153460529-lism2u2cvjuruicuue51krv7sbr2cted@developer.gserviceaccount.com",
		PrivateKey: pkey,
		Scopes: []string{
			bigquery.BigqueryScope,
		},
		TokenURL: google.JWTTokenURL,
	}
	// Initiate an http.Client, the following GET request will be
	client := conf.Client(oauth2.NoContext)

	// "wego-cloud", "analytics-golang"
	bq, errBq := bigquery.New(client)
	if errBq != nil {
		log.Fatalf("Unable to create BigQuery service: %v", errBq)
	}
	return &BqUploader{
		bq:        bq,
		projectId: projectId,
		datasetId: datasetId,
	}
}

func (bu *BqUploader) CreateTable(tableId string, schema []byte) error {
	bq := bu.bq
	_, err := bq.Tables.Get(bu.projectId, bu.datasetId, tableId).Do()

	// if error -> table doesn't exist -> create
	if err != nil {
		var tfs bigquery.TableSchema
		json.Unmarshal(schema, &tfs)

		// Create a new table.
		_, err := bq.Tables.Insert(bu.projectId, bu.datasetId, &bigquery.Table{
			Schema: &tfs,
			TableReference: &bigquery.TableReference{
				DatasetId: bu.datasetId,
				ProjectId: bu.projectId,
				TableId:   tableId,
			},
		}).Do()
		if err != nil {
			return err
		}
		fmt.Println("Done creating table.")
	}
	return nil
}

func (bu *BqUploader) InsertRows(tableId string, list []map[string]bigquery.JsonValue) error {
	rows := make([]*bigquery.TableDataInsertAllRequestRows, 0)
	for i, row := range list {
		fmt.Println(i)
		rows = append(rows, &bigquery.TableDataInsertAllRequestRows{
			Json: row,
		})
	}
	err := bu.SendInsert(tableId, rows)
	return err
}

func (bu *BqUploader) SendInsert(tableId string, rows []*bigquery.TableDataInsertAllRequestRows) error {
	req := &bigquery.TableDataInsertAllRequest{
		Rows: rows,
	}
	result, err := bu.bq.Tabledata.InsertAll(bu.projectId, bu.datasetId, tableId, req).Do()
	if err != nil {
		return err
	} else {
		if len(result.InsertErrors) > 0 {
			je, _ := json.Marshal(result.InsertErrors)
			return errors.New(string(je))
		}
	}
	return nil
}

// InsertRow inserts a new row into the desired project, dataset and table or returns an error
func (bu *BqUploader) InsertRow(tableId string, rowData map[string]bigquery.JsonValue) error {
	rows := []*bigquery.TableDataInsertAllRequestRows{
		{
			Json: rowData,
		},
	}
	err := bu.SendInsert(tableId, rows)
	return err
}

func BytesToBqJsonRow(data []byte) map[string]bigquery.JsonValue {
	rowData := make(map[string]bigquery.JsonValue)
	json.Unmarshal(data, &rowData)
	return rowData
}
