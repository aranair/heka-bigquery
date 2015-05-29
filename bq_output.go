package bq

import (
	"bufio"
	"bytes"
	"fmt"
	"io/ioutil"
	"os"

	"google.golang.org/api/bigquery/v2"

	"./bq"

	. "github.com/mozilla-services/heka/pipeline"
)

// Id is actually the datasetId
type BqOutputConfig struct {
	ProjectId      string `toml:"project_id"`
	DatasetId      string `toml:"dataset_id"`
	TableId        string `toml:"table_id"`
	TickerInterval uint   `toml:"ticker_interval"`
	FilePath       string `toml:"file_path"`
	BqFilePath     string `toml:"bq_file_path"`
}

type BqOutput struct {
	config *BqOutputConfig
	bu     *bqUploader
}

func (bqo *BqOutput) ConfigStruct() interface{} {
	return &BqOutputConfig{}
}

func (bqo *BqOutput) Init(config interface{}) (err error) {
	bqo.config = config.(*BqOutputConfig)

	pkey, _ := ioutil.ReadFile("big_query.pem")
	schema, _ := ioutil.ReadFile("test_schema.json")

	// TODO: change to pkey, projectId, datasetId in production
	bu := bq.NewBqUploader(pkey, "wego-cloud", "analytics_golang")

	bqo.bu = bu
	return
}

func (bqo *BqOutput) Run(or OutputRunner, h PluginHelper) (err error) {
	inChan := or.InChan()
	tickerChan := or.Ticker()
	buf := bytes.NewBuffer(nil)
	f, _ := os.OpenFile(bqo.config.FilePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0666)
	fw := bufio.NewWriter(f)

	var (
		pack *PipelinePack
		pl   []byte
		ok   = true
	)

	for ok {
		// TODO: if midnight -> force upload
		select {
		case pack, ok = <-inChan:
			if !ok {
				break
			}
			err = nil
			pl = []byte(pack.Message.GetPayload())
			if _, err = fw.Write(pl); err != nil {
				logError(or, "Write to File", err)
			}

			if _, err = buf.Write(pl); err != nil {
				logError(or, "Write to Buffer", err)
			}
			pack.Recycle()
		case <-tickerChan:
			logUpdate("Ticker fired, uploading.")
			err := bqo.UploadBuffer(buf)
			if err != nil {
				logError(or, "Upload Buffer", err)
				fw.Flush() // Forces buffered writes into file

				if fileUploadErr := bqo.UploadFile(fw); fileUploadErr != nil {
					logError(or, "Upload File", fileUploadErr)
				}
			}

			cleanUp(f, buf)
			logUpdate("Uploading successful")
		}
	}
	logUpdate("Shutting down BQ output runner.")
	return
}

func logUpdate(or OutputRunner, title string) {
	or.LogMessage(title)
}

func logError(or OutputRunner, title string, err error) {
	or.LogMessage(fmt.Sprintf("%s - Error -: %s", title, err))
}

func cleanUp(f *os.File, b *bytes.buffer) {
	f.Close()
	f.Truncate(0)
	f.Open()
	buf.reset()
}

func readData(i interface{}) {
	switch v := i.(type) {
	default:
		return v.ReadBytes('\n')
	}
}

func (bqo *BqOutput) Upload(i interface{}) (err error) {
	var data []byte
	tableId := bqo.config.TableId
	_ = bqo.bu.CreateTable(tableId, schema)

	list := make([]map[string]bigquery.JsonValue, 0)
	data, _ = readData(i)
	for len(data) > 0 {
		data, _ = readData(i)
		list = append(list, bq.BytesToBqJsonRow(data))
	}

	return uploader.InsertRows(tableId, list)
}

func (bqo *BqOutput) UploadFile(fw *bufio.Reader) (err error) {
	return bqo.Upload(fw)
}

func (bqo *BqOutput) UploadBuffer(buf *bytes.Buffer) (err error) {
	return bqo.Upload(buf)
}

func init() {
	RegisterPlugin("BqOutput", func() interface{} {
		return new(BqOutput)
	})
}
