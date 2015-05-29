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
	FilePath       string `toml:"file_path"`
	BqFilePath     string `toml:"bq_file_path"`
	TickerInterval uint   `toml:"ticker_interval"`
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

			if err = bqo.UploadBuffer(buf); err != nil {
				logError(or, "Upload Buffer", err)
				if errFile := bqo.UploadFile(fw); errFile != nil {
					logError(or, "Upload File", errFile)
				}
			}
			cleanUp(f, buf)
			logUpdate("Upload successful")
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
	currentDate := t.Local().Format("2006-01-02 15:00:00 +0800")[0:10]

	datedtable := bqo.config.TableId + currentDate
	// TODO check if creation date is current date?
	_ = bqo.bu.CreateTable(datedtable, schema)

	list := make([]map[string]bigquery.JsonValue, 0)
	data, _ = readData(i)
	for len(data) > 0 {
		data, _ = readData(i)
		list = append(list, bq.BytesToBqJsonRow(data))
	}

	return uploader.InsertRows(datedtable, list)
}

// Wrapper for Upload from File
func (bqo *BqOutput) UploadFile(fw *bufio.Reader) (err error) {
	fw.Flush() // Forces buffered writes into file
	return bqo.Upload(fw)
}

// Wrapper for Upload from Buffer
func (bqo *BqOutput) UploadBuffer(buf *bytes.Buffer) (err error) {
	return bqo.Upload(buf)
}

func init() {
	RegisterPlugin("BqOutput", func() interface{} {
		return new(BqOutput)
	})
}
