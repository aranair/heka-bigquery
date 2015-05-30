package bq

import (
	"bufio"
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"time"

	"google.golang.org/api/bigquery/v2"

	"./bq"

	. "github.com/mozilla-services/heka/pipeline"
)

// Id is actually the datasetId
type BqOutputConfig struct {
	ProjectId      string `toml:"project_id"`
	DatasetId      string `toml:"dataset_id"`
	TableId        string `toml:"table_id"`
	PemFilePath    string `toml:"pem_file_path"`
	SchemaFilePath string `toml:"schema_file_path"`
	BufferPath     string `toml:"buffer_path"`
	BufferFile     string `toml:"buffer_file"`
	TickerInterval uint   `toml:"ticker_interval"`
}

type BqOutput struct {
	schema []byte
	config *BqOutputConfig
	bu     *bqUploader
}

func (bqo *BqOutput) ConfigStruct() interface{} {
	return &BqOutputConfig{}
}

func (bqo *BqOutput) Init(config interface{}) (err error) {
	bqo.config = config.(*BqOutputConfig)

	pkey, _ := ioutil.ReadFile(bqo.config.PemFilePath)
	schema, _ := ioutil.ReadFile(bqo.config.SchemaPath)

	bu := bq.NewBqUploader(pkey, bqo.config.ProjectId, bqo.config.DatasetId)

	bqo.schema = schema
	bqo.bu = bu
	return
}

func exists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

func mkDirectories(path string) {
	ok, err := exists(path)
	if ok, err := exists(path); !ok {
		_ := os.MkdirAll(path, 0666)
	}
}

func (bqo *BqOutput) tableName(d time.Time) string {
	return bqo.config.TableId + formatDate(d)
}

func (bqo *BqOutput) Run(or OutputRunner, h PluginHelper) (err error) {
	var (
		pack      *PipelinePack
		payload   []byte
		ok        = true
		f         *os.File
		savedDate time.Time
	)

	inChan := or.InChan()
	tickerChan := or.Ticker()
	buf := bytes.NewBuffer(nil)
	fileOp := os.O_CREATE | os.O_APPEND | os.O_WRONLY

	MkDirectories(bqo.config.BufferPath)
	fp := bqo.config.BufferPath + "/" + bqo.config.BufferFile // form full path
	f, _ = os.OpenFile(fp, fileOp, 0666)

	oldDay = time.Now().Local()

	for ok {
		// Time Check
		if now := time.Now().Local(); isNewDay(oldDay, now) {
			f.Close() // Close file for uploading
			bqo.UploadAndReset(buf, fp, oldDay, or)
			f, _ = os.OpenFile(fp, fileOp, 0666)

			if err = bqo.bu.CreateTable(bqo.tableName(now), bqo.schema); err != nil {
				logError(or, "Create Table", err)
			}
			oldDay = now
		}

		// Channel Listeners
		select {
		case pack, ok = <-inChan:
			if !ok {
				break
			}
			err = nil
			payload = []byte(pack.Message.GetPayload())
			if _, err = f.Write(payload); err != nil {
				logError(or, "Write to File", err)
			}

			if _, err = buf.Write(payload); err != nil {
				logError(or, "Write to Buffer", err)
			}
			pack.Recycle()

		case <-tickerChan:
			logUpdate("Ticker fired, uploading.")

			f.Close() // close file for uploading
			bqo.UploadAndReset(buf, fp, oldDay, or)
			f, _ = os.OpenFile(fp, fileOp, 0666)

			logUpdate("Upload successful")
		}
	}
	logUpdate("Shutting down BQ output runner.")
	return
}

// Prepares data and uploads them to the BigQuery Table.
func (bqo *BqOutput) Upload(i interface{}, tableName string) (err error) {
	var data []byte
	list := make([]map[string]bigquery.JsonValue, 0)

	data, _ = readData(i)
	for len(data) > 0 {
		data, _ = readData(i)
		list = append(list, bq.BytesToBqJsonRow(data))
	}
	return bqo.bu.InsertRows(tableName, list)
}

func readData(i interface{}) {
	switch v := i.(type) {
	default:
		return v.ReadBytes('\n')
	}
}

func (bqo *BqOutput) UploadAndReset(buf *bytes.Buffer, path string, d time.Time, or OutputRunner) {
	tn := bqo.tableName(d)
	if err = bqo.Upload(buf, tn); err != nil {
		logError(or, "Upload Buffer", err)
		if err = bqo.UploadFile(path, tn); err != nil {
			logError(or, "Upload File", err)
		}
	}
	// Cleanup and Reset
	buf.Reset()
	_ = os.Remove(path)
}

func (bqo *BqOutput) UploadFile(path string, tableName string) (err error) {
	f, _ := os.Open(path)
	fr := bufio.NewReader(f)
	err := bqo.Upload(fr, tableName)
	f.Close()
	return
}

func formatDate(t time.Time) string {
	return fmt.Sprintf("%d%d%d", t.Year(), t.Month(), t.Day())
}

func logUpdate(or OutputRunner, title string) {
	or.LogMessage(title)
}

func logError(or OutputRunner, title string, err error) {
	or.LogMessage(fmt.Sprintf("%s - Error -: %s", title, err))
}

func isNewDay(t1 time.Time, t2 time.Time) bool {
	return (t1.Year() != t2.Year() || t1.Month() != t2.Month() || t1.Day() != t2.Day())
}

func init() {
	RegisterPlugin("BqOutput", func() interface{} {
		return new(BqOutput)
	})
}
