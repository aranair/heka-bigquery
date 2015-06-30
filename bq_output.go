package hbq

import (
	"bufio"
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"time"

	"google.golang.org/api/bigquery/v2"

	"github.com/aranair/heka-bigquery/bq"

	. "github.com/mozilla-services/heka/pipeline"
)

const INTERVAL_PERIOD time.Duration = 24 * time.Hour
const HOUR_TO_TICK int = 00
const MINUTE_TO_TICK int = 00
const SECOND_TO_TICK int = 00
const MAX_BUFFER_SIZE = 1000

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
	bu     *bq.BqUploader
}

func (bqo *BqOutput) ConfigStruct() interface{} {
	return &BqOutputConfig{}
}

func (bqo *BqOutput) Init(config interface{}) (err error) {
	bqo.config = config.(*BqOutputConfig)

	pkey, _ := ioutil.ReadFile(bqo.config.PemFilePath)
	schema, _ := ioutil.ReadFile(bqo.config.SchemaFilePath)

	bu := bq.NewBqUploader(pkey, bqo.config.ProjectId, bqo.config.DatasetId)

	bqo.schema = schema
	bqo.bu = bu
	return
}

func (bqo *BqOutput) Run(or OutputRunner, h PluginHelper) (err error) {
	var (
		pack    *PipelinePack
		payload []byte
		f       *os.File
		oldDay  time.Time
		now     time.Time
		ok      = true
	)

	inChan := or.InChan()
	midnightTicker := midnightTickerUpdate()

	buf := bytes.NewBuffer(nil)
	fileOp := os.O_CREATE | os.O_APPEND | os.O_WRONLY

	mkDirectories(bqo.config.BufferPath)

	fp := bqo.config.BufferPath + "/" + bqo.config.BufferFile // form full path
	f, _ = os.OpenFile(fp, fileOp, 0666)

	oldDay = time.Now().Local()

	if err = bqo.bu.CreateTable(bqo.tableName(oldDay), bqo.schema); err != nil {
		logError(or, "Initialize Table", err)
	}

	for ok {
		select {
		case pack, ok = <-inChan:
			if !ok {
				break
			}

			payload = []byte(pack.Message.GetPayload())
			pack.Recycle()

			// Write to both file and buffer
			if _, err = f.Write(payload); err != nil {
				logError(or, "Write to File", err)
			}
			if _, err = buf.Write(payload); err != nil {
				logError(or, "Write to Buffer", err)
			}

			// Upload Stuff (1mb)
			if buf.Len() > MAX_BUFFER_SIZE {
				f.Close() // Close file for uploading
				bqo.UploadAndReset(buf, fp, oldDay, or)
				f, _ = os.OpenFile(fp, fileOp, 0666)
			}
		case <-midnightTicker.C:
			// Time Check
			now = time.Now().Local()
			if buf.Len() > 0 {
				f.Close() // Close file for uploading
				bqo.UploadAndReset(buf, fp, oldDay, or)
				f, _ = os.OpenFile(fp, fileOp, 0666)
			}
			logUpdate(or, "Midnight! Creating new table: "+bqo.tableName(now))

			if err = bqo.bu.CreateTable(bqo.tableName(now), bqo.schema); err != nil {
				logError(or, "Create New Day Table", err)
			}
			oldDay = now
		}
	}

	logUpdate(or, "Shutting down BQ output runner.")
	return
}

// Prepares data and uploads them to the BigQuery Table.
func (bqo *BqOutput) Upload(i interface{}, tableName string) (err error) {
	var data []byte
	list := make([]map[string]bigquery.JsonValue, 0)

	for {
		data, _ = readData(i)
		if len(data) == 0 {
			break
		}
		list = append(list, bq.BytesToBqJsonRow(data))
	}
	return bqo.bu.InsertRows(tableName, list)
}

func readData(i interface{}) (line []byte, err error) {
	switch v := i.(type) {
	case *bytes.Buffer:
		line, err = v.ReadBytes('\n')
	case *bufio.Reader:
		line, err = v.ReadBytes('\n')
	}
	return
}

func (bqo *BqOutput) UploadAndReset(buf *bytes.Buffer, path string, d time.Time, or OutputRunner) {
	tn := bqo.tableName(d)
	logUpdate(or, "Ticker fired, uploading"+tn)

	if err := bqo.Upload(buf, tn); err != nil {
		logError(or, "Upload Buffer", err)
		if err := bqo.UploadFile(path, tn); err != nil {
			logError(or, "Upload File", err)
		} else {
			logUpdate(or, "Upload File Successful")
		}
	} else {
		logUpdate(or, "Upload Buffer Successful")
	}

	// Cleanup and Reset
	buf.Reset()
	_ = os.Remove(path)
}

func (bqo *BqOutput) UploadFile(path string, tableName string) (err error) {
	f, _ := os.Open(path)
	fr := bufio.NewReader(f)
	err = bqo.Upload(fr, tableName)
	f.Close()
	return
}

func formatDate(t time.Time) string {
	return fmt.Sprintf(t.Format("20060102"))
}

func logUpdate(or OutputRunner, title string) {
	or.LogMessage(title)
}

func logError(or OutputRunner, title string, err error) {
	or.LogMessage(fmt.Sprintf("%s - Error -: %s", title, err))
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
	if ok, _ := exists(path); !ok {
		_ = os.MkdirAll(path, 0666)
	}
}

func midnightTickerUpdate() *time.Ticker {
	nextTick := time.Date(time.Now().Year(), time.Now().Month(),
		time.Now().Day(), HOUR_TO_TICK, MINUTE_TO_TICK, SECOND_TO_TICK,
		0, time.Local)
	if !nextTick.After(time.Now()) {
		nextTick = nextTick.Add(INTERVAL_PERIOD)
	}
	diff := nextTick.Sub(time.Now())
	return time.NewTicker(diff)
}

func (bqo *BqOutput) tableName(d time.Time) string {
	return bqo.config.TableId + formatDate(d)
}

func init() {
	RegisterPlugin("BqOutput", func() interface{} {
		return new(BqOutput)
	})
}
