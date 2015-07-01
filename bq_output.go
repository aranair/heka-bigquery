// Copyright 2015 Boa Ho Man. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

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

// Interval to tick
const IntervalPeriod time.Duration = 24 * time.Hour

// Hour for 1st tick
const TickHour int = 00

// Minute for 1st tick
const TickMinute int = 00

// Second for 1st tick
const TickSecond int = 00

// Max buffer size before it attempts an upload in bytes, currently 1000 for testing.
const MaxBuffer = 1000

// A BqOutputConfig holds the information needed to configure the heka plugin.
// Service Email: Service account email found in Google Developers console
// Pem File: PKCS12 file that is generated from the .p12 file
// Schema file: BigQuery schema json file. See example schema file `realtime_log.schema.sample`
// BufferPath + BufferFile: Full path to the 'backup' file that is written to at the same time as the buffer
type BqOutputConfig struct {
	ProjectId      string `toml:"project_id"`
	DatasetId      string `toml:"dataset_id"`
	TableId        string `toml:"table_id"`
	ServiceEmail   string `toml:"service_email"`
	PemFilePath    string `toml:"pem_file_path"`
	SchemaFilePath string `toml:"schema_file_path"`
	BufferPath     string `toml:"buffer_path"`
	BufferFile     string `toml:"buffer_file"`
}

// A BqOutput holds the uploader/schema.
type BqOutput struct {
	schema []byte
	config *BqOutputConfig
	bu     *bq.BqUploader
}

func (bqo *BqOutput) ConfigStruct() interface{} {
	return &BqOutputConfig{}
}

// Init function that gets run by Heka when the plugin gets loaded
// Reads PEM files/schema files and initializes the BqUploader objects
func (bqo *BqOutput) Init(config interface{}) (err error) {
	bqo.config = config.(*BqOutputConfig)

	pkey, _ := ioutil.ReadFile(bqo.config.PemFilePath)
	schema, _ := ioutil.ReadFile(bqo.config.SchemaFilePath)

	bu := bq.NewBqUploader(pkey, bqo.config.ProjectId, bqo.config.DatasetId, bqo.config.ServiceEmail)

	bqo.schema = schema
	bqo.bu = bu
	return
}

// Gets called by Heka when the plugin is running.
// For more information, visit https://hekad.readthedocs.org/en/latest/developing/plugin.html
func (bqo *BqOutput) Run(or OutputRunner, h PluginHelper) (err error) {
	var (
		// Heka messages
		pack    *PipelinePack
		payload []byte

		// File used for the backup buffer
		f *os.File

		// The "current" time that is used to upload. Used to keep track of old/new day when midnight ticker ticks.
		oldDay time.Time

		// When the midnight ticker ticks, this is used to format the new bigquery table name.
		now time.Time
		ok  = true
	)

	// Channel that delivers the heka payloads
	inChan := or.InChan()
	midnightTicker := midnightTickerUpdate()

	// Buffer that is used to store logs before uploading to bigquery
	buf := bytes.NewBuffer(nil)
	fileOp := os.O_CREATE | os.O_APPEND | os.O_WRONLY

	// Ensures that the directories are there before saving
	mkDirectories(bqo.config.BufferPath)

	fp := bqo.config.BufferPath + "/" + bqo.config.BufferFile // form full path
	f, _ = os.OpenFile(fp, fileOp, 0666)

	oldDay = time.Now().Local()

	// Initializes the current day table
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
			if buf.Len() > MaxBuffer {
				f.Close() // Close file for uploading
				bqo.UploadAndReset(buf, fp, oldDay, or)
				f, _ = os.OpenFile(fp, fileOp, 0666)
			}
		case <-midnightTicker.C:
			now = time.Now().Local()

			// If Buffer is not empty, upload the rest of the contents to the oldday's table.
			if buf.Len() > 0 {
				f.Close() // Close file for uploading
				bqo.UploadAndReset(buf, fp, oldDay, or)
				f, _ = os.OpenFile(fp, fileOp, 0666)
			}
			logUpdate(or, "Midnight! Creating new table: "+bqo.tableName(now))

			// Create a new table for the new day and update current date.
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
// Shared by both file/buffer uploads
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

// Uploads buffer, and if it fails/contains errors, falls back to using the file to upload.
// After which clears the buffer and deletes the backup file
func (bqo *BqOutput) UploadAndReset(buf *bytes.Buffer, path string, d time.Time, or OutputRunner) {
	tn := bqo.tableName(d)
	logUpdate(or, "Buffer limit reached, uploading"+tn)

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

// Uploads file at `path` to BigQuery table
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
		time.Now().Day(), TickHour, TickMinute, TickSecond,
		0, time.Local)
	if !nextTick.After(time.Now()) {
		nextTick = nextTick.Add(IntervalPeriod)
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
