package context

import (
	"encoding/json"
	"io"
	"io/ioutil"
	"log"
	"os"

	"github.com/gocql/gocql"

	"github.com/aws/aws-sdk-go/service/s3"
)

type Ctx struct {
	C *gocql.Session // pointer to Cassandra

	S3     *s3.S3 // pointer to S3
	Bucket string // name of S3 bucket

	Debug   *log.Logger
	Info    *log.Logger
	Warning *log.Logger
}

func (c *Ctx) SetLogging(file, level string) {
	// default: only log to stdout
	handler := io.MultiWriter(os.Stdout)

	if file != "" {
		// log to file
		if _, err := os.Stat(file); os.IsNotExist(err) {
			err := ioutil.WriteFile(file, []byte(""), 0600)
			if err != nil {
				panic("Couldn't create the log!")
			}
		}

		f, err := os.OpenFile(file, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
		if err != nil {
			panic("Failed to open log file!")
		}

		handler = io.MultiWriter(f, os.Stdout)
	}

	// TODO: make this nicer....
	empty := io.MultiWriter()
	if level == "warning" {
		c.Warning = log.New(handler, "WARNING: ", log.Ldate|log.Ltime|log.Lshortfile)
		c.Info = log.New(empty, "INFO: ", log.Ldate|log.Ltime)
		c.Debug = log.New(empty, "DEBUG: ", log.Ldate|log.Ltime|log.Lshortfile)
	} else if level == "info" {
		c.Warning = log.New(handler, "WARNING: ", log.Ldate|log.Ltime|log.Lshortfile)
		c.Info = log.New(handler, "INFO: ", log.Ldate|log.Ltime)
		c.Debug = log.New(empty, "DEBUG: ", log.Ldate|log.Ltime|log.Lshortfile)
	} else {
		c.Warning = log.New(handler, "WARNING: ", log.Ldate|log.Ltime|log.Lshortfile)
		c.Info = log.New(handler, "INFO: ", log.Ldate|log.Ltime)
		c.Debug = log.New(handler, "DEBUG: ", log.Ldate|log.Ltime|log.Lshortfile)
	}
}

type Request struct {
	Apikey     string           `json:"apikey"`
	Module     string           `json:"module"`
	Action     string           `json:"action"`
	Parameters *json.RawMessage `json:"parameters"`
}

type Response struct {
	Error  string      `json:"error"`
	Result interface{} `json:"result"`
}

func ErrorResponse(err string) *Response {
	return &Response{Error: err}
}
