package submissions

import (
	"encoding/json"
	"time"

	"git.hcr.io/cynexit/holmes-api/context"

	"github.com/gocql/gocql"
)

type Submission struct {
	Id      string    `json:"id"`
	SHA256  string    `json:"sha256"`
	UserId  string    `json:"user_id"`
	Source  string    `json:"source"`
	Date    time.Time `json:"date"`
	ObjName string    `json:"obj_name"`
	Tags    []string  `json:"tags"`
	Comment string    `json:"comment"`
}

func GetRoutes() map[string]func(*context.Ctx, *json.RawMessage) *context.Response {
	r := make(map[string]func(*context.Ctx, *json.RawMessage) *context.Response)

	r["get"] = Get

	return r
}

type GetParameters struct {
	Id string `json:"id"`
}

func Get(c *context.Ctx, parametersRaw *json.RawMessage) *context.Response {
	p := &GetParameters{}
	err := json.Unmarshal(*parametersRaw, p)
	if err != nil {
		return &context.Response{Error: err.Error()}
	}

	submission := &Submission{}

	uuid, err := gocql.ParseUUID(p.Id)
	if err != nil {
		return &context.Response{Error: err.Error()}
	}

	err = c.C.Query(`SELECT * FROM submissions WHERE id = ? LIMIT 1`, uuid).Scan(
		&submission.Id,
		&submission.Comment,
		&submission.Date,
		&submission.ObjName,
		&submission.SHA256,
		&submission.Source,
		&submission.Tags,
		&submission.UserId,
	)

	if err != nil {
		return &context.Response{Error: err.Error()}
	}

	return &context.Response{Result: struct {
		Submission *Submission
	}{
		submission,
	},
	}
}
