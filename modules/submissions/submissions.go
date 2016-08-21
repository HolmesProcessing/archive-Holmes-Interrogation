package submissions

import (
	"encoding/json"
	"strconv"
	"strings"
	"time"

	"github.com/cynexit/Holmes-Interrogation/context"

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
	r["search"] = Search

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

type SearchParameters struct {
	SHA256    string `json:"sha256"`
	ObjName   string `json:"obj_name"`
	Source    string `json:"source"`
	DateStart string `json:"date"`
	DateStop  string `json:"date"`
	Limit     string `json:"limit"`
	Filtering string `json:"filtering"`
}

func Search(c *context.Ctx, parametersRaw *json.RawMessage) *context.Response {
	p := &SearchParameters{}
	err := json.Unmarshal(*parametersRaw, p)
	if err != nil {
		return &context.Response{Error: err.Error()}
	}

	submissions := []*Submission{}
	submission := &Submission{}

	var whereStmt []string
	var whereStmtValues []interface{}

	if p.SHA256 != "" {
		whereStmt = append(whereStmt, "sha256 = ?")
		whereStmtValues = append(whereStmtValues, p.SHA256)
	}

	if p.ObjName != "" {
		whereStmt = append(whereStmt, "obj_name = ?")
		whereStmtValues = append(whereStmtValues, p.ObjName)
	}

	if p.Source != "" {
		whereStmt = append(whereStmt, "source = ?")
		whereStmtValues = append(whereStmtValues, p.Source)
	}

	if p.DateStart != "" {
		if t, err := time.Parse("2006-01-02 15:04:05", p.DateStart); err == nil {
			whereStmt = append(whereStmt, "date > ?")
			whereStmtValues = append(whereStmtValues, t)
		} else {
			return &context.Response{Error: err.Error()}
		}
	}

	if p.DateStop != "" {
		if t, err := time.Parse("2006-01-02 15:04:05", p.DateStop); err == nil {
			whereStmt = append(whereStmt, "date < ?")
			whereStmtValues = append(whereStmtValues, t)
		} else {
			return &context.Response{Error: err.Error()}
		}
	}

	limit, err := strconv.Atoi(p.Limit)
	if limit == 0 || err != nil {
		limit = 100
	}

	where := ""
	if len(whereStmt) > 0 {
		where = " WHERE " + strings.Join(whereStmt, " AND ")
	}

	where += " LIMIT " + strconv.Itoa(limit)

	if p.Filtering == "on" {
		where += " ALLOW FILTERING"
	}

	q := c.C.Query("SELECT id, sha256, obj_name, source, date FROM submissions"+where, whereStmtValues...)

	iter := q.Iter()
	for iter.Scan(
		&submission.Id,
		&submission.SHA256,
		&submission.ObjName,
		&submission.Source,
		&submission.Date,
	) {
		submissions = append(submissions, submission)
		submission = &Submission{}
	}

	if err = iter.Close(); err != nil {
		return &context.Response{Error: err.Error()}
	}

	return &context.Response{Result: struct {
		Submissions []*Submission
	}{
		submissions,
	},
	}
}
