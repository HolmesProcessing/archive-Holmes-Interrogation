package objects

import (
	"encoding/json"
	"io/ioutil"
	"strings"

	"git.hcr.io/cynexit/holmes-api/context"

	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/gocql/gocql"
)

type Object struct {
	SHA256      string   `json:"sha256"`
	SHA1        string   `json:"sha1"`
	MD5         string   `json:"md5"`
	MIME        string   `json:"mime"`
	Source      []string `json:"source"`
	ObjName     []string `json:"obj_name"`
	Submissions []string `json:"submissions"`
}

func GetRoutes() map[string]func(*context.Ctx, *json.RawMessage) *context.Response {
	r := make(map[string]func(*context.Ctx, *json.RawMessage) *context.Response)

	r["get"] = Get
	r["download"] = Download
	r["search"] = Search

	return r
}

type GetParameters struct {
	SHA256 string `json:"sha256"`
}

func Get(c *context.Ctx, parametersRaw *json.RawMessage) *context.Response {
	p := &GetParameters{}
	err := json.Unmarshal(*parametersRaw, p)
	if err != nil {
		return &context.Response{Error: err.Error()}
	}

	object := &Object{}

	// TODO: fix results, make everything lower case and revisit this statement
	err = c.C.Query(`SELECT * FROM objects WHERE sha256 = ?`, strings.ToLower(p.SHA256)).Scan(
		&object.SHA256,
		&object.MD5,
		&object.MIME,
		&object.ObjName,
		&object.SHA1,
		&object.Source,
		&object.Submissions,
	)

	if err != nil {
		return &context.Response{Error: err.Error()}
	}

	return &context.Response{Result: struct {
		Object *Object
	}{
		object,
	},
	}
}

type DownloadParameters struct {
	SHA256 string `json:"sha256"`
}

func Download(c *context.Ctx, parametersRaw *json.RawMessage) *context.Response {
	p := &DownloadParameters{}
	err := json.Unmarshal(*parametersRaw, p)
	if err != nil {
		return &context.Response{Error: err.Error()}
	}

	resp, err := c.S3.GetObject(&s3.GetObjectInput{
		Bucket: &c.Bucket,
		Key:    &p.SHA256,
	})

	if err != nil {
		return &context.Response{Error: err.Error()}
	}

	objBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return &context.Response{Error: err.Error()}
	}

	return &context.Response{Result: struct {
		Bytes []byte
	}{
		objBytes,
	},
	}
}

type SearchParameters struct {
	SHA256      string `json:"sha256"`
	SHA1        string `json:"sha1"`
	MD5         string `json:"md5"`
	MIME        string `json:"mime"`
	Source      string `json:"source"`
	ObjName     string `json:"obj_name"`
	Submissions string `json:"submissions"`
}

func Search(c *context.Ctx, parametersRaw *json.RawMessage) *context.Response {
	p := &SearchParameters{}
	err := json.Unmarshal(*parametersRaw, p)
	if err != nil {
		return &context.Response{Error: err.Error()}
	}

	// since there is only an index on the SHA256
	// we need to cycle trough everything to filter
	// for other fields...
	objects := []*Object{}
	object := &Object{}

	var q *gocql.Query
	if p.SHA256 != "" {
		q = c.C.Query(`SELECT * FROM objects WHERE sha256 = ?`, p.SHA256)
	} else {
		q = c.C.Query(`SELECT * FROM objects`)
	}

	iter := q.Iter()
	for iter.Scan(
		&object.SHA256,
		&object.MD5,
		&object.MIME,
		&object.ObjName,
		&object.SHA1,
		&object.Source,
		&object.Submissions,
	) {
		if p.SHA256 != "" && object.SHA256 != p.SHA256 {
			object = &Object{}
			continue
		}

		if p.MD5 != "" && object.MD5 != p.MD5 {
			object = &Object{}
			continue
		}

		if p.MIME != "" && object.MIME != p.MIME {
			object = &Object{}
			continue
		}

		if p.SHA1 != "" && object.SHA1 != p.SHA1 {
			object = &Object{}
			continue
		}

		if p.ObjName != "" && !stringInSlice(p.ObjName, object.ObjName) {
			object = &Object{}
			continue
		}

		if p.Source != "" && !stringInSlice(p.Source, object.Source) {
			object = &Object{}
			continue
		}

		if p.Submissions != "" && !stringInSlice(p.Submissions, object.Submissions) {
			object = &Object{}
			continue
		}

		objects = append(objects, object)
		object = &Object{}
	}

	if err = iter.Close(); err != nil {
		return &context.Response{Error: err.Error()}
	}

	return &context.Response{Result: struct {
		Objects []*Object
	}{
		objects,
	},
	}
}

func stringInSlice(a string, list []string) bool {
	for _, b := range list {
		if b == a {
			return true
		}
	}
	return false
}
