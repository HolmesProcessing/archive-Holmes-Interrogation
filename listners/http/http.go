package http

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/cynexit/Holmes-Interrogation/context"
	"github.com/cynexit/Holmes-Interrogation/router"
)

var (
	ctx *context.Ctx
)

func Start(c *context.Ctx, httpBinding string) {
	ctx = c

	http.HandleFunc("/", httpGenericRequestHandler)
	http.ListenAndServe(httpBinding, nil)
}

func httpGenericRequestHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Headers", "origin, content-type, accept")
	w.Header().Set("Content-Type", "application/json")

	if r.Method == "OPTIONS" {
		return
	}

	decoder := json.NewDecoder(r.Body)
	var cReq context.Request
	err := decoder.Decode(&cReq)
	if err != nil {
		err500(w, r, err)
		return
	}

	ctx.Debug.Printf("Request: %+v\n", cReq)

	j, err := json.Marshal(router.Route(ctx, &cReq))
	if err != nil {
		err500(w, r, err)
		return
	}

	w.Write(j)
}

func err500(w http.ResponseWriter, r *http.Request, err interface{}) {
	ctx.Warning.Println(err)
	http.Error(w, fmt.Sprintf("Server error occured! - %v", err), 500)
}
