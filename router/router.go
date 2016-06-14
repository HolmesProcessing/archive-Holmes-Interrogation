package router

import (
	"encoding/json"

	"git.hcr.io/cynexit/holmes-api/context"
	"git.hcr.io/cynexit/holmes-api/modules/objects"
	"git.hcr.io/cynexit/holmes-api/modules/results"
	"git.hcr.io/cynexit/holmes-api/modules/submissions"
)

var (
	routes = make(map[string]map[string]func(*context.Ctx, *json.RawMessage) *context.Response)
)

func init() {
	routes["objects"] = objects.GetRoutes()
	routes["submissions"] = submissions.GetRoutes()
	routes["results"] = results.GetRoutes()
}

func Route(c *context.Ctx, req *context.Request) *context.Response {
	if req.Parameters == nil {
		return &context.Response{
			Error: "Please supply parameters!",
		}
	}

	if _, routeExists := routes[req.Module][req.Action]; !routeExists {
		return &context.Response{
			Error: "Module / Action invalid!",
		}
	}

	return routes[req.Module][req.Action](c, req.Parameters)
}
