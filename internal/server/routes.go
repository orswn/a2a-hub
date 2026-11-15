package server

import "strings"

// routes are the keys of the route manifest of the hub protocol.
var routes = []string{
	"GET /healthz",
	"GET /agents",
	"GET /agents/:id",
	"POST /link/register",
	"GET /link/:agent/events",
	"DELETE /link/:agent",
	"POST /link/:agent/ack",
	"POST /link/:agent/state",
	"POST /link/:agent/status",
	"POST /link/:agent/tasks/:taskId",
	"POST /link/:agent/topics",
	"POST /link/:agent/control",
	"GET /link/:agent/inbox",
	"POST /link/:agent/read",
	"GET /link/:agent/message/:idOrPrefix",
	"GET /link/:agent/pending",
	"GET /link/:agent/owed",
	"GET /link/:agent/thread/:contextOrPrefix",
	"POST /link/:agent/children",
}

// matchRoute gives the first route key that matches, or "".
func matchRoute(method string, segments []string) string {
	for _, route := range routes {
		routeMethod, path, _ := strings.Cut(route, " ")
		parts := strings.Split(strings.Trim(path, "/"), "/")
		if routeMethod != method || len(parts) != len(segments) {
			continue
		}
		match := true
		for i, part := range parts {
			if !strings.HasPrefix(part, ":") && part != segments[i] {
				match = false
				break
			}
		}
		if match {
			return route
		}
	}
	return ""
}
