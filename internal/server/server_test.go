package server

import (
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func get(t *testing.T, base, method, path string) (int, string, string) {
	t.Helper()
	req, err := http.NewRequest(method, base+path, nil)
	if err != nil {
		t.Fatal(err)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = res.Body.Close() }()
	body, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatal(err)
	}
	return res.StatusCode, res.Header.Get("Content-Type"), string(body)
}

func TestHealthz(t *testing.T) {
	ts := httptest.NewServer(New(Options{Version: "1.2.3"}))
	defer ts.Close()
	for _, path := range []string{"/healthz", "/healthz/", "//healthz", "/healthz?x=1", "/%68ealthz"} {
		status, contentType, body := get(t, ts.URL, http.MethodGet, path)
		if status != 200 || contentType != "application/json" || body != `{"ok":true,"version":"1.2.3","agents":0}` {
			t.Errorf("GET %s = %d %q %s", path, status, contentType, body)
		}
	}
}

func TestEverythingElseIsNotFound(t *testing.T) {
	ts := httptest.NewServer(New(Options{Version: "1.2.3"}))
	defer ts.Close()
	cases := []struct{ method, path string }{
		{http.MethodPost, "/healthz"},
		{http.MethodHead, "/healthz"},
		{http.MethodDelete, "/healthz"},
		{http.MethodGet, "/"},
		{http.MethodGet, "/agents"},
		{http.MethodPost, "/link/register"},
		{http.MethodGet, "/healthz/x"},
	}
	for _, c := range cases {
		status, contentType, body := get(t, ts.URL, c.method, c.path)
		if status != 404 || contentType != "application/json" {
			t.Errorf("%s %s = %d %q %s", c.method, c.path, status, contentType, body)
		}
		if c.method != http.MethodHead && body != `{"error":"not found"}` {
			t.Errorf("%s %s body = %s", c.method, c.path, body)
		}
	}
}

func rawRequest(t *testing.T, addr, line string) string {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	if _, err := io.WriteString(conn, line+"\r\nHost: hub\r\nConnection: close\r\n\r\n"); err != nil {
		t.Fatal(err)
	}
	out, err := io.ReadAll(conn)
	if err != nil {
		t.Fatal(err)
	}
	return string(out)
}

func TestMalformedPercentEscapeIsBadRequest(t *testing.T) {
	ts := httptest.NewServer(New(Options{Version: "1.2.3"}))
	defer ts.Close()
	for _, path := range []string{"/agents/%ff", "/link/%e0%a4/events"} {
		out := rawRequest(t, ts.Listener.Addr().String(), "GET "+path+" HTTP/1.1")
		if !strings.HasPrefix(out, "HTTP/1.1 400 ") || !strings.Contains(out, "Content-Type: application/json\r\n") ||
			!strings.HasSuffix(out, `{"error":"malformed percent escape in path"}`) {
			t.Errorf("GET %s:\n%s", path, out)
		}
	}
}

// net/http parses the request target with url.ParseRequestURI before any handler runs, so a non-hex escape
// gets its own 400 with a text/plain body, not the JSON error body of the TypeScript hub.
func TestEscapeThatIsNotHexIsBadRequest(t *testing.T) {
	ts := httptest.NewServer(New(Options{Version: "1.2.3"}))
	defer ts.Close()
	for _, path := range []string{"/agents/%zz", "/a%"} {
		out := rawRequest(t, ts.Listener.Addr().String(), "GET "+path+" HTTP/1.1")
		if !strings.HasPrefix(out, "HTTP/1.1 400 ") {
			t.Errorf("GET %s:\n%s", path, out)
		}
	}
}

func TestPanicGivesInternalError(t *testing.T) {
	var logged []string
	s := New(Options{Log: func(line string) { logged = append(logged, line) }})
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	panicky := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.ServeHTTP(&panicWriter{ResponseWriter: w}, r)
	})
	panicky.ServeHTTP(rec, req)
	if rec.Code != 500 || rec.Body.String() != `{"error":"internal error"}` || len(logged) != 1 {
		t.Errorf("got %d %s, logged %q", rec.Code, rec.Body.String(), logged)
	}
}

// panicWriter panics on the first WriteHeader, as a failing handler would before it answers.
type panicWriter struct {
	http.ResponseWriter
	panicked bool
}

func (w *panicWriter) WriteHeader(status int) {
	if !w.panicked {
		w.panicked = true
		panic("boom")
	}
	w.ResponseWriter.WriteHeader(status)
}
