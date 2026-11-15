package linkclient

import (
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
)

func TestStreamReadsFramesHeartbeatsAndReplaced(t *testing.T) {
	raw := ": hb\n\nid: 4\nevent: message\ndata: {\"a\":\ndata: 1}\n\nevent: replaced\ndata: {}\n\n"
	stream := &Stream{body: io.NopCloser(strings.NewReader(raw))}
	stream.reader = newReader(stream.body)
	lines := 0
	first, err := stream.Next(func() { lines++ })
	if err != nil || first.Seq != 4 || first.Type != "message" || string(first.Payload) != "{\"a\":\n1}" {
		t.Fatalf("first = %+v, %v", first, err)
	}
	if lines != 7 {
		t.Fatalf("alive ran %d times", lines)
	}
	second, err := stream.Next(nil)
	if err != nil || second.Seq != -1 || second.Type != "replaced" {
		t.Fatalf("second = %+v, %v", second, err)
	}
	if _, err := stream.Next(nil); !errors.Is(err, io.EOF) {
		t.Fatalf("end = %v", err)
	}
}

func TestFatalStatuses(t *testing.T) {
	for status, want := range map[int]bool{400: true, 401: true, 403: true, 426: true, 404: false, 409: false, 500: false, 502: false} {
		err := fmt.Errorf("wrapped: %w", &HTTPError{Status: status, Body: `{"error":"x"}`})
		if Fatal(err) != want {
			t.Fatalf("Fatal(%d) = %v", status, !want)
		}
	}
	if Fatal(io.EOF) {
		t.Fatal("a network error is fatal")
	}
	if got := (&HTTPError{Status: 426, Body: `{"error":"upgrade the client"}`}).Error(); got != "hub answered 426: upgrade the client" {
		t.Fatalf("error text = %q", got)
	}
}
