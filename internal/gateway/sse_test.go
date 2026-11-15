package gateway

import (
	"reflect"
	"strings"
	"testing"
)

func pushAll(p *Parser, chunks ...string) []Frame {
	var frames []Frame
	for _, chunk := range chunks {
		frames = append(frames, p.Push([]byte(chunk))...)
	}
	return frames
}

func TestParserJoinsAFrameThatComesInSeveralReads(t *testing.T) {
	var p Parser
	frames := pushAll(&p, "da", "ta: {\"a\"", ":1}\n", "\n")
	if want := []Frame{{Data: `{"a":1}`}}; !reflect.DeepEqual(frames, want) {
		t.Errorf("got %+v", frames)
	}
	if p.Buffered() != 0 {
		t.Errorf("%d bytes left", p.Buffered())
	}
}

func TestParserTakesCRLFAndMixedLineEnds(t *testing.T) {
	var p Parser
	frames := pushAll(&p, "data: one\r\n\r\ndata: two\n\r\ndata: three\r\n\ndata: x\r", "\n\r\n")
	var got []string
	for _, frame := range frames {
		got = append(got, frame.Data)
	}
	if want := []string{"one", "two", "three", "x"}; !reflect.DeepEqual(got, want) {
		t.Errorf("got %q", got)
	}
}

func TestParserDropsCommentsAndReadsEventAndIDLines(t *testing.T) {
	var p Parser
	frames := pushAll(&p, ": keepalive\n\n", "id: 7\nevent: error\n: note\ndata: {\"x\":1}\ndata:  two\ndata\n\n", ": done\n\n")
	want := []Frame{{}, {Event: "error", ID: "7", Data: "{\"x\":1}\n two"}, {}}
	if !reflect.DeepEqual(frames, want) {
		t.Errorf("got %+v", frames)
	}
}

func TestParserReadsAFrameLargerThanOneRead(t *testing.T) {
	var p Parser
	big := strings.Repeat("x", 100_000)
	var frames []Frame
	text := "data: " + big + "\n\n"
	for len(text) > 0 {
		n := min(4096, len(text))
		frames = append(frames, p.Push([]byte(text[:n]))...)
		text = text[n:]
	}
	if len(frames) != 1 || frames[0].Data != big {
		t.Fatalf("got %d frames", len(frames))
	}
}

func TestParserKeepsAFrameThatTheStreamEndsInTheMiddleOf(t *testing.T) {
	var p Parser
	frames := pushAll(&p, "data: one\n\ndata: tw", "o\n")
	if len(frames) != 1 || frames[0].Data != "one" {
		t.Errorf("got %+v", frames)
	}
	if p.Buffered() != len("data: two\n") {
		t.Errorf("buffered %d", p.Buffered())
	}
}
