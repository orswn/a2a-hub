package gateway

import (
	"bytes"
	"strings"
)

// Frame is one server-sent event. Comment lines are dropped.
type Frame struct {
	Event string
	ID    string
	// Data joins the data lines with "\n". One space after the colon is not part of the value.
	Data string
}

// Parser splits a byte stream into server-sent events. A frame ends at a blank line: "\n\n", "\r\n\r\n", or a
// mix of the two. Bytes after the last blank line wait for the next Push; at the end of the stream they are
// not an event.
type Parser struct {
	buf []byte
}

// Buffered is the number of bytes of an unfinished frame.
func (p *Parser) Buffered() int { return len(p.buf) }

// Push adds bytes and returns every frame that they complete.
func (p *Parser) Push(chunk []byte) []Frame {
	p.buf = append(p.buf, chunk...)
	var frames []Frame
	for {
		end, size := frameEnd(p.buf)
		if end < 0 {
			break
		}
		frames = append(frames, parseFrame(string(p.buf[:end])))
		p.buf = p.buf[end+size:]
	}
	if len(p.buf) == 0 {
		p.buf = nil
	}
	return frames
}

// frameEnd finds the first match of /\r?\n\r?\n/: its start and its length.
func frameEnd(buf []byte) (int, int) {
	for from := 0; ; {
		i := bytes.IndexByte(buf[from:], '\n')
		if i < 0 {
			return -1, 0
		}
		i += from
		start := i
		if start > 0 && buf[start-1] == '\r' {
			start--
		}
		next := i + 1
		if next < len(buf) && buf[next] == '\r' {
			next++
		}
		if next < len(buf) && buf[next] == '\n' {
			return start, next + 1 - start
		}
		from = i + 1
	}
}

func parseFrame(text string) Frame {
	var frame Frame
	var data []string
	for _, line := range strings.Split(text, "\n") {
		line = strings.TrimSuffix(line, "\r")
		field, value, found := strings.Cut(line, ":")
		if field == "" {
			continue
		}
		if found {
			value = strings.TrimPrefix(value, " ")
		}
		switch field {
		case "data":
			if found {
				data = append(data, value)
			}
		case "event":
			frame.Event = value
		case "id":
			frame.ID = value
		}
	}
	frame.Data = strings.Join(data, "\n")
	return frame
}
