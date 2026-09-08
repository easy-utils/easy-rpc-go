package easyrpc

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
)

// ServerStreamReader reads a server-stream response from the wire. Useful for
// server-side handlers that want to serve a stream of messages.
type ServerStreamReader struct {
	r   io.Reader
	end bool
}

// NewServerStreamReader wraps r to read framed responses.
func NewServerStreamReader(r io.Reader) *ServerStreamReader {
	return &ServerStreamReader{r: r}
}

// Next returns the next message payload (de-framed); io.EOF at end.
func (s *ServerStreamReader) Next() ([]byte, error) {
	if s.end {
		return nil, io.EOF
	}
	payload, end, err := ReadFrame(s.r)
	if err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return nil, io.EOF
		}
		return nil, err
	}
	if end {
		// EndStreamMessage carries error/trailers; decode best-effort.
		es := DecodeEndStream(payload)
		if es.Code != 0 {
			return nil, &RPCError{Code: es.Code, Message: es.Message}
		}
		s.end = true
		return nil, io.EOF
	}
	return payload, nil
}

// StreamWriter writes framed responses for a server-stream handler.
type StreamWriter struct {
	w   io.Writer
	end bool
}

// NewStreamWriter wraps w to write framed responses.
func NewStreamWriter(w io.Writer) *StreamWriter {
	return &StreamWriter{w: w}
}

// Write sends one message payload as a frame.
func (s *StreamWriter) Write(payload []byte) error {
	if s.end {
		return io.ErrClosedPipe
	}
	_, err := s.w.Write(Frame(payload, false))
	return err
}

// End terminates the stream with an optional error code.
func (s *StreamWriter) End(code int, message string) error {
	if s.end {
		return nil
	}
	var payload []byte
	if code != 0 {
		payload = EncodeEndStream(EndStreamMessage{Code: code, Message: message})
	}
	_, err := s.w.Write(Frame(payload, true))
	s.end = true
	return err
}

// ---- unary helpers for server handlers ----

// DecodeRequest reads a unary request body into bytes.
func DecodeRequest(r io.Reader) ([]byte, error) { return io.ReadAll(r) }

// EncodeResponse returns a unary response.
func EncodeResponse(body []byte) *bytes.Buffer {
	return bytes.NewBuffer(body)
}

// encodeFrameWithFlags is a lower-level helper (unused externally).
func encodeFrameWithFlags(payload []byte, flags byte) []byte {
	buf := make([]byte, 5+len(payload))
	buf[0] = flags
	binary.BigEndian.PutUint32(buf[1:5], uint32(len(payload)))
	copy(buf[5:], payload)
	return buf
}

// DecodeRawFrame reads a raw frame returning flags + payload (used by tests).
func DecodeRawFrame(r io.Reader) (flags byte, payload []byte, err error) {
	var hdr [5]byte
	if _, err = io.ReadFull(r, hdr[:]); err != nil {
		return 0, nil, err
	}
	flags = hdr[0]
	length := binary.BigEndian.Uint32(hdr[1:5])
	payload = make([]byte, length)
	_, err = io.ReadFull(r, payload)
	return flags, payload, err
}

// ---- context helpers ----

// WithRequestHeader injects headers into a request's context for handlers.
type contextKey string

const reqKey contextKey = "easyrpc.request"

// RequestFromContext returns the request headers/metadata, if present.
func RequestFromContext(ctx context.Context) (Request, bool) {
	r, ok := ctx.Value(reqKey).(Request)
	return r, ok
}

// WithRequest stores a Request in context.
func WithRequest(ctx context.Context, r Request) context.Context {
	return context.WithValue(ctx, reqKey, r)
}
