// Package easyrpc provides the zero-runtime-bindings core: a Transport
// interface + the Connect wire protocol (unary + server-stream). Concrete HTTP
// runtimes are provided by separate bridge packages; the protocol logic here
// never imports a specific HTTP library.
package easyrpc

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
)

// Headers is a generic multi-value header map.
type Headers map[string][]string

// Request is a normalized RPC request independent of any HTTP runtime.
type Request struct {
	URL     string
	Method  string // http method (GET / POST / ...)
	Headers Headers
	Body    []byte
}

// Response is a normalized response.
type Response struct {
	Status  int
	Headers Headers
	Body    []byte
	// Trailers are optional (set on streaming end).
	Trailers Headers
	// Error carries a non-nil value when the RPC failed.
	Error *RPCError
}

// RPCError is the wire-level error with a Connect code.
type RPCError struct {
	Code    int    // Connect code (3 invalid, 5 notfound, ...)
	Message string
}

func (e *RPCError) Error() string { return fmt.Sprintf("easyrpc: code=%d %s", e.Code, e.Message) }

// HTTPStatus maps a Connect code to an HTTP status.
func HTTPStatus(code int) int {
	switch code {
	case 3:
		return http.StatusBadRequest
	case 5:
		return http.StatusNotFound
	case 7:
		return http.StatusForbidden
	case 8:
		return http.StatusTooManyRequests
	case 16:
		return http.StatusUnauthorized
	case 14:
		return http.StatusServiceUnavailable
	default:
		return http.StatusInternalServerError
	}
}

// ConnectCode maps an HTTP status to a Connect code.
func connectFromStatus(status int) int {
	switch status {
	case http.StatusBadRequest:
		return 3
	case http.StatusNotFound:
		return 5
	case http.StatusForbidden:
		return 7
	case http.StatusUnauthorized:
		return 16
	case http.StatusTooManyRequests:
		return 8
	case http.StatusServiceUnavailable:
		return 14
	default:
		return 13
	}
}

// Transport is the core interface a bridge must implement.
type Transport interface {
	// Send performs a unary call.
	Send(ctx context.Context, req Request) (Response, error)
	// OpenStream opens a server-stream call. The returned stream yields frames
	// until closed or canceled via Close().
	OpenStream(ctx context.Context, req Request) (Stream, error)
}

// Stream is a server-stream response.
type Stream interface {
	// Recv reads the next raw frame payload (already de-framed). Returns
	// io.EOF when the stream ends.
	Recv() ([]byte, error)
	// Cancel terminates the stream (v1 cancellation).
	Cancel()
	// Close shuts down resources.
	Close() error
}

// SetHeader is a helper to set a single-value header.
func (h Headers) Set(k, v string) { h[k] = []string{v} }
func (h Headers) Get(k string) string {
	if v := h[k]; len(v) > 0 {
		return v[0]
	}
	return ""
}

// ---- protocol helpers (Connect wire) ----

// EncodeUnaryBody wraps a request message into unary body (identity: it IS the
// message bytes for unary, no framing).
func EncodeUnaryBody(msg []byte) []byte { return msg }

// DecodeUnaryBody returns the unary response message bytes.
func DecodeUnaryBody(rep Response) ([]byte, error) {
	if rep.Error != nil {
		return nil, rep.Error
	}
	if rep.Status >= 300 {
		return nil, &RPCError{Code: connectFromStatus(rep.Status), Message: rep.Headers.Get("connect-error")}
	}
	return rep.Body, nil
}

// Frame encodes a single streaming frame. flags bit0=compressed (always 0),
// bit1=endStream.
func Frame(payload []byte, endStream bool) []byte {
	var flags byte
	if endStream {
		flags |= 0x02
	}
	buf := make([]byte, 5+len(payload))
	buf[0] = flags
	binary.BigEndian.PutUint32(buf[1:5], uint32(len(payload)))
	copy(buf[5:], payload)
	return buf
}

// ReadFrame reads one frame from r. Returns payload, endStream, error
// (io.EOF clean end). It validates length bounds.
func ReadFrame(r io.Reader) (payload []byte, endStream bool, err error) {
	var hdr [5]byte
	if _, err = io.ReadFull(r, hdr[:]); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, false, io.EOF
		}
		return nil, false, err
	}
	flags := hdr[0]
	length := binary.BigEndian.Uint32(hdr[1:5])
	if length > 64*1024*1024 {
		return nil, false, errors.New("easyrpc: frame too large")
	}
	payload = make([]byte, length)
	if _, err = io.ReadFull(r, payload); err != nil {
		return nil, false, err
	}
	return payload, flags&0x02 != 0, nil
}

// EndStreamMessage is a minimal trailer frame payload used to signal end.
type EndStreamMessage struct {
	Code    int
	Message string
}

// EncodeEndStream encodes an EndStreamMessage as a frame payload.
func EncodeEndStream(m EndStreamMessage) []byte {
	var b bytes.Buffer
	b.WriteByte(byte(m.Code))
	b.WriteString("\x00")
	b.WriteString(m.Message)
	return b.Bytes()
}

// DecodeEndStream decodes an EndStreamMessage payload.
func DecodeEndStream(payload []byte) EndStreamMessage {
	if len(payload) == 0 {
		return EndStreamMessage{}
	}
	code := int(payload[0])
	rest := payload[1:]
	msg := string(rest)
	if idx := bytes.IndexByte(rest, 0); idx >= 0 {
		msg = string(rest[idx+1:])
	}
	return EndStreamMessage{Code: code, Message: msg}
}

// URLFor builds the default gRPC-style path for a method.
func URLFor(pkg, service, method string) string {
	return "/" + pkg + "." + service + "/" + method
}

// MethodSpec describes a generated RPC method (mirrors what generators emit).
type MethodSpec struct {
	Service        string // e.g. "easyrpc.conformance.v1.ConformanceService"
	Name           string // e.g. "Echo"
	Path           string // resolved HTTP path (REST or gRPC style)
	HTTPMethod     string // GET / POST
	ClientStream   bool
	ServerStream   bool
	Body           string // body binding ("*" or field name) for REST
}

// ServiceDesc is the runtime descriptor for a generated service.
type ServiceDesc struct {
	TypeName string
	Methods  []MethodSpec
}

// SortMethods returns methods sorted by name (deterministic generator output).
func SortMethods(m []MethodSpec) { sort.Slice(m, func(i, j int) bool { return m[i].Name < m[j].Name }) }

// StatusFromHeader parses connect-error into an RPCError.
func statusFromHeader(h Headers) *RPCError {
	code := h.Get("connect-code")
	if code == "" {
		// tolerate gRPC-style for interop
		return nil
	}
	c, _ := strconv.Atoi(code)
	return &RPCError{Code: c, Message: h.Get("connect-error")}
}
