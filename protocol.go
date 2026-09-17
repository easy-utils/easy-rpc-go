// Package easyrpc provides the zero-runtime-bindings core: a Transport
// interface + the Connect wire protocol (unary + server-stream). Concrete HTTP
// runtimes are provided by separate bridge packages; the protocol logic here
// never imports a specific HTTP library.
package easyrpc

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Headers is a generic multi-value header map.
type Headers map[string][]string

// metadataKey is the context key for RPC metadata headers.
type metadataKey struct{}

// ContextWithHeaders attaches per-RPC metadata headers to a context. Bridges
// and server handlers use this to pass auth/tracing metadata without touching
// the wire-level Request/Response types.
func ContextWithHeaders(ctx context.Context, hdr Headers) context.Context {
	return context.WithValue(ctx, metadataKey{}, hdr)
}

// HeadersFromContext returns the metadata headers attached via
// ContextWithHeaders, or nil. It lets a service implementation read incoming
// request metadata (e.g. an Authorization header) for authz.
func HeadersFromContext(ctx context.Context) Headers {
	h, _ := ctx.Value(metadataKey{}).(Headers)
	return h
}

// MetadataTransport decorates a Transport by merging fixed metadata headers
// (auth tokens, tenant ids, credentials, ...) into every outgoing request.
// This is the documented way to support auth without changing core: the wire
// stays additive headers; TLS/CA config lives in the bridge client.
type MetadataTransport struct {
	rt Transport
	md Headers
}

// WithMetadata wraps rt so each request carries md in its headers. Mutable
// request-specific headers are preserved; call-specific ones win.
func WithMetadata(md Headers, rt Transport) *MetadataTransport {
	return &MetadataTransport{rt: rt, md: md}
}

// Send implements Transport, merging fixed metadata into the request headers.
func (t *MetadataTransport) Send(ctx context.Context, req Request) (Response, error) {
	return t.rt.Send(ctx, t.augment(req))
}

// OpenStream implements Transport, merging fixed metadata into the request headers.
func (t *MetadataTransport) OpenStream(ctx context.Context, req Request) (Stream, error) {
	return t.rt.OpenStream(ctx, t.augment(req))
}

func (t *MetadataTransport) augment(req Request) Request {
	if req.Headers == nil {
		req.Headers = Headers{}
	}
	for k, vs := range t.md {
		if _, ok := req.Headers[k]; !ok {
			req.Headers[k] = vs
		}
	}
	return req
}

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
	Code    int // Connect code (3 invalid, 5 notfound, ...)
	Message string
}

func (e *RPCError) Error() string { return fmt.Sprintf("easyrpc: code=%d %s", e.Code, e.Message) }

// HTTPStatus maps a Connect code to an HTTP status. Covers the full
// gRPC/Connect error-code space:
//
//	0 OK(200) 1 499 2 500 3 400 4 504 5 404 6 409 7 403 8 429
//	9 400 10 409 11 400 12 501 13 500 14 503 15 500 16 401
func HTTPStatus(code int) int {
	switch code {
	case 1:
		return 499 // client closed
	case 2:
		return http.StatusInternalServerError
	case 3:
		return http.StatusBadRequest
	case 4:
		return http.StatusGatewayTimeout
	case 5:
		return http.StatusNotFound
	case 6:
		return http.StatusConflict
	case 7:
		return http.StatusForbidden
	case 8:
		return http.StatusTooManyRequests
	case 9:
		return http.StatusBadRequest
	case 10:
		return http.StatusConflict
	case 11:
		return http.StatusBadRequest
	case 12:
		return http.StatusNotImplemented
	case 13:
		return http.StatusInternalServerError
	case 14:
		return http.StatusServiceUnavailable
	case 15:
		return http.StatusInternalServerError
	case 16:
		return http.StatusUnauthorized
	default:
		return http.StatusInternalServerError
	}
}

// ConnectCode maps an HTTP status to a Connect code. Because several Connect
// codes share an HTTP status (400<->3/9/11, 409<->6/10, 500<->2/13/15), the
// reverse direction is lossy and returns the most common code for that status.
func ConnectFromStatus(status int) int {
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
	case http.StatusConflict:
		return 10
	case http.StatusGatewayTimeout:
		return 4
	case http.StatusNotImplemented:
		return 12
	case 499:
		return 1
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
	// Canonical MIME header form (matches net/http's canonicalization).
	ck := http.CanonicalHeaderKey(k)
	if v := h[ck]; len(v) > 0 {
		return v[0]
	}
	for key, v := range h {
		if strings.EqualFold(key, k) && len(v) > 0 {
			return v[0]
		}
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

// EndStreamMessage is the Connect end-stream payload. A clean end carries
// Code == 0; a failure carries the Connect code + message. It serializes as
// `{"error":{"code":"<name>","message":"..."}}` (see the Connect protocol).
type EndStreamMessage struct {
	Code    int
	Message string
}

// CodeNames maps Connect codes to their stable lowercase wire names.
var CodeNames = map[int]string{
	0: "ok", 1: "canceled", 2: "unknown", 3: "invalid_argument",
	4: "deadline_exceeded", 5: "not_found", 6: "already_exists",
	7: "permission_denied", 8: "resource_exhausted", 9: "failed_precondition",
	10: "aborted", 11: "out_of_range", 12: "unimplemented", 13: "internal",
	14: "unavailable", 15: "data_loss", 16: "unauthenticated",
}

// CodeToString returns the stable lowercase name for a Connect code.
func CodeToString(code int) string {
	if s, ok := CodeNames[code]; ok {
		return s
	}
	return "unknown"
}

// CodeFromString maps a wire code name back to its Connect code (unknown -> 2).
func CodeFromString(name string) int {
	for c, s := range CodeNames {
		if s == name {
			return c
		}
	}
	return 2
}

type endStreamJSON struct {
	Error *struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error,omitempty"`
}

// EncodeEndStream encodes an EndStreamMessage as a Connect end-stream payload.
func EncodeEndStream(m EndStreamMessage) []byte {
	if m.Code == 0 {
		return nil
	}
	var es endStreamJSON
	es.Error = &struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	}{Code: CodeToString(m.Code), Message: m.Message}
	b, _ := json.Marshal(es)
	return b
}

// DecodeEndStream decodes a Connect end-stream payload (empty => clean end).
func DecodeEndStream(payload []byte) EndStreamMessage {
	if len(payload) == 0 {
		return EndStreamMessage{}
	}
	var es endStreamJSON
	if err := json.Unmarshal(payload, &es); err != nil || es.Error == nil {
		return EndStreamMessage{}
	}
	return EndStreamMessage{Code: CodeFromString(es.Error.Code), Message: es.Error.Message}
}

// HeaderTimeout is the Connect request-timeout header.
const HeaderTimeout = "connect-timeout-ms"

// ParseTimeout parses the Connect timeout header into a duration (0 = none).
func ParseTimeout(value string) time.Duration {
	if value == "" {
		return 0
	}
	ms, err := strconv.Atoi(value)
	if err != nil || ms <= 0 {
		return 0
	}
	return time.Duration(ms) * time.Millisecond
}

// WithTimeout attaches a deadline to a request's headers (and context, if the
// caller uses the returned context).
func WithTimeout(req Request, d time.Duration) Request {
	if d <= 0 {
		return req
	}
	if req.Headers == nil {
		req.Headers = Headers{}
	}
	req.Headers.Set(HeaderTimeout, strconv.FormatInt(d.Milliseconds(), 10))
	return req
}

// URLFor builds the default gRPC-style path for a method.
func URLFor(pkg, service, method string) string {
	return "/" + pkg + "." + service + "/" + method
}

// MethodSpec describes a generated RPC method (mirrors what generators emit).
type MethodSpec struct {
	Service      string // e.g. "easyrpc.conformance.v1.ConformanceService"
	Name         string // e.g. "Echo"
	Path         string // resolved HTTP path (REST or gRPC style)
	HTTPMethod   string // GET / POST
	ClientStream bool
	ServerStream bool
	Body         string // body binding ("*" or field name) for REST
}

// ServiceDesc is the runtime descriptor for a generated service.
type ServiceDesc struct {
	TypeName string
	Methods  []MethodSpec
}

// SortMethods returns methods sorted by name (deterministic generator output).
func SortMethods(m []MethodSpec) { sort.Slice(m, func(i, j int) bool { return m[i].Name < m[j].Name }) }

// StatusFromHeader parses connect-error into an RPCError.
func StatusFromHeader(h Headers) *RPCError {
	code := h.Get("connect-code")
	if code == "" {
		// tolerate gRPC-style for interop
		return nil
	}
	c, _ := strconv.Atoi(code)
	return &RPCError{Code: c, Message: h.Get("connect-error")}
}

func connectFromStatus(status int) int { return ConnectFromStatus(status) }

// statusFromHeader is the internal alias kept for existing callers.
func statusFromHeader(h Headers) *RPCError { return StatusFromHeader(h) }
