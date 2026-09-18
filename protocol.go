// Package easyrpc provides the zero-runtime-bindings core: a Transport
// interface + the Connect wire protocol (unary + server-stream). Concrete HTTP
// runtimes are provided by separate bridge packages; the protocol logic here
// never imports a specific HTTP library.
package easyrpc

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
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

// Version is the easy-rpc Go core version.
const Version = "1.0.0"

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

// MetadataTransport decorates a Transport by merging fixed metadata headers.
//
// Deprecated: use WithInterceptors(rt, MetadataInterceptor(md)). The interceptor
// form composes with TimeoutInterceptor and any user interceptor, and works
// over any adapter.
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

// Interceptor wraps a Transport call. It may mutate the request (attach
// auth/metadata), impose a deadline, observe the result, or short-circuit.
// Unary receives (ctx, req); stream receives (ctx, req) and returns a Stream.
// `next` performs the actual call.
type Interceptor struct {
	Unary  func(ctx context.Context, req Request, next func(context.Context, Request) (Response, error)) (Response, error)
	Stream func(ctx context.Context, req Request, next func(context.Context, Request) (Stream, error)) (Stream, error)
}

// InterceptorTransport applies interceptors (outermost first) around a Transport.
type InterceptorTransport struct {
	ics []Interceptor
	rt  Transport
}

// WithInterceptors wraps rt with the given interceptors (first = outermost).
func WithInterceptors(rt Transport, ics ...Interceptor) *InterceptorTransport {
	return &InterceptorTransport{ics: ics, rt: rt}
}

func (t *InterceptorTransport) send(ctx context.Context, i int, req Request) (Response, error) {
	if i >= len(t.ics) {
		return t.rt.Send(ctx, req)
	}
	ic := t.ics[i]
	if ic.Unary == nil {
		return t.send(ctx, i+1, req)
	}
	return ic.Unary(ctx, req, func(c context.Context, r Request) (Response, error) {
		return t.send(c, i+1, r)
	})
}

func (t *InterceptorTransport) stream(ctx context.Context, i int, req Request) (Stream, error) {
	if i >= len(t.ics) {
		return t.rt.OpenStream(ctx, req)
	}
	ic := t.ics[i]
	if ic.Stream == nil {
		return t.stream(ctx, i+1, req)
	}
	return ic.Stream(ctx, req, func(c context.Context, r Request) (Stream, error) {
		return t.stream(c, i+1, r)
	})
}

// Send implements Transport.
func (t *InterceptorTransport) Send(ctx context.Context, req Request) (Response, error) {
	return t.send(ctx, 0, req)
}

// OpenStream implements Transport.
func (t *InterceptorTransport) OpenStream(ctx context.Context, req Request) (Stream, error) {
	return t.stream(ctx, 0, req)
}

// MetadataInterceptor attaches fixed metadata to every call.
func MetadataInterceptor(md Headers) Interceptor {
	aug := func(req Request) Request {
		if req.Headers == nil {
			req.Headers = Headers{}
		}
		for k, vs := range md {
			if _, ok := req.Headers[k]; !ok {
				req.Headers[k] = vs
			}
		}
		return req
	}
	return Interceptor{
		Unary: func(ctx context.Context, req Request, next func(context.Context, Request) (Response, error)) (Response, error) {
			return next(ctx, aug(req))
		},
		Stream: func(ctx context.Context, req Request, next func(context.Context, Request) (Stream, error)) (Stream, error) {
			return next(ctx, aug(req))
		},
	}
}

// TimeoutInterceptor attaches the Connect timeout header to every call.
func TimeoutInterceptor(d time.Duration) Interceptor {
	aug := func(req Request) Request { return WithTimeout(req, d) }
	return Interceptor{
		Unary: func(ctx context.Context, req Request, next func(context.Context, Request) (Response, error)) (Response, error) {
			return next(ctx, aug(req))
		},
		Stream: func(ctx context.Context, req Request, next func(context.Context, Request) (Stream, error)) (Stream, error) {
			return next(ctx, aug(req))
		},
	}
}

// Request is a normalized RPC request independent of any HTTP runtime.
// All easy-rpc calls are POST (spec §0); no method field.
type Request struct {
	URL     string
	Headers Headers
	Body    []byte
}

// Response is a normalized response.
type Response struct {
	Status  int
	Headers Headers
	Body    []byte
	// Trailers are the unary trailing metadata (demuxed from `trailer-*`
	// response headers by the bridge).
	Trailers Headers
	// Error carries a non-nil value when the RPC failed.
	Error *RPCError
}

// ErrorDetail is one structured error detail (spec §4.1, aligned with Connect
// Error Details / gRPC google.rpc status details). Type is a type URL; Value
// is opaque bytes (typically an encoded protobuf message).
type ErrorDetail struct {
	Type  string `json:"type"`
	Value []byte `json:"value,omitempty"`
}

// RPCError is the wire-level error with a Connect code.
type RPCError struct {
	Code    int // Connect code (3 invalid, 5 notfound, ...)
	Message string
	// Optional structured details (spec §4.1); opaque to the wire layer.
	Details []ErrorDetail
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
	// Trailers returns the trailing metadata (available after io.EOF).
	Trailers() Headers
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
	if length > DefaultMaxMessageBytes {
		return nil, false, &RPCError{Code: 8, Message: "frame too large"}
	}
	payload = make([]byte, length)
	if _, err = io.ReadFull(r, payload); err != nil {
		return nil, false, err
	}
	return payload, flags&0x02 != 0, nil
}

// EndStreamMessage is the Connect end-stream payload. A clean end carries
// Code == 0; a failure carries the Connect code + message. It serializes as
// `{"error":{"code":"<name>","message":"..."},"metadata":{...}}`.
type EndStreamMessage struct {
	Code    int
	Message string
	// Optional structured details carried in the end-stream JSON (spec §4.1).
	Details []ErrorDetail
	// Trailing metadata (spec §3.3); nil = none.
	Metadata Headers
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
		Code    string       `json:"code"`
		Message string       `json:"message"`
		Details []wireDetail `json:"details,omitempty"`
	} `json:"error,omitempty"`
	Metadata Headers `json:"metadata,omitempty"`
}

// wireDetail is the JSON shape of one detail: {type, base64 value}.
type wireDetail struct {
	Type  string `json:"type"`
	Value string `json:"value"`
}

func encodeWireDetails(details []ErrorDetail) []wireDetail {
	if len(details) == 0 {
		return nil
	}
	out := make([]wireDetail, 0, len(details))
	for _, d := range details {
		out = append(out, wireDetail{Type: d.Type, Value: base64.RawStdEncoding.EncodeToString(d.Value)})
	}
	return out
}

// decodeWireBase64 accepts standard OR URL-safe base64, padded or unpadded
// (Connect emits unpadded RawStdEncoding).
func decodeWireBase64(v string) ([]byte, error) {
	v = strings.TrimRight(v, "=")
	v = strings.NewReplacer("-", "+", "_", "/").Replace(v)
	return base64.RawStdEncoding.DecodeString(v)
}

// decodeWireDetails parses the JSON details array; malformed entries (bad
// base64, missing type) are skipped, never fatal (matrix M7).
func decodeWireDetails(v []wireDetail) []ErrorDetail {
	out := make([]ErrorDetail, 0, len(v))
	for _, d := range v {
		if d.Type == "" || d.Value == "" {
			continue
		}
		raw, err := decodeWireBase64(d.Value)
		if err != nil {
			continue
		}
		out = append(out, ErrorDetail{Type: d.Type, Value: raw})
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// EncodeEndStream encodes an EndStreamMessage as a Connect end-stream payload.
// A clean end still serializes as `{}` — Connect's parser requires valid JSON.
func EncodeEndStream(m EndStreamMessage) []byte {
	var es endStreamJSON
	if m.Code != 0 || len(m.Details) > 0 {
		code := m.Code
		if code == 0 {
			// Details without an error code are not representable.
			code = 2
		}
		es.Error = &struct {
			Code    string       `json:"code"`
			Message string       `json:"message"`
			Details []wireDetail `json:"details,omitempty"`
		}{Code: CodeToString(code), Message: m.Message, Details: encodeWireDetails(m.Details)}
	}
	if len(m.Metadata) > 0 {
		es.Metadata = m.Metadata
	}
	b, _ := json.Marshal(es)
	return b
}

// DecodeEndStream decodes a Connect end-stream payload (empty => clean end).
// Malformed input decodes to the zero value (clean end, matrix M2).
func DecodeEndStream(payload []byte) EndStreamMessage {
	if len(payload) == 0 {
		return EndStreamMessage{}
	}
	var es endStreamJSON
	if err := json.Unmarshal(payload, &es); err != nil {
		return EndStreamMessage{}
	}
	if es.Error == nil {
		// 0-code clean end, but may carry metadata (spec M14).
		return EndStreamMessage{Metadata: es.Metadata}
	}
	code := 2
	if es.Error.Code != "" {
		code = CodeFromString(es.Error.Code)
	}
	return EndStreamMessage{Code: code, Message: es.Error.Message, Details: decodeWireDetails(es.Error.Details), Metadata: es.Metadata}
}

// HeaderTimeout is the Connect request-timeout header.
const HeaderTimeout = "connect-timeout-ms"

// HeaderProtocolVersion is the Connect protocol-version header.
const HeaderProtocolVersion = "connect-protocol-version"

// ConnectProtocolVersion is the version this package speaks.
const ConnectProtocolVersion = "1"

// DefaultMaxMessageBytes is the default read/write size cap (Connect default).
const DefaultMaxMessageBytes = 4 * 1024 * 1024

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

// Compression headers.
const (
	HeaderAcceptEncoding  = "connect-accept-encoding"
	HeaderContentEncoding = "connect-content-encoding"
	EncodingGzip          = "gzip"
	CompressMinBytes      = 1024
)

// DeadlineInterceptor attaches the Connect timeout header AND enforces the
// deadline locally via context, so it works over any adapter that takes a
// context (all net/http-based ones do).
func DeadlineInterceptor(d time.Duration) Interceptor {
	return Interceptor{
		Unary: func(ctx context.Context, req Request, next func(context.Context, Request) (Response, error)) (Response, error) {
			if d <= 0 {
				return next(ctx, req)
			}
			c, cancel := context.WithTimeout(ctx, d)
			defer cancel()
			return next(c, WithTimeout(req, d))
		},
		Stream: func(ctx context.Context, req Request, next func(context.Context, Request) (Stream, error)) (Stream, error) {
			if d <= 0 {
				return next(ctx, req)
			}
			// The stream's caller owns cancellation; derive the deadline from
			// the caller's context so it collapses when they cancel. The
			// deadline context is released when the stream closes.
			c, cancel := context.WithTimeout(ctx, d)
			st, err := next(c, WithTimeout(req, d))
			if err != nil {
				cancel()
				return nil, err
			}
			return cancelOnClose{st, cancel}, nil
		},
	}
}

// cancelOnClose releases the deadline context when the stream is closed.
type cancelOnClose struct {
	Stream
	cancel context.CancelFunc
}

func (c cancelOnClose) Close() error {
	c.cancel()
	return c.Stream.Close()
}

func (c cancelOnClose) Cancel() {
	c.cancel()
	c.Stream.Cancel()
}

// FrameCompressed wraps a payload in a frame with the Compressed flag set.
func FrameCompressed(payload []byte) []byte {
	z, err := GzipCompress(payload)
	if err != nil {
		return Frame(payload, false) // opportunistic: identity on failure
	}
	out := Frame(z, false)
	out[0] |= 0x01
	return out
}

// ReadFrameDecompressed reads one frame and gzip-decompresses it when the
// Compressed flag is set.
func ReadFrameDecompressed(r io.Reader) (payload []byte, endStream bool, err error) {
	payload, end, flags, err := readFrameFlags(r)
	if err != nil {
		return nil, false, err
	}
	if flags&0x01 != 0 {
		// Fault matrix M10: a flagged-but-corrupt gzip payload is a protocol
		// error, never silently-yielded raw compressed bytes.
		z, derr := GzipDecompress(payload)
		if derr != nil {
			return nil, false, &RPCError{Code: 13, Message: "corrupt gzip frame: " + derr.Error()}
		}
		payload = z
	}
	return payload, end, nil
}

// readFrameFlags is ReadFrame but also returns the raw frame flags byte.
func readFrameFlags(r io.Reader) (payload []byte, endStream bool, flags byte, err error) {
	var hdr [5]byte
	if _, err = io.ReadFull(r, hdr[:]); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, false, 0, io.EOF
		}
		return nil, false, 0, err
	}
	flags = hdr[0]
	length := binary.BigEndian.Uint32(hdr[1:5])
	if length > DefaultMaxMessageBytes {
		return nil, false, 0, &RPCError{Code: 8, Message: "frame too large"}
	}
	payload = make([]byte, length)
	if _, err = io.ReadFull(r, payload); err != nil {
		return nil, false, 0, err
	}
	return payload, flags&0x02 != 0, flags, nil
}

// GzipCompress gzip-compresses data.
func GzipCompress(data []byte) ([]byte, error) {
	var b bytes.Buffer
	w := gzip.NewWriter(&b)
	if _, err := w.Write(data); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

// GzipDecompress gzip-decompresses data.
func GzipDecompress(data []byte) ([]byte, error) {
	r, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer r.Close()
	return io.ReadAll(r)
}

// URLFor builds the default gRPC-style path for a method.
func URLFor(pkg, service, method string) string {
	return "/" + pkg + "." + service + "/" + method
}

// TrailerHeaderPrefix is the prefix for unary trailing metadata on response
// headers (`trailer-<key>`), matching Connect.
const TrailerHeaderPrefix = "trailer-"

// MuxTrailers merges trailing metadata into response headers using the
// `trailer-` prefix.
func MuxTrailers(headers, trailers Headers) Headers {
	out := Headers{}
	for k, v := range headers {
		out[k] = v
	}
	for k, v := range trailers {
		out[TrailerHeaderPrefix+strings.ToLower(k)] = v
	}
	return out
}

// DemuxTrailers splits response headers into (headers, trailers) by the
// case-insensitive `trailer-` prefix.
func DemuxTrailers(all Headers) (Headers, Headers) {
	h := Headers{}
	t := Headers{}
	for k, v := range all {
		lk := strings.ToLower(k)
		if strings.HasPrefix(lk, TrailerHeaderPrefix) {
			t[lk[len(TrailerHeaderPrefix):]] = v
		} else {
			h[k] = v
		}
	}
	return h, t
}

// HandlerContext is passed to generated unary/stream handlers. It exposes
// request metadata and a channel to set trailing metadata. It travels inside
// the handler's context (see HandlerContextFromContext), so the generated
// service interface keeps its plain (ctx, in) signature.
type HandlerContext struct {
	Headers Headers
	header  Headers
	trailer Headers
}

// NewHandlerContext builds a context carrying request metadata.
func NewHandlerContext(headers Headers) *HandlerContext {
	return &HandlerContext{Headers: headers, header: Headers{}, trailer: Headers{}}
}

// SetHeader records a response header (non-trailer). Emitted on both unary and
// server-stream responses; repeated calls accumulate multiple wire values.
func (c *HandlerContext) SetHeader(key, value string) {
	if c.header == nil {
		c.header = Headers{}
	}
	c.header[key] = append(c.header[key], value)
}

// ResponseHeaders returns the accumulated response headers.
func (c *HandlerContext) ResponseHeaders() Headers { return c.header }

// SetTrailer records a trailing-metadata entry (unary: `trailer-*` header;
// server-stream: END-frame metadata).
func (c *HandlerContext) SetTrailer(key, value string) {
	if c.trailer == nil {
		c.trailer = Headers{}
	}
	c.trailer[key] = append(c.trailer[key], value)
}

// Trailers returns the accumulated trailing metadata.
func (c *HandlerContext) Trailers() Headers { return c.trailer }

type handlerContextKey struct{}

// ContextWithHandlerContext attaches hc to ctx.
func ContextWithHandlerContext(ctx context.Context, hc *HandlerContext) context.Context {
	return context.WithValue(ctx, handlerContextKey{}, hc)
}

// HandlerContextFromContext returns the handler context, or nil.
func HandlerContextFromContext(ctx context.Context) *HandlerContext {
	hc, _ := ctx.Value(handlerContextKey{}).(*HandlerContext)
	return hc
}

// MethodSpec describes a generated RPC method (mirrors what generators emit).
// easy-rpc v2: every method is POST with a gRPC-style path; no verb/body.
type MethodSpec struct {
	Service      string // e.g. "easyrpc.conformance.v1.ConformanceService"
	Name         string // e.g. "Echo"
	Path         string // gRPC-style path "/pkg.Service/Method"
	ClientStream bool
	ServerStream bool
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
