package easyrpc

import (
	"context"
	"encoding/json"
	"io"
	"strconv"
	"strings"
)

// UnaryHandler decodes request bytes -> response bytes. kind is "proto"|"json".
// ctx carries request metadata headers (see HeadersFromContext) for authz.
type UnaryHandler func(ctx context.Context, req []byte, kind string) (resp []byte, err error)

// StreamHandler serves server-stream; emit(payload,true) ends.
type StreamHandler func(ctx context.Context, req []byte, kind string, emit func(payload []byte, end bool) error) error

// ServiceRegistry maps method name -> handler.
type ServiceRegistry struct {
	Unary  map[string]UnaryHandler
	Stream map[string]StreamHandler
}

// NewServiceRegistry returns an empty registry.
func NewServiceRegistry() *ServiceRegistry {
	return &ServiceRegistry{Unary: map[string]UnaryHandler{}, Stream: map[string]StreamHandler{}}
}

// ResponseWriter is the push-based server sink. Dispatch writes the response
// into it as it is produced; a runtime adapter (net/http, fasthttp, ...)
// implements it for its transport. Stream frames are written with WriteFrame
// and flushed by the adapter — never buffered.
type ResponseWriter interface {
	// Status sets the HTTP status (called before the first write).
	Status(code int)
	// Header receives all response headers before the first write.
	Header(h Headers)
	// WriteFrame writes one payload: an already-framed stream frame, or the raw
	// unary body.
	WriteFrame(payload []byte) error
}

// contentKind picks "proto" or "json" from the incoming headers, defaulting to proto.
func contentKindHeaders(h Headers) string {
	// Streaming JSON arrives as application/connect+json — both prefixes are
	// JSON kinds (spec §2).
	if ct := h.Get("Content-Type"); strings.HasPrefix(ct, "application/json") || strings.HasPrefix(ct, "application/connect+json") {
		return "json"
	}
	if ac := h.Get("Accept"); strings.HasPrefix(ac, "application/json") || strings.HasPrefix(ac, "application/connect+json") {
		return "json"
	}
	return "proto"
}

// Dispatch is the ASGI-style pure application. It decodes an RPC request, runs
// the handler, and PUSHES the response into w frame-by-frame (server-stream is
// never buffered). It is protocol-agnostic: it never imports a specific HTTP
// runtime. Backends (net/http, fasthttp, a hand-rolled server, ...) only adapt
// by providing a ResponseWriter.
func Dispatch(ctx context.Context, req Request, methods []MethodSpec, reg *ServiceRegistry, w ResponseWriter) error {
	kind := contentKindHeaders(req.Headers)
	// Protocol version: reject an explicitly-unsupported version.
	if pv := req.Headers.Get(HeaderProtocolVersion); pv != "" && pv != ConnectProtocolVersion {
		return writeError(w, &RPCError{Code: 12, Message: "unsupported connect-protocol-version: " + pv})
	}
	if len(req.Body) > DefaultMaxMessageBytes {
		return writeError(w, &RPCError{Code: 8, Message: "request too large"})
	}
	path := req.URL
	if i := strings.IndexByte(path, '?'); i >= 0 {
		path = path[:i]
	}
	ct := "application/proto"
	if kind == "json" {
		ct = "application/json"
	}
	// find method by path
	var spec *MethodSpec
	for i := range methods {
		if methods[i].Path == path {
			spec = &methods[i]
			break
		}
	}
	if spec == nil {
		return writeError(w, &RPCError{Code: 5, Message: "not found"})
	}
	if spec.ServerStream {
		h := reg.Stream[spec.Name]
		if h == nil {
			return writeError(w, &RPCError{Code: 5, Message: "method not found"})
		}
		// Connect semantics: a server-stream is always HTTP 200; failures ride
		// the END frame, never an HTTP status.
		w.Status(200)
		w.Header(Headers{"Content-Type": []string{streamContent(ct)}})
		wantsGzip := false
		for _, v := range req.Headers[HeaderAcceptEncoding] {
			for _, e := range strings.Split(v, ",") {
				if strings.TrimSpace(e) == EncodingGzip {
					wantsGzip = true
				}
			}
		}
		var ended bool
		emit := func(p []byte, end bool) error {
			if ended {
				return nil
			}
			if end {
				ended = true
				return w.WriteFrame(Frame(nil, true))
			}
			if wantsGzip && len(p) >= CompressMinBytes {
				if z, err := GzipCompress(p); err == nil {
					return w.WriteFrame(FrameCompressed(z))
				}
			}
			return w.WriteFrame(Frame(p, false))
		}
		if err := h(ctx, req.Body, kind, emit); err != nil {
			re := asRPCError(err)
			if !ended {
				ended = true
				_ = w.WriteFrame(Frame(EncodeEndStream(EndStreamMessage{Code: re.Code, Message: re.Message, Details: re.Details}), true))
			}
			return nil
		}
		if !ended {
			_ = w.WriteFrame(Frame(nil, true))
		}
		return nil
	}
	h := reg.Unary[spec.Name]
	if h == nil {
		return writeError(w, &RPCError{Code: 5, Message: "method not found"})
	}
	// Unary: resolve fully before writing so a failure can set a real status.
	resp, err := h(ctx, req.Body, kind)
	if err != nil {
		return writeError(w, asRPCError(err))
	}
	w.Status(200)
	w.Header(Headers{"Content-Type": []string{ct}})
	return w.WriteFrame(resp)
}

// writeError emits a non-200 error response. Only reached before any body
// bytes. The Connect code travels as the `connect-code` header so the client
// can reconstruct the exact error (the HTTP status alone is lossy).
func writeError(w ResponseWriter, err *RPCError) error {
	// Connect unary error: HTTP status + JSON body `{code,message}`. The legacy
	// connect-code/connect-error headers are kept for backward compatibility.
	w.Status(HTTPStatus(err.Code))
	w.Header(Headers{
		"Content-Type":  []string{"application/json"},
		"Connect-Code":  []string{strconv.Itoa(err.Code)},
		"Connect-Error": []string{err.Message},
	})
	return w.WriteFrame(EncodeErrorJSON(err.Code, err.Message, err.Details))
}

// EncodeErrorJSON builds a Connect unary error body (details omitted when
// empty, keeping the v1.0 byte-for-byte shape).
func EncodeErrorJSON(code int, message string, details []ErrorDetail) []byte {
	body := struct {
		Code    string       `json:"code"`
		Message string       `json:"message"`
		Details []wireDetail `json:"details,omitempty"`
	}{Code: CodeToString(code), Message: message, Details: encodeWireDetails(details)}
	b, _ := json.Marshal(body)
	return b
}

// DecodeErrorJSON parses a Connect unary error body; (0, "") when not an error
// body. Tolerates plain-text bodies.
func DecodeErrorJSON(body []byte) (int, string, []ErrorDetail) {
	if len(body) == 0 {
		return 0, "", nil
	}
	var es struct {
		Code    string       `json:"code"`
		Message string       `json:"message"`
		Details []wireDetail `json:"details"`
	}
	if err := json.Unmarshal(body, &es); err == nil && es.Code != "" {
		return CodeFromString(es.Code), es.Message, decodeWireDetails(es.Details)
	}
	return 0, "", nil
}

func streamContent(ct string) string {
	if ct == "application/json" {
		return "application/connect+json"
	}
	return "application/connect+proto"
}

func asRPCError(err error) *RPCError {
	if err == nil {
		return nil
	}
	if re, ok := err.(*RPCError); ok {
		return re
	}
	return &RPCError{Code: 13, Message: err.Error()}
}

// StreamWriter writes framed responses for a server-stream handler, writing
// frames directly to an io.Writer (e.g. an http.ResponseWriter).
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
