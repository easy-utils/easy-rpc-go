package easyrpc

import (
	"context"
	"encoding/base64"
	"io"
	"net/http"
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

// contentKind picks "proto" or "json" from the incoming headers, defaulting to proto.
func contentKindHeaders(h Headers) string {
	if ct := h.Get("Content-Type"); strings.HasPrefix(ct, "application/json") {
		return "json"
	}
	if ac := h.Get("Accept"); strings.HasPrefix(ac, "application/json") {
		return "json"
	}
	return "proto"
}

// Dispatch is the ASGI-style pure application. It decodes an RPC request into
// a response given method specs + a service registry. It is protocol-agnostic:
// it never imports a specific HTTP runtime. Backends (net/http, fasthttp, a
// hand-rolled server, ...) only adapt `Request -> Response` by calling Dispatch.
func Dispatch(ctx context.Context, req Request, methods []MethodSpec, reg *ServiceRegistry) Response {
	kind := contentKindHeaders(req.Headers)
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
		return errorResponse(&RPCError{Code: 5, Message: "not found"})
	}
	if spec.ServerStream {
		h := reg.Stream[spec.Name]
		if h == nil {
			return errorResponse(&RPCError{Code: 5, Message: "method not found"})
		}
		hdrs := Headers{"Content-Type": []string{streamContent(ct)}}
		frames := [][]byte{Frame(nil, true)} // placeholder appended below
		_ = frames
		var payloads [][]byte
		emit := func(p []byte, end bool) error {
			if end {
				return nil
			}
			payloads = append(payloads, p)
			return nil
		}
		err := h(ctx, req.Body, kind, emit)
		if err != nil {
			hdrs["Content-Type"] = []string{"text/plain"}
			return Response{Status: HTTPStatus(asRPCError(err).Code), Headers: hdrs, Body: []byte(err.Error()), Error: asRPCError(err)}
		}
		var out []byte
		for _, p := range payloads {
			out = append(out, Frame(p, false)...)
		}
		out = append(out, Frame(nil, true)...)
		return Response{Status: 200, Headers: hdrs, Body: out}
	}
	h := reg.Unary[spec.Name]
	if h == nil {
		return errorResponse(&RPCError{Code: 5, Message: "method not found"})
	}
	resp, err := h(ctx, req.Body, kind)
	if err != nil {
		return errorResponse(asRPCError(err))
	}
	return Response{Status: 200, Headers: Headers{"Content-Type": []string{ct}}, Body: resp}
}

func errorResponse(err *RPCError) Response {
	return Response{
		Status:  HTTPStatus(err.Code),
		Headers: Headers{"Content-Type": []string{"text/plain"}},
		Body:    []byte(err.Message),
		Error:   err,
	}
}

// ServeNetHTTP adapts Dispatch to a net/http handler. It is the default bridge
// backend for Go. Any other backend can call Dispatch directly.
func ServeNetHTTP(methods []MethodSpec, reg *ServiceRegistry) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		hdrs := goHeaders(r.Header)
		req := Request{
			URL:     r.URL.Path,
			Method:  r.Method,
			Headers: hdrs,
			Body:    body,
		}
		ctx := ContextWithHeaders(r.Context(), hdrs)
		res := Dispatch(ctx, req, methods, reg)
		// write response headers
		for k, vs := range res.Headers {
			w.Header()[k] = vs
		}
		w.WriteHeader(res.Status)
		_, _ = w.Write(res.Body)
	})
}

// Serve is a compatibility alias: returns a net/http handler for the given
// methods + registry (h1, h2c via http.Protocols when the caller configures it).
func Serve(methods []MethodSpec, reg *ServiceRegistry) http.Handler {
	return ServeNetHTTP(methods, reg)
}

func streamContent(ct string) string {
	if ct == "application/json" {
		return "application/connect+json"
	}
	return "application/connect+proto"
}

func writeError(w http.ResponseWriter, err *RPCError) {
	code := HTTPStatus(err.Code)
	w.Header().Set("Content-Type", "text/plain")
	w.Header().Set("connect-code", itoa(err.Code))
	w.Header().Set("connect-error", encodeErr(err.Message))
	w.WriteHeader(code)
	_, _ = w.Write([]byte(err.Message))
}

func encodeErr(msg string) string {
	return base64.StdEncoding.EncodeToString([]byte(msg))
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

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	neg := i < 0
	if neg {
		i = -i
	}
	var b [20]byte
	pos := len(b)
	for i > 0 {
		pos--
		b[pos] = byte('0' + i%10)
		i /= 10
	}
	if neg {
		pos--
		b[pos] = '-'
	}
	return string(b[pos:])
}

// StreamWriter writes framed responses for a server-stream handler, writing
// frames directly to an http.ResponseWriter.
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
