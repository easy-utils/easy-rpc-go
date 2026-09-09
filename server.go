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

// contentKind picks "proto" or "json" from the Content-Type/Accept header,
// defaulting to proto.
func contentKind(r *http.Request) string {
	ct := r.Header.Get("Content-Type")
	if strings.HasPrefix(ct, "application/json") {
		return "json"
	}
	accept := r.Header.Get("Accept")
	if strings.HasPrefix(accept, "application/json") {
		return "json"
	}
	return "proto"
}

// Serve builds an http.Handler dispatching by method specs to a service
// registry, honoring REST paths and the proto/json content negotiation.
func Serve(methods []MethodSpec, reg *ServiceRegistry) http.Handler {
	mux := http.NewServeMux()
	for _, m := range methods {
		path := m.Path
		if !strings.HasPrefix(path, "/") {
			path = "/" + path
		}
		spec := m
		mux.HandleFunc(path, func(w http.ResponseWriter, r *http.Request) {
			body, _ := io.ReadAll(r.Body)
			kind := contentKind(r)
			// Attach incoming metadata headers to the handler context so a
			// service can read auth/tracing headers.
			ctx := ContextWithHeaders(r.Context(), goHeaders(r.Header))
			ct := "application/proto"
			if kind == "json" {
				ct = "application/json"
			}
			if spec.ServerStream {
				h := reg.Stream[spec.Name]
				if h == nil {
					writeError(w, &RPCError{Code: 5, Message: "method not found"})
					return
				}
				w.Header().Set("Content-Type", streamContent(ct))
				sw := NewStreamWriter(w)
				err := h(ctx, body, kind, func(p []byte, end bool) error {
					if end {
						return sw.End(0, "")
					}
					return sw.Write(p)
				})
				if err != nil {
					_ = sw.End(13, err.Error())
					return
				}
				_ = sw.End(0, "")
				return
			}
			h := reg.Unary[spec.Name]
			if h == nil {
				writeError(w, &RPCError{Code: 5, Message: "method not found"})
				return
			}
			resp, err := h(ctx, body, kind)
			if err != nil {
				writeError(w, asRPCError(err))
				return
			}
			w.Header().Set("Content-Type", ct)
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(resp)
		})
	}
	return mux
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
