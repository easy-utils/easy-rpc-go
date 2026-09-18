package easyrpc

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"io"
	"strconv"
	"strings"
)

// UnaryHandler decodes request bytes -> response bytes. ctx carries request
// metadata (see HeadersFromContext) and the HandlerContext (trailer channel).
type UnaryHandler func(ctx context.Context, req []byte) (resp []byte, err error)

// StreamHandler serves server-stream; emit(payload,false) sends a frame,
// emit(nil,true) ends. The handler's ctx carries the HandlerContext.
type StreamHandler func(ctx context.Context, req []byte, emit func(payload []byte, end bool) error) error

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

// proto-only content types (spec §2); easy-rpc v2 has no JSON codec.
const (
	ContentTypeUnary  = "application/proto"
	ContentTypeStream = "application/connect+proto"
)

// Dispatch is the ASGI-style pure application. It decodes an RPC request, runs
// the handler, and PUSHES the response into w frame-by-frame. Protocol-agnostic:
// it never imports a specific HTTP runtime.
func Dispatch(ctx context.Context, req Request, methods []MethodSpec, reg *ServiceRegistry, w ResponseWriter) error {
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

	// proto-only content type check (spec §2).
	wantCT := ContentTypeUnary
	if spec.ServerStream {
		wantCT = ContentTypeStream
	}
	gotCT := strings.TrimSpace(strings.ToLower(strings.SplitN(req.Headers.Get("Content-Type"), ";", 2)[0]))
	if gotCT != wantCT {
		return writeErrorStatus(w, &RPCError{Code: 3, Message: "unsupported content-type: expected " + wantCT}, 415)
	}

	// Per-RPC metadata + trailer channel.
	hc := NewHandlerContext(req.Headers)
	ctx = ContextWithHeaders(ctx, req.Headers)
	ctx = ContextWithHandlerContext(ctx, hc)

	timeout := ParseTimeout(req.Headers.Get(HeaderTimeout))
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	if spec.ServerStream {
		h := reg.Stream[spec.Name]
		if h == nil {
			return writeError(w, &RPCError{Code: 5, Message: "method not found"})
		}
		// Stream request body is ENVELOPED: one data frame carrying the single
		// request message (Connect spec). Unframe before dispatch.
		reqBody, ferr := readSingleFrame(req.Body)
		if ferr != nil {
			return writeError(w, ferr)
		}
		// Connect semantics: a server-stream is always HTTP 200; failures ride
		// the END frame, never an HTTP status.
		w.Status(200)
		w.Header(Headers{"Content-Type": []string{ContentTypeStream}})
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
				return w.WriteFrame(Frame(EncodeEndStream(EndStreamMessage{Metadata: hc.Trailers()}), true))
			}
			if wantsGzip && len(p) >= CompressMinBytes {
				if z, err := GzipCompress(p); err == nil {
					return w.WriteFrame(FrameCompressed(z))
				}
			}
			return w.WriteFrame(Frame(p, false))
		}
		if err := h(ctx, reqBody, emit); err != nil {
			re := asRPCError(err)
			if !ended {
				ended = true
				_ = w.WriteFrame(Frame(EncodeEndStream(EndStreamMessage{Code: re.Code, Message: re.Message, Details: re.Details, Metadata: hc.Trailers()}), true))
			}
			return nil
		}
		if !ended {
			_ = w.WriteFrame(Frame(EncodeEndStream(EndStreamMessage{Metadata: hc.Trailers()}), true))
		}
		return nil
	}

	h := reg.Unary[spec.Name]
	if h == nil {
		return writeError(w, &RPCError{Code: 5, Message: "method not found"})
	}
	// Unary: resolve fully before writing so a failure can set a real status.
	resp, err := h(ctx, req.Body)
	if err != nil {
		re := asRPCError(err)
		w.Header(MuxTrailers(Headers{"Content-Type": []string{"application/json"}}, hc.Trailers()))
		w.Status(HTTPStatus(re.Code))
		_ = w.WriteFrame(EncodeErrorJSON(re.Code, re.Message, re.Details))
		return nil
	}
	// Unary gzip (spec §3.5): compress when the client accepts gzip.
	if acceptsGzip(req.Headers[HeaderAcceptEncoding]) && len(resp) >= CompressMinBytes {
		if z, zerr := GzipCompress(resp); zerr == nil {
			w.Header(MuxTrailers(Headers{"Content-Type": []string{ContentTypeUnary}, "Content-Encoding": []string{EncodingGzip}}, hc.Trailers()))
			w.Status(200)
			return w.WriteFrame(z)
		}
	}
	w.Status(200)
	w.Header(MuxTrailers(Headers{"Content-Type": []string{ContentTypeUnary}}, hc.Trailers()))
	return w.WriteFrame(resp)
}

func acceptsGzip(vals []string) bool {
	for _, v := range vals {
		for _, e := range strings.Split(v, ",") {
			if strings.TrimSpace(e) == EncodingGzip {
				return true
			}
		}
	}
	return false
}

// readSingleFrame reads exactly one frame (the enveloped server-stream request
// message). Returns the payload. Errors on truncation / size violation.
func readSingleFrame(body []byte) ([]byte, *RPCError) {
	if len(body) < 5 {
		return nil, &RPCError{Code: 13, Message: "stream request: truncated frame header"}
	}
	flags := body[0]
	length := binary.BigEndian.Uint32(body[1:5])
	if int(length) > DefaultMaxMessageBytes {
		return nil, &RPCError{Code: 8, Message: "frame too large"}
	}
	if len(body) < 5+int(length) {
		return nil, &RPCError{Code: 13, Message: "stream request: truncated frame"}
	}
	payload := body[5 : 5+length]
	if flags&0x01 != 0 {
		z, err := GzipDecompress(payload)
		if err != nil {
			return nil, &RPCError{Code: 13, Message: "stream request: corrupt gzip frame"}
		}
		return z, nil
	}
	return payload, nil
}

// writeError emits a non-200 error response (Connect HTTP status + JSON body).
func writeError(w ResponseWriter, err *RPCError) error {
	return writeErrorStatus(w, err, HTTPStatus(err.Code))
}

func writeErrorStatus(w ResponseWriter, err *RPCError, status int) error {
	w.Status(status)
	w.Header(Headers{
		"Content-Type":  []string{"application/json"},
		"Connect-Code":  []string{strconv.Itoa(err.Code)},
		"Connect-Error": []string{err.Message},
	})
	return w.WriteFrame(EncodeErrorJSON(err.Code, err.Message, err.Details))
}

// EncodeErrorJSON builds a Connect unary error body.
func EncodeErrorJSON(code int, message string, details []ErrorDetail) []byte {
	body := struct {
		Code    string       `json:"code"`
		Message string       `json:"message"`
		Details []wireDetail `json:"details,omitempty"`
	}{Code: CodeToString(code), Message: message, Details: encodeWireDetails(details)}
	b, _ := json.Marshal(body)
	return b
}

// DecodeErrorJSON parses a Connect unary error body; (0, "") when not an error.
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

func asRPCError(err error) *RPCError {
	if err == nil {
		return nil
	}
	if re, ok := err.(*RPCError); ok {
		return re
	}
	return &RPCError{Code: 13, Message: err.Error()}
}

// StreamWriter writes framed responses for a server-stream handler.
type StreamWriter struct {
	w   io.Writer
	end bool
}

// NewStreamWriter wraps w to write framed responses.
func NewStreamWriter(w io.Writer) *StreamWriter { return &StreamWriter{w: w} }

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
