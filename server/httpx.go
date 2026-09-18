// Package server contains the net/http server adapter for easy-rpc. The core
// easyrpc package is transport-agnostic (client bridges + push dispatch); this
// package wires Dispatch to the Go standard HTTP server. Import it only when
// running an easy-rpc server.
//
// Streaming is real: every frame is written and flushed to the client as it is
// produced, so server-stream RPCs (Prompt/WatchSession/WatchSessions) reach the
// client incrementally instead of being buffered until the handler returns.
package server

import (
	"bufio"
	"context"
	"io"
	"net"
	"net/http"

	easyrpc "github.com/easy-utils/easy-rpc-go"
)

// httpWriter is a push-based easyrpc.ResponseWriter over an http.ResponseWriter.
// The HTTP status + headers are written lazily on the first frame (so an error
// before any body can still set a real status), and every frame is flushed.
type httpWriter struct {
	w       http.ResponseWriter
	status  int
	headers easyrpc.Headers
	started bool
}

func (h *httpWriter) Status(code int) { h.status = code }

func (h *httpWriter) Header(hdrs easyrpc.Headers) {
	if h.headers == nil {
		h.headers = easyrpc.Headers{}
	}
	for k, vs := range hdrs {
		h.headers[k] = append(h.headers[k], vs...)
	}
}

func (h *httpWriter) start() {
	if h.started {
		return
	}
	h.started = true
	status := h.status
	if status == 0 {
		status = 200
	}
	for k, vs := range h.headers {
		for _, v := range vs {
			h.w.Header().Add(k, v)
		}
	}
	h.w.WriteHeader(status)
}

func (h *httpWriter) WriteFrame(payload []byte) error {
	h.start()
	if _, err := h.w.Write(payload); err != nil {
		return err
	}
	h.flush()
	return nil
}

func (h *httpWriter) flush() {
	if f, ok := h.w.(http.Flusher); ok {
		f.Flush()
		return
	}
	// Some wrappers don't implement http.Flusher directly; unwrap once.
	if u, ok := h.w.(interface{ Unwrap() http.ResponseWriter }); ok {
		if f, ok := u.Unwrap().(http.Flusher); ok {
			f.Flush()
		}
	}
}

// ServeNetHTTP adapts easyrpc.Dispatch to a net/http handler. Stream frames are
// flushed immediately (real streaming); unary responses are written once.
func ServeNetHTTP(methods []easyrpc.MethodSpec, reg *easyrpc.ServiceRegistry) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		hdrs := easyrpc.GoHeaders(r.Header)
		hdrs.Set(":method", r.Method)
		req := easyrpc.Request{
			URL:     r.URL.Path,
			Headers: hdrs,
			Body:    body,
		}
		ctx := easyrpc.ContextWithHeaders(r.Context(), hdrs)
		// Apply the Connect request deadline to the context so handlers can
		// observe cancellation; the adapter also bounds the write loop.
		if d := easyrpc.ParseTimeout(hdrs.Get(easyrpc.HeaderTimeout)); d > 0 {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, d)
			defer cancel()
		}
		hw := &httpWriter{w: w}
		if err := easyrpc.Dispatch(ctx, req, methods, reg, hw); err != nil && !hw.started {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})
}

// Serve is a compatibility alias for ServeNetHTTP.
func Serve(methods []easyrpc.MethodSpec, reg *easyrpc.ServiceRegistry) http.Handler {
	return ServeNetHTTP(methods, reg)
}

// Ensure bufio/net stay referenced for adapters that may need conn hijacking.
var _ = bufio.NewReader
var _ net.Conn
