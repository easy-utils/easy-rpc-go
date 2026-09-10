// Package server contains the net/http server adapter for easy-rpc. The core
// easyrpc package is transport-agnostic (client bridges + pure dispatch); this
// package wires Dispatch to the Go standard HTTP server. Import it only when
// running an easy-rpc server.
package server

import (
	"io"
	"net/http"

	easyrpc "github.com/easy-utils/easy-rpc-go"
)

// ServeNetHTTP adapts easyrpc.Dispatch to a net/http handler.
func ServeNetHTTP(methods []easyrpc.MethodSpec, reg *easyrpc.ServiceRegistry) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		hdrs := easyrpc.GoHeaders(r.Header)
		req := easyrpc.Request{
			URL:     r.URL.Path,
			Method:  r.Method,
			Headers: hdrs,
			Body:    body,
		}
		ctx := easyrpc.ContextWithHeaders(r.Context(), hdrs)
		res := easyrpc.Dispatch(ctx, req, methods, reg)
		for k, vs := range res.Headers {
			w.Header()[k] = vs
		}
		w.WriteHeader(res.Status)
		_, _ = w.Write(res.Body)
	})
}

// Serve is a compatibility alias for ServeNetHTTP.
func Serve(methods []easyrpc.MethodSpec, reg *easyrpc.ServiceRegistry) http.Handler {
	return ServeNetHTTP(methods, reg)
}
