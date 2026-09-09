package easyrpc

import (
	"bytes"
	"context"
	"crypto/tls"
	"io"
	"net/http"

	"github.com/quic-go/quic-go/http3"
)

// HTTP3 is the quic-go/http3 bridge implementing Transport (HTTP/3 only).
// It does NOT cover HTTP/1 or HTTP/2 — those are provided by NetHTTP (net/http).
// Use NewAutoHTTP to get a single Transport that negotiates h3 -> h2 -> h1.
type HTTP3 struct {
	client *http.Client
	tr     *http3.Transport
}

// NewHTTP3 returns an HTTP/3 bridge over the given http.Client (nil => built
// from TLSClientConfig for custom CA / self-signed certs).
func NewHTTP3(client *http.Client, tlsConfig *tls.Config) *HTTP3 {
	tr := &http3.Transport{TLSClientConfig: tlsConfig}
	if client == nil {
		client = &http.Client{Transport: tr}
	}
	return &HTTP3{client: client, tr: tr}
}

// NewHTTP3Default returns an HTTP/3 bridge with default TLS config.
func NewHTTP3Default() *HTTP3 {
	return NewHTTP3(nil, nil)
}

// Send implements Transport.Send for unary over HTTP/3.
func (b *HTTP3) Send(ctx context.Context, req Request) (Response, error) {
	httpReq, err := http.NewRequestWithContext(ctx, req.Method, req.URL, bytes.NewReader(req.Body))
	if err != nil {
		return Response{}, err
	}
	httpReq.Header = httpHeader(req.Headers)
	if httpReq.Header.Get("Content-Type") == "" {
		httpReq.Header.Set("Content-Type", "application/proto")
	}
	resp, err := b.client.Do(httpReq)
	if err != nil {
		return Response{}, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return Response{}, err
	}
	hdrs := goHeaders(resp.Header)
	hdrs.Set("proto", resp.Proto)
	return Response{Status: resp.StatusCode, Headers: hdrs, Body: body}, nil
}

// OpenStream implements Transport.OpenStream for server-stream over HTTP/3.
func (b *HTTP3) OpenStream(ctx context.Context, req Request) (Stream, error) {
	httpReq, err := http.NewRequestWithContext(ctx, req.Method, req.URL, bytes.NewReader(req.Body))
	if err != nil {
		return nil, err
	}
	httpReq.Header = httpHeader(req.Headers)
	httpReq.Header.Set("Content-Type", "application/connect+proto")
	resp, err := b.client.Do(httpReq)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 300 {
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		return nil, &RPCError{Code: connectFromStatus(resp.StatusCode), Message: string(body)}
	}
	return &httpStream{resp: resp, body: resp.Body}, nil
}

// Close releases the underlying QUIC transport.
func (b *HTTP3) Close() error {
	if b.tr != nil {
		b.tr.Close()
	}
	return nil
}

// WithTLS returns a copy configured with a custom TLS client config (for
// self-signed CA / mutual TLS). Only meaningful when the bridge owns its client.
func (b *HTTP3) WithTLS(cfg *tls.Config) *HTTP3 {
	if b.tr == nil {
		return &HTTP3{tr: &http3.Transport{TLSClientConfig: cfg},
			client: &http.Client{Transport: &http3.Transport{TLSClientConfig: cfg}}}
	}
	b.tr.TLSClientConfig = cfg
	return b
}
