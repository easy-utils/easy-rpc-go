// Package h3 provides the optional HTTP/3 bridge for easy-rpc-go. Link it to
// enable RealmAuto (h3 -> h2/h2c -> h1). It carries the quic-go dependency,
// keeping the root module transport-agnostic.
package h3

import (
	"bytes"
	"context"
	"crypto/tls"
	"io"
	"net/http"

	"github.com/quic-go/quic-go/http3"
	"github.com/easy-utils/easy-rpc-go"
)

func init() {
	easyrpc.RegisterAuto(func() easyrpc.Transport { return NewAutoHTTP() })
}

// HTTP3 is the quic-go/http3 bridge implementing Transport (HTTP/3 only).
type HTTP3 struct {
	client *http.Client
	tr     *http3.Transport
}

func NewHTTP3(client *http.Client, tlsConfig *tls.Config) *HTTP3 {
	tr := &http3.Transport{TLSClientConfig: tlsConfig}
	if client == nil {
		client = &http.Client{Transport: tr}
	}
	return &HTTP3{client: client, tr: tr}
}

func NewHTTP3Default() *HTTP3 { return NewHTTP3(nil, nil) }

func (b *HTTP3) Send(ctx context.Context, req easyrpc.Request) (easyrpc.Response, error) {
	httpReq, err := http.NewRequestWithContext(ctx, req.Method, req.URL, bytes.NewReader(req.Body))
	if err != nil {
		return easyrpc.Response{}, err
	}
	httpReq.Header = easyrpc.HeaderToHTTP(req.Headers)
	if httpReq.Header.Get("Content-Type") == "" {
		httpReq.Header.Set("Content-Type", "application/proto")
	}
	resp, err := b.client.Do(httpReq)
	if err != nil {
		return easyrpc.Response{}, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return easyrpc.Response{}, err
	}
	return easyrpc.Response{Status: resp.StatusCode, Headers: easyrpc.Headers(req.Headers), Body: body}, nil
}

func (b *HTTP3) OpenStream(ctx context.Context, req easyrpc.Request) (easyrpc.Stream, error) {
	httpReq, err := http.NewRequestWithContext(ctx, req.Method, req.URL, bytes.NewReader(req.Body))
	if err != nil {
		return nil, err
	}
	httpReq.Header = easyrpc.HeaderToHTTP(req.Headers)
	httpReq.Header.Set("Content-Type", "application/connect+proto")
	resp, err := b.client.Do(httpReq)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 300 {
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		return nil, &easyrpc.RPCError{Code: easyrpc.ConnectFromStatus(resp.StatusCode), Message: string(body)}
	}
	return easyrpc.NewStream(resp), nil
}

func (b *HTTP3) Close() error {
	if b.tr != nil {
		b.tr.Close()
	}
	return nil
}

func (b *HTTP3) WithTLS(cfg *tls.Config) *HTTP3 {
	if b.tr == nil {
		return &HTTP3{tr: &http3.Transport{TLSClientConfig: cfg},
			client: &http.Client{Transport: &http3.Transport{TLSClientConfig: cfg}}}
	}
	b.tr.TLSClientConfig = cfg
	return b
}

type AutoHTTP struct {
	std *easyrpc.NetHTTP
	h3  *HTTP3
}

func NewAutoHTTP() *AutoHTTP {
	return &AutoHTTP{std: easyrpc.NewNetHTTP(nil), h3: NewHTTP3Default()}
}

func (a *AutoHTTP) Send(ctx context.Context, req easyrpc.Request) (easyrpc.Response, error) {
	if isH3(req.URL) {
		if resp, err := a.h3.Send(ctx, req); err == nil {
			return resp, nil
		}
	}
	return a.std.Send(ctx, req)
}

func (a *AutoHTTP) OpenStream(ctx context.Context, req easyrpc.Request) (easyrpc.Stream, error) {
	if isH3(req.URL) {
		if st, err := a.h3.OpenStream(ctx, req); err == nil {
			return st, nil
		}
	}
	return a.std.OpenStream(ctx, req)
}

func (a *AutoHTTP) Close() error {
	if a.h3 != nil {
		return a.h3.Close()
	}
	return nil
}

func isH3(u string) bool { return len(u) >= 8 && u[:8] == "https://" }
