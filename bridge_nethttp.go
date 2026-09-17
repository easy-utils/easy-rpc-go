package easyrpc

import (
	"context"
	"crypto/tls"
	"io"
	"net/http"
	"strings"
)

// ProtocolPrefs selects which HTTP protocols a bridge negotiates.
// Zero value = HTTP/1 + unencrypted HTTP/2 (h2c prior-knowledge). Bridges use
// Go's http.Protocols so a single client/server can serve h1 + h2 + h2c.
type ProtocolPrefs struct {
	HTTP1 bool // HTTP/1.x (default true)
	HTTP2 bool // HTTP/2 over TLS (ALPN "h2")
	H2C   bool // unencrypted HTTP/2 (h2c prior knowledge)
	// TLSClientConfig, when set, is used by the client for https:// URLs
	// (custom CA / client certs / InsecureSkipVerify for self-signed CA).
	TLSClientConfig *tls.Config
}

// DefaultProtocols returns HTTP/1 + h2c (the negotiation-friendly default
// that works in cleartext interop without requiring TLS setup).
func DefaultProtocols() ProtocolPrefs {
	return ProtocolPrefs{HTTP1: true, H2C: true}
}

// protocols converts ProtocolPrefs to a *http.Protocols.
func (p ProtocolPrefs) protocols() *http.Protocols {
	pr := &http.Protocols{}
	if p.HTTP1 || (!p.HTTP1 && !p.HTTP2 && !p.H2C) {
		pr.SetHTTP1(true)
	}
	if p.HTTP2 {
		pr.SetHTTP2(true)
	}
	if p.H2C || (!p.HTTP1 && !p.HTTP2 && !p.H2C) {
		pr.SetUnencryptedHTTP2(true)
	}
	return pr
}

// NetHTTP is the net/http bridge implementing Transport. It speaks Connect
// wire: unary uses direct body; server-stream uses the framing protocol.
// `http.Protocols` is used so http:// can do h2c prior-knowledge when desired.
//
// This is the reference bridge for Go (official runtime). It is NOT part of
// the core protocol logic; it only adapts net/http to the Transport interface.
type NetHTTP struct {
	client *http.Client
	prefs  ProtocolPrefs
}

// NewNetHTTP returns a bridge over the given client (nil => http.DefaultClient).
// If client is non-nil its transport is respected; otherwise a transport with
// the given/negotiiting protocols is built.
func NewNetHTTP(client *http.Client) *NetHTTP {
	return NewNetHTTPWith(DefaultProtocols(), client)
}

// NewNetHTTPWith returns a bridge using the given protocol preferences and
// optional client (nil => built from the preferences, incl. TLS config for
// self-signed CA).
func NewNetHTTPWith(prefs ProtocolPrefs, client *http.Client) *NetHTTP {
	if client == nil {
		tr := &http.Transport{Protocols: prefs.protocols()}
		tr.TLSClientConfig = prefs.TLSClientConfig
		client = &http.Client{Transport: tr}
	}
	return &NetHTTP{client: client, prefs: prefs}
}

// Send implements Transport.Send for unary.
func (b *NetHTTP) Send(ctx context.Context, req Request) (Response, error) {
	httpReq, err := http.NewRequestWithContext(ctx, req.Method, req.URL, strings.NewReader(string(req.Body)))
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
	out := Response{
		Status:  resp.StatusCode,
		Headers: hdrs,
		Body:    body,
	}
	// Non-2xx: reconstruct the exact Connect error from the headers (the HTTP
	// status alone is lossy — several Connect codes share a status).
	if resp.StatusCode >= 300 {
		if e := statusFromHeader(hdrs); e != nil {
			out.Error = e
		} else {
			out.Error = &RPCError{Code: ConnectFromStatus(resp.StatusCode), Message: string(body)}
		}
	}
	return out, nil
}

// OpenStream implements Transport.OpenStream for server-stream.
func (b *NetHTTP) OpenStream(ctx context.Context, req Request) (Stream, error) {
	httpReq, err := http.NewRequestWithContext(ctx, req.Method, req.URL, strings.NewReader(string(req.Body)))
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

// httpStream adapts an http.Response body to the Stream interface.
type httpStream struct {
	resp *http.Response
	body io.ReadCloser
}

func (s *httpStream) Recv() ([]byte, error) {
	payload, end, err := ReadFrame(s.body)
	if err != nil {
		if err == io.EOF {
			return nil, io.EOF
		}
		return nil, err
	}
	if end {
		// A non-empty END payload is a Connect end-stream error.
		if m := DecodeEndStream(payload); m.Code != 0 {
			return nil, &RPCError{Code: m.Code, Message: m.Message}
		}
		return nil, io.EOF
	}
	return payload, nil
}

func (s *httpStream) Cancel() {
	if s.body != nil {
		_ = s.body.Close()
	}
}

func (s *httpStream) Close() error {
	return s.body.Close()
}

func httpHeader(h Headers) http.Header {
	nh := make(http.Header)
	for k, v := range h {
		nh[k] = v
	}
	return nh
}

// GoHeaders converts net/http.Header to easy-rpc Headers.
func GoHeaders(h http.Header) Headers { return goHeaders(h) }

func goHeaders(h http.Header) Headers {
	nh := make(Headers)
	for k, v := range h {
		nh[k] = v
	}
	return nh
}

// HeaderToHTTP converts easy-rpc Headers to net/http.Header.
func HeaderToHTTP(h Headers) http.Header { return httpHeader(h) }

// NetHTTPStream wraps a response body as an easy-rpc Stream.
type NetHTTPStream struct{ *httpStream }

// NewStream wraps a *http.Response into an easy-rpc Stream (used by optional
// bridges, e.g. h3, that fetch a *http.Response themselves).
func NewStream(resp *http.Response) Stream { return &httpStream{resp: resp, body: resp.Body} }
