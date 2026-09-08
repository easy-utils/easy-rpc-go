package easyrpc

import (
	"context"
	"io"
	"net/http"
	"strings"
)

// NetHTTP is the net/http bridge implementing Transport. It speaks Connect
// wire: unary uses direct body; server-stream uses the framing protocol.
// `http.Protocols` is used so http:// can do h2c prior-knowledge when desired.
//
// This is the reference bridge for Go (official runtime). It is NOT part of
// the core protocol logic; it only adapts net/http to the Transport interface.
type NetHTTP struct {
	client *http.Client
}

// NewNetHTTP returns a bridge over the given client (nil => http.DefaultClient).
func NewNetHTTP(client *http.Client) *NetHTTP {
	if client == nil {
		client = http.DefaultClient
	}
	return &NetHTTP{client: client}
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
	return Response{
		Status:  resp.StatusCode,
		Headers: hdrs,
		Body:    body,
	}, nil
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

func goHeaders(h http.Header) Headers {
	nh := make(Headers)
	for k, v := range h {
		nh[k] = v
	}
	return nh
}
