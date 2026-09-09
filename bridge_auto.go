package easyrpc

import (
	"context"
)

// Realm selects how much protocol coverage a client Transport wants.
type Realm string

const (
	// RealmStd is the minimal-dependency realm: HTTP/1 + HTTP/2 (h2, h2c) via
	// net/http. No QUIC/HTTP3 dependency is linked.
	RealmStd Realm = "std"
	// RealmAuto is the full-featured realm: HTTP/1 + HTTP/2 + HTTP/3 (QUIC via
	// quic-go/http3), negotiating h3 -> h2 -> h1 internally.
	RealmAuto Realm = "auto"
)

// AutoHTTP is the full-featured Transport. It tries HTTP/3 first (when the
// scheme is https:/quic), then falls back to HTTP/2/h2c, then HTTP/1. It wraps
// a NetHTTP (net/http) transport for h1/h2/h2c and an HTTP3 (quic-go) transport
// for h3, so the single returned Transport covers all three.
type AutoHTTP struct {
	std  *NetHTTP
	h3   *HTTP3
}

// NewAutoHTTP builds a Transport that negotiates h3 -> h2/h2c -> h1.
func NewAutoHTTP() *AutoHTTP {
	return &AutoHTTP{
		std: NewNetHTTP(nil),
		h3:  NewHTTP3Default(),
	}
}

// Send implements Transport.Send, preferring HTTP/3 when the URL is https://.
func (a *AutoHTTP) Send(ctx context.Context, req Request) (Response, error) {
	if isH3(req.URL) {
		if resp, err := a.h3.Send(ctx, req); err == nil {
			return resp, nil
		}
		// fall through to h2/h1
	}
	return a.std.Send(ctx, req)
}

// OpenStream implements Transport.OpenStream, preferring HTTP/3 for https://.
func (a *AutoHTTP) OpenStream(ctx context.Context, req Request) (Stream, error) {
	if isH3(req.URL) {
		if st, err := a.h3.OpenStream(ctx, req); err == nil {
			return st, nil
		}
	}
	return a.std.OpenStream(ctx, req)
}

// Close releases the HTTP/3 transport.
func (a *AutoHTTP) Close() error {
	if a.h3 != nil {
		return a.h3.Close()
	}
	return nil
}

// NewTransport returns a Transport for the given realm. RealmAuto links the
// quic-go/http3 dependency; RealmStd only links net/http.
func NewTransport(realm Realm) Transport {
	if realm == RealmAuto {
		return NewAutoHTTP()
	}
	return NewNetHTTP(nil)
}

func isH3(u string) bool {
	return len(u) >= 8 && (u[:8] == "https://")
}
