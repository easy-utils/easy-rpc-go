package easyrpc

import (
	"context"
	"time"
)

// Mode selects the adapter (transport) for a client.
type Mode string

const (
	// ModeAuto negotiates h3 -> h2/h2c -> h1 when the optional h3 submodule is
	// linked; otherwise h1+h2/h2c via net/http.
	ModeAuto Mode = "auto"
	// ModeStd is the net/http adapter (h1 + h2/h2c).
	ModeStd Mode = "std"
)

// ConnectOptions configures the composition root.
type ConnectOptions struct {
	BaseURL string
	// Bearer token attached to every call as metadata.
	Token string
	Mode  Mode
	// Per-call deadline (0 = none).
	Timeout time.Duration
	// Extra interceptors, applied after the built-ins (closest to the adapter).
	Interceptors []Interceptor
}

// Connect builds a ready-to-use Transport: adapter(Mode) wrapped with the
// built-in metadata + deadline interceptors (when configured) and any user
// interceptors. Swapping Mode leaves the interceptors unchanged.
func Connect(opts ConnectOptions) Transport {
	mode := opts.Mode
	if mode == "" {
		mode = ModeAuto
	}
	var inner Transport
	switch mode {
	case ModeAuto:
		inner = NewTransport(RealmAuto)
	default:
		inner = NewTransport(RealmStd)
	}
	// Prefix relative paths with the base URL.
	if opts.BaseURL != "" {
		inner = &baseURLTransport{rt: inner, base: opts.BaseURL}
	}

	var ics []Interceptor
	if opts.Token != "" {
		ics = append(ics, MetadataInterceptor(Headers{"Authorization": {"Bearer " + opts.Token}}))
	}
	if opts.Timeout > 0 {
		ics = append(ics, DeadlineInterceptor(opts.Timeout))
	}
	ics = append(ics, opts.Interceptors...)
	if len(ics) == 0 {
		return inner
	}
	return WithInterceptors(inner, ics...)
}

// baseURLTransport prepends BaseURL to relative request URLs.
type baseURLTransport struct {
	rt   Transport
	base string
}

func (b *baseURLTransport) prepend(u string) string {
	if len(u) >= 7 && (u[:7] == "http://" || (len(u) >= 8 && u[:8] == "https://")) {
		return u
	}
	return b.base + u
}

func (b *baseURLTransport) Send(ctx context.Context, req Request) (Response, error) {
	req.URL = b.prepend(req.URL)
	return b.rt.Send(ctx, req)
}

func (b *baseURLTransport) OpenStream(ctx context.Context, req Request) (Stream, error) {
	req.URL = b.prepend(req.URL)
	return b.rt.OpenStream(ctx, req)
}
