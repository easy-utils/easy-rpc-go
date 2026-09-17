package easyrpc

import (
	"context"
	"testing"
	"time"
)

type fakeAdapter struct{ seen Headers }

func (f *fakeAdapter) Send(_ context.Context, req Request) (Response, error) {
	f.seen = req.Headers
	return Response{Status: 200}, nil
}
func (f *fakeAdapter) OpenStream(_ context.Context, _ Request) (Stream, error) {
	panic("unused")
}

// Injection parity: Connect(Adapter:...) wraps a custom adapter with the SAME
// built-in interceptors as a mode-picked one.
func TestConnectInjectsCustomAdapter(t *testing.T) {
	fake := &fakeAdapter{}
	rt := Connect(ConnectOptions{BaseURL: "http://x", Token: "sekret", Timeout: 1500 * time.Millisecond, Adapter: fake})
	if _, err := rt.Send(context.Background(), Request{URL: "/x"}); err != nil {
		t.Fatal(err)
	}
	if got := fake.seen.Get("Authorization"); got != "Bearer sekret" {
		t.Errorf("metadata missing on injected adapter: %q", got)
	}
	if got := fake.seen.Get(HeaderTimeout); got != "1500" {
		t.Errorf("deadline header missing on injected adapter: %q", got)
	}
}

func TestConnectInjectedAdapterUntouchedWithoutOpts(t *testing.T) {
	fake := &fakeAdapter{}
	rt := Connect(ConnectOptions{Adapter: fake})
	if _, err := rt.Send(context.Background(), Request{URL: "/x", Headers: Headers{"a": {"b"}}}); err != nil {
		t.Fatal(err)
	}
	if _, ok := fake.seen["Authorization"]; ok {
		t.Errorf("unexpected auth header")
	}
	if fake.seen.Get("a") != "b" {
		t.Errorf("call headers must pass through untouched")
	}
}
