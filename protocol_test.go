package easyrpc

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestFrameRoundtrip(t *testing.T) {
	var buf bytes.Buffer
	buf.Write(Frame([]byte("hello"), false))
	buf.Write(Frame([]byte("world"), true))
	r := bytes.NewReader(buf.Bytes())
	p1, end1, err := ReadFrame(r)
	if err != nil || end1 || string(p1) != "hello" {
		t.Fatalf("frame1 %q %v %v", p1, end1, err)
	}
	p2, end2, err := ReadFrame(r)
	if err != nil || !end2 || string(p2) != "world" {
		t.Fatalf("frame2 %q %v %v", p2, end2, err)
	}
}

func TestUnarySend(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "application/proto")
		_, _ = w.Write(append([]byte("pong:"), body...))
	}))
	defer srv.Close()
	rt := NewNetHTTP(nil)
	resp, err := rt.Send(context.Background(), Request{
		URL: srv.URL + "/easyrpc.conformance.v1.ConformanceService/Echo", Method: "POST", Body: []byte("ping"),
	})
	if err != nil {
		t.Fatal(err)
	}
	b, err := DecodeUnaryBody(resp)
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != "pong:ping" {
		t.Fatalf("got %q", b)
	}
}

func TestServerStream(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/connect+proto")
		sw := NewStreamWriter(w)
		for _, s := range []string{"0", "1", "2"} {
			_ = sw.Write([]byte(s))
		}
		_ = sw.End(0, "")
	}))
	defer srv.Close()
	rt := NewNetHTTP(nil)
	stream, err := rt.OpenStream(context.Background(), Request{
		URL: srv.URL + "/easyrpc.conformance.v1.ConformanceService/Count", Method: "POST", Body: []byte{0, 0, 0, 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	var got []string
	for {
		p, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, string(p))
	}
	if len(got) != 3 || got[0] != "0" || got[2] != "2" {
		t.Fatalf("got %v", got)
	}
}

func TestHTTPStatusMapping(t *testing.T) {
	if HTTPStatus(3) != 400 || HTTPStatus(5) != 404 || HTTPStatus(16) != 401 {
		t.Fatal("bad status mapping")
	}
}

func TestEndStreamErrorJSON(t *testing.T) {
	enc := EncodeEndStream(EndStreamMessage{Code: 5, Message: "nope"})
	if string(enc) != `{"error":{"code":"not_found","message":"nope"}}` {
		t.Fatalf("encode: %s", enc)
	}
	m := DecodeEndStream(enc)
	if m.Code != 5 || m.Message != "nope" {
		t.Fatalf("decode: %+v", m)
	}
	if got := EncodeEndStream(EndStreamMessage{}); len(got) != 0 {
		t.Fatalf("clean end should be empty, got %q", got)
	}
	if m := DecodeEndStream(nil); m.Code != 0 {
		t.Fatalf("clean decode: %+v", m)
	}
}

func TestStreamRecvSurfacesEndError(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/stream-fail", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/connect+proto")
		sw := NewStreamWriter(w)
		_ = sw.Write([]byte{1})
		_ = sw.End(16, "missing bearer token")
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	rt := NewNetHTTP(nil)
	st, err := rt.OpenStream(context.Background(), Request{URL: srv.URL + "/v1/stream-fail", Method: "POST", Body: []byte{}})
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if _, err := st.Recv(); err != nil {
		t.Fatal("first frame:", err)
	}
	_, err = st.Recv()
	re, ok := err.(*RPCError)
	if !ok || re.Code != 16 {
		t.Fatalf("expected RPCError 16, got %v", err)
	}
}

func TestTimeoutHelpers(t *testing.T) {
	if ParseTimeout("") != 0 || ParseTimeout("abc") != 0 || ParseTimeout("0") != 0 {
		t.Fatalf("parse zero cases")
	}
	if ParseTimeout("250") != 250*time.Millisecond {
		t.Fatalf("parse 250")
	}
	req := WithTimeout(Request{URL: "/x"}, 300*time.Millisecond)
	if req.Headers.Get(HeaderTimeout) != "300" {
		t.Fatalf("with timeout header = %q", req.Headers.Get(HeaderTimeout))
	}
}

func TestInterceptorTransport(t *testing.T) {
	var got Headers
	base := &captureTransport{onSend: func(req Request) { got = req.Headers }}
	it := WithInterceptors(base, MetadataInterceptor(Headers{"x-test": {"abc"}}), TimeoutInterceptor(250*time.Millisecond))
	_, _ = it.Send(context.Background(), Request{URL: "/x", Method: "POST"})
	if got.Get("x-test") != "abc" || got.Get(HeaderTimeout) != "250" {
		t.Fatalf("interceptors not applied: %+v", got)
	}
}

type captureTransport struct{ onSend func(Request) }

func (c *captureTransport) Send(_ context.Context, req Request) (Response, error) {
	if c.onSend != nil {
		c.onSend(req)
	}
	return Response{Status: 200}, nil
}
func (c *captureTransport) OpenStream(context.Context, Request) (Stream, error) { return nil, nil }
