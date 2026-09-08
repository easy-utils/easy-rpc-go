package easyrpc

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
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
