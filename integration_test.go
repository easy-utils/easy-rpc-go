package easyrpc

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestConformanceUnaryAndStream(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/easyrpc.conformance.v1.ConformanceService/Echo", func(w http.ResponseWriter, r *http.Request) {
		body := readAllReq(r)
		w.Header().Set("Content-Type", "application/proto")
		_, _ = w.Write(append([]byte("echo:"), body...))
	})
	mux.HandleFunc("/easyrpc.conformance.v1.ConformanceService/Count", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/connect+proto")
		sw := NewStreamWriter(w)
		for i := 0; i < 3; i++ {
			_ = sw.Write([]byte{byte(i)})
		}
		_ = sw.End(0, "")
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	rt := NewNetHTTP(nil)
	resp, err := rt.Send(context.Background(), Request{URL: srv.URL + "/easyrpc.conformance.v1.ConformanceService/Echo", Body: []byte("hi")})
	if err != nil {
		t.Fatal(err)
	}
	if string(resp.Body) != "echo:hi" {
		t.Fatalf("echo %q", resp.Body)
	}

	st, err := rt.OpenStream(context.Background(), Request{URL: srv.URL + "/easyrpc.conformance.v1.ConformanceService/Count", Body: []byte{0}})
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	var n int
	for {
		if _, err := st.Recv(); err != nil {
			break
		}
		n++
	}
	if n != 3 {
		t.Fatalf("count %d", n)
	}
}

func readAllReq(r *http.Request) []byte {
	var out []byte
	b := make([]byte, 4096)
	for {
		n, err := r.Body.Read(b)
		out = append(out, b[:n]...)
		if err != nil {
			break
		}
	}
	return out
}
