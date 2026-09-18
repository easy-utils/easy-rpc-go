package server_test

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	easyrpc "github.com/easy-utils/easy-rpc-go"
	easyrpcserver "github.com/easy-utils/easy-rpc-go/server"
)

func TestStreamIsIncremental(t *testing.T) {
	reg := easyrpc.NewServiceRegistry()
	reg.Stream["Slow"] = func(ctx context.Context, req []byte, emit func([]byte, bool) error) error {
		for i := 0; i < 3; i++ {
			if err := emit([]byte{byte(i)}, false); err != nil {
				return err
			}
			time.Sleep(200 * time.Millisecond)
		}
		return emit(nil, true)
	}
	methods := []easyrpc.MethodSpec{{Path: "/t.Slow", Name: "Slow", ServerStream: true}}
	srv := httptest.NewServer(easyrpcserver.ServeNetHTTP(methods, reg))
	defer srv.Close()

	start := time.Now()
	var firstAt time.Duration
	req, _ := http.NewRequest("POST", srv.URL+"/t.Slow", bytes.NewReader(nil))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	buf := make([]byte, 4096)
	for {
		n, err := resp.Body.Read(buf)
		if n > 0 && firstAt == 0 {
			firstAt = time.Since(start)
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	if firstAt > 150*time.Millisecond {
		t.Fatalf("first frame arrived at %v — stream is buffered", firstAt)
	}
	t.Logf("PASS: first frame at %v", firstAt)
}
