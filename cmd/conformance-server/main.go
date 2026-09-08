package main

import (
	"io"
	"net/http"

	easyrpc "github.com/easy-utils/easy-rpc-go"
	cv1 "github.com/easy-utils/easy-rpc-go/easyrpc/conformance/v1"

	"google.golang.org/protobuf/proto"
)

// Both REST (google.api.http) and gRPC-style paths are served so any client
// (TS generated with either path style) can interoperate.
func main() {
	mux := http.NewServeMux()

	unaryEcho := func(w http.ResponseWriter, r *http.Request) {
		req, _ := io.ReadAll(r.Body)
		var in cv1.EchoRequest
		_ = proto.Unmarshal(req, &in)
		w.Header().Set("Content-Type", "application/proto")
		out := &cv1.EchoResponse{Output: "echo:" + in.Input}
		b, _ := proto.Marshal(out)
		_, _ = w.Write(b)
	}
	streamCount := func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/connect+proto")
		sw := easyrpc.NewStreamWriter(w)
		for i := 0; i < 3; i++ {
			out := &cv1.CountResponse{Index: int32(i)}
			b, _ := proto.Marshal(out)
			_ = sw.Write(b)
		}
		_ = sw.End(0, "")
	}
	health := func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/proto")
		b, _ := proto.Marshal(&cv1.HealthResponse{Ok: true, Name: "conformance"})
		_, _ = w.Write(b)
	}

	// REST paths
	mux.HandleFunc("/v1/health", health)
	mux.HandleFunc("/v1/echo", unaryEcho)
	mux.HandleFunc("/v1/count", streamCount)
	// gRPC-style paths (fallback)
	base := "/easyrpc.conformance.v1.ConformanceService/"
	mux.HandleFunc(base+"Health", health)
	mux.HandleFunc(base+"Echo", unaryEcho)
	mux.HandleFunc(base+"Count", streamCount)

	_ = http.ListenAndServe("127.0.0.1:18888", mux)
}
