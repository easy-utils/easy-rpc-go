package main

import (
	"context"
	"net/http"
	"os"

	easyrpc "github.com/easy-utils/easy-rpc-go"
	cv1 "github.com/easy-utils/easy-rpc-go/easyrpc/conformance/v1"
	easyrpcserver "github.com/easy-utils/easy-rpc-go/server"
)

type impl struct{}

func (impl) Health(c context.Context, _ *cv1.HealthRequest) (*cv1.HealthResponse, error) {
	return &cv1.HealthResponse{Ok: true, Name: "conformance"}, nil
}
func (impl) Echo(c context.Context, in *cv1.EchoRequest) (*cv1.EchoResponse, error) {
	return &cv1.EchoResponse{Output: "echo:" + in.Input}, nil
}
func (impl) Count(c context.Context, in *cv1.CountRequest, emit func(*cv1.CountResponse) error) error {
	for i := 0; i < 3; i++ {
		if err := emit(&cv1.CountResponse{Index: int32(i)}); err != nil {
			return err
		}
	}
	return nil
}
func (impl) Fail(c context.Context, in *cv1.FailRequest) (*cv1.FailResponse, error) {
	return &cv1.FailResponse{Ok: in.Message == ""}, nil
}

// StreamFail emits `emit_before` frames, then ends the stream with a Connect
// end-stream error carrying `code`/`message` — the client MUST surface it.
func (impl) StreamFail(c context.Context, in *cv1.StreamFailRequest, emit func(*cv1.StreamFailResponse) error) error {
	for i := int32(0); i < in.EmitBefore; i++ {
		if err := emit(&cv1.StreamFailResponse{Index: i}); err != nil {
			return err
		}
	}
	return &easyrpc.RPCError{Code: int(in.Code), Message: in.Message}
}

// EchoMeta mirrors selected request metadata into the response map.
func (impl) EchoMeta(c context.Context, in *cv1.EchoMetaRequest) (*cv1.EchoMetaResponse, error) {
	md := map[string]string{}
	if h := easyrpc.HeadersFromContext(c); h != nil {
		for _, k := range []string{"x-test", "authorization"} {
			if v := h.Get(k); v != "" {
				md[k] = v
			}
		}
	}
	return &cv1.EchoMetaResponse{Input: in.Input, Meta: md}, nil
}

// Big returns a response whose serialized size is >= in.Size bytes.
func (impl) Big(c context.Context, in *cv1.BigRequest) (*cv1.BigResponse, error) {
	return &cv1.BigResponse{Size: in.Size}, nil
}

func port() string {
	p := os.Getenv("PORT")
	if p == "" {
		p = "18888"
	}
	return p
}

func main() {
	methods := cv1.ConformanceService_Methods()
	reg := cv1.RegisterConformanceServiceService(impl{})

	// Serve h1 + h2c on the same port (protocol negotiation), mirroring the
	// client's h1/h2c negotiation for server-to-server RPC.
	proto := &http.Protocols{}
	proto.SetHTTP1(true)
	proto.SetUnencryptedHTTP2(true)

	srv := &http.Server{
		Addr:      "127.0.0.1:" + port(),
		Handler:   easyrpcserver.Serve(methods, reg),
		Protocols: proto,
	}
	_ = srv.ListenAndServe()
}
