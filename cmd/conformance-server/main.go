package main

import (
	"context"
	"net/http"
	"os"

	easyrpc "github.com/easy-utils/easy-rpc-go"
	cv1 "github.com/easy-utils/easy-rpc-go/easyrpc/conformance/v1"
)

type impl struct{}
func (impl) Health(c context.Context, _ *cv1.HealthRequest) (*cv1.HealthResponse, error) { return &cv1.HealthResponse{Ok: true, Name: "conformance"}, nil }
func (impl) Echo(c context.Context, in *cv1.EchoRequest) (*cv1.EchoResponse, error) { return &cv1.EchoResponse{Output: "echo:" + in.Input}, nil }
func (impl) Count(c context.Context, in *cv1.CountRequest, emit func(*cv1.CountResponse) error) error { for i := 0; i < 3; i++ { if err := emit(&cv1.CountResponse{Index: int32(i)}); err != nil { return err } }; return nil }
func (impl) Fail(c context.Context, in *cv1.FailRequest) (*cv1.FailResponse, error) { return &cv1.FailResponse{Ok: in.Message == ""}, nil }

func port() string { p := os.Getenv("PORT"); if p == "" { p = "18888" }; return p }

func main() {
	methods := cv1.ConformanceService_Methods()
	reg := cv1.RegisterConformanceServiceService(impl{})
	_ = http.ListenAndServe("127.0.0.1:"+port(), easyrpc.Serve(methods, reg))
}
