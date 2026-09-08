package main

import (
	"context"
	"net/http"

	easyrpc "github.com/easy-utils/easy-rpc-go"
	cv1 "github.com/easy-utils/easy-rpc-go/easyrpc/conformance/v1"
)

type impl struct{}

func (impl) Health(ctx context.Context, _ *cv1.HealthRequest) (*cv1.HealthResponse, error) {
	return &cv1.HealthResponse{Ok: true, Name: "conformance"}, nil
}
func (impl) Echo(ctx context.Context, in *cv1.EchoRequest) (*cv1.EchoResponse, error) {
	return &cv1.EchoResponse{Output: "echo:" + in.Input}, nil
}
func (impl) Count(ctx context.Context, in *cv1.CountRequest, emit func(*cv1.CountResponse) error) error {
	for i := 0; i < 3; i++ {
		if err := emit(&cv1.CountResponse{Index: int32(i)}); err != nil {
			return err
		}
	}
	return nil
}
func (impl) Fail(ctx context.Context, in *cv1.FailRequest) (*cv1.FailResponse, error) {
	return &cv1.FailResponse{Ok: in.Message == ""}, nil
}

func main() {
	methods := cv1.ConformanceService_Methods()
	reg := cv1.RegisterConformanceServiceService(impl{})
	_ = easyrpc.Serve(methods, reg)
	_ = http.ListenAndServe("127.0.0.1:18888", easyrpc.Serve(methods, reg))
}
