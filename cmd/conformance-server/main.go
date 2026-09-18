package main

import (
	"context"
	"net/http"
	"os"
	"time"

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
	n := int32(3)
	if in.Count > 0 {
		n = in.Count
	}
	for i := int32(0); i < n; i++ {
		if err := emit(&cv1.CountResponse{Index: i}); err != nil {
			return err
		}
	}
	return nil
}
func (impl) Fail(c context.Context, in *cv1.FailRequest) (*cv1.FailResponse, error) {
	if in.Message != "" {
		return nil, &easyrpc.RPCError{Code: 3, Message: in.Message}
	}
	return &cv1.FailResponse{Ok: true}, nil
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

// EchoBytes round-trips arbitrary bytes (non-UTF-8).
func (impl) EchoBytes(c context.Context, in *cv1.EchoBytesRequest) (*cv1.EchoBytesResponse, error) {
	return &cv1.EchoBytesResponse{Data: in.Data}, nil
}

// Sleep blocks `millis`; exercises the Connect deadline (M12/M13).
func (impl) Sleep(c context.Context, in *cv1.SleepRequest) (*cv1.SleepResponse, error) {
	select {
	case <-time.After(time.Duration(in.Millis) * time.Millisecond):
	case <-c.Done():
		return nil, &easyrpc.RPCError{Code: 4, Message: "deadline exceeded"}
	}
	return &cv1.SleepResponse{Ok: true}, nil
}

// Empty returns an empty message.
func (impl) Empty(c context.Context, in *cv1.EmptyRequest) (*cv1.EmptyResponse, error) {
	return &cv1.EmptyResponse{}, nil
}

// BigStream emits `count` frames declaring `size` bytes each.
func (impl) BigStream(c context.Context, in *cv1.BigStreamRequest, emit func(*cv1.BigStreamResponse) error) error {
	n := in.Count
	if n <= 0 {
		n = 3
	}
	for i := int32(0); i < n; i++ {
		if err := emit(&cv1.BigStreamResponse{Index: i, Size: in.Size}); err != nil {
			return err
		}
	}
	return nil
}

// FailDetails fails the unary call with an error carrying structured details
// (spec §4.1): {type: detail_type, value: utf8(detail_text)}.
func (impl) FailDetails(c context.Context, in *cv1.FailDetailsRequest) (*cv1.FailDetailsResponse, error) {
	return &cv1.FailDetailsResponse{Ok: false}, &easyrpc.RPCError{
		Code:    int(in.Code),
		Message: in.Message,
		Details: []easyrpc.ErrorDetail{{Type: in.DetailType, Value: []byte(in.DetailText)}},
	}
}

// StreamFailDetails emits `emit_before` frames, then ends the stream with an
// error carrying structured details (spec §4.1).
func (impl) StreamFailDetails(c context.Context, in *cv1.StreamFailDetailsRequest, emit func(*cv1.StreamFailDetailsResponse) error) error {
	for i := int32(0); i < in.EmitBefore; i++ {
		if err := emit(&cv1.StreamFailDetailsResponse{Index: i}); err != nil {
			return err
		}
	}
	return &easyrpc.RPCError{
		Code:    int(in.Code),
		Message: in.Message,
		Details: []easyrpc.ErrorDetail{{Type: in.DetailType, Value: []byte(in.DetailText)}},
	}
}

// EchoTrailer sets unary trailing metadata (spec §3.3).
func (impl) EchoTrailer(c context.Context, in *cv1.EchoTrailerRequest) (*cv1.EchoTrailerResponse, error) {
	if hc := easyrpc.HandlerContextFromContext(c); hc != nil {
		hc.SetTrailer("x-trl", "unary-"+in.Input)
	}
	return &cv1.EchoTrailerResponse{Output: "trailer:" + in.Input}, nil
}

// CountTrailer sets streaming trailing metadata (spec §3.3).
func (impl) CountTrailer(c context.Context, in *cv1.CountTrailerRequest, emit func(*cv1.CountTrailerResponse) error) error {
	if hc := easyrpc.HandlerContextFromContext(c); hc != nil {
		hc.SetTrailer("x-ctrailer", "done")
	}
	n := int32(3)
	if in.Count > 0 {
		n = in.Count
	}
	for i := int32(0); i < n; i++ {
		if err := emit(&cv1.CountTrailerResponse{Index: i}); err != nil {
			return err
		}
	}
	return nil
}

func addr() string {
	// BIND=127.0.0.1 keeps it pod-local; BIND=0.0.0.0 exposes the conformance
	// surface to in-cluster peers (macOS worker transport tests).
	b := os.Getenv("BIND")
	if b == "" {
		b = "0.0.0.0"
	}
	p := os.Getenv("PORT")
	if p == "" {
		p = "18888"
	}
	return b + ":" + p
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
		Addr:      addr(),
		Handler:   easyrpcserver.Serve(methods, reg),
		Protocols: proto,
	}
	_ = srv.ListenAndServe()
}
