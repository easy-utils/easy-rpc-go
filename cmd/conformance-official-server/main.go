// Official ConnectRPC conformance server (easy-rpc), server-under-test mode.
//
// Reads a size-prefixed ServerCompatRequest from stdin, starts an easy-rpc
// HTTP server on an ephemeral port implementing the official
// connectrpc.conformance.v1.ConformanceService, then writes the
// ServerCompatResponse to stdout. Validates our Connect subset against the
// real `connectconformance` runner.
package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	easyrpc "github.com/easy-utils/easy-rpc-go"
	cv1 "github.com/easy-utils/easy-rpc-go/conformance/official/connectrpc/conformance/v1"
	easyrpcserver "github.com/easy-utils/easy-rpc-go/server"
)

const headerTimeout = "connect-timeout-ms"

type impl struct{}

func main() {
	// Read one size-prefixed ServerCompatRequest.
	var lenBuf [4]byte
	if _, err := io.ReadFull(os.Stdin, lenBuf[:]); err != nil {
		panic(err)
	}
	n := binary.BigEndian.Uint32(lenBuf[:])
	body := make([]byte, n)
	if _, err := io.ReadFull(os.Stdin, body); err != nil {
		panic(err)
	}
	var req cv1.ServerCompatRequest
	if err := proto.Unmarshal(body, &req); err != nil {
		panic(err)
	}

	methods := cv1.ConformanceService_Methods()
	reg := cv1.RegisterConformanceServiceService(impl{})

	proto2 := new(http.Protocols)
	proto2.SetHTTP1(true)
	proto2.SetUnencryptedHTTP2(true)
	srv := &http.Server{Handler: easyrpcserver.Serve(methods, reg), Protocols: proto2}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	go func() { _ = srv.Serve(ln) }()

	port := ln.Addr().(*net.TCPAddr).Port
	resp := &cv1.ServerCompatResponse{Host: "127.0.0.1", Port: uint32(port)}
	out, _ := proto.Marshal(resp)
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(len(out)))
	_, _ = os.Stdout.Write(hdr[:])
	_, _ = os.Stdout.Write(out)
	os.Stdout.Sync()

	select {}
}

// ---- request info echo ----

func requestInfo(ctx context.Context, in proto.Message, hdrs easyrpc.Headers, query string) *cv1.ConformancePayload_RequestInfo {
	hs := easyrpc.HeadersFromContext(ctx)
	if hs == nil {
		hs = hdrs
	}
	var out []*cv1.Header
	for k, vs := range hs {
		out = append(out, &cv1.Header{Name: k, Value: vs})
	}
	msgAny, _ := anypb.New(in)
	info := &cv1.ConformancePayload_RequestInfo{
		RequestHeaders: out,
		Requests:       []*anypb.Any{msgAny},
	}
	if t := hs.Get(headerTimeout); t != "" {
		if ms, err := time.ParseDuration(t + "ms"); err == nil {
			info.TimeoutMs = proto.Int64(int64(ms / time.Millisecond))
		}
	}
	if query != "" {
		info.ConnectGetInfo = &cv1.ConformancePayload_ConnectGetInfo{QueryParams: parseQuery(query)}
	}
	return info
}

func parseQuery(q string) []*cv1.Header {
	var out []*cv1.Header
	for _, kv := range strings.Split(q, "&") {
		if kv == "" {
			continue
		}
		parts := strings.SplitN(kv, "=", 2)
		v := ""
		if len(parts) == 2 {
			v = parts[1]
		}
		out = append(out, &cv1.Header{Name: parts[0], Value: []string{v}})
	}
	return out
}

func applyHeaders(list []*cv1.Header, hc *easyrpc.HandlerContext, trailer bool) {
	for _, h := range list {
		for _, v := range h.Value {
			if trailer {
				hc.SetTrailer(h.Name, v)
			} else {
				hc.SetHeader(h.Name, v)
			}
		}
	}
}

func toRPCError(e *cv1.Error, info *cv1.ConformancePayload_RequestInfo) *easyrpc.RPCError {
	re := &easyrpc.RPCError{Code: int(e.GetCode()), Message: e.GetMessage()}
	for _, d := range e.GetDetails() {
		re.Details = append(re.Details, easyrpc.ErrorDetail{
			Type:  d.GetTypeUrl()[strings.LastIndex(d.GetTypeUrl(), "/")+1:],
			Value: d.GetValue(),
		})
	}
	if info != nil {
		b, _ := proto.Marshal(info)
		re.Details = append(re.Details, easyrpc.ErrorDetail{
			Type:  "connectrpc.conformance.v1.ConformancePayload.RequestInfo",
			Value: b,
		})
	}
	return re
}

// ---- ConformanceService ----

func (impl) Unary(ctx context.Context, in *cv1.UnaryRequest) (*cv1.UnaryResponse, error) {
	hc := easyrpc.HandlerContextFromContext(ctx)
	info := requestInfo(ctx, in, nil, "")
	def := in.GetResponseDefinition()
	if def == nil {
		return &cv1.UnaryResponse{Payload: &cv1.ConformancePayload{RequestInfo: info}}, nil
	}
	applyHeaders(def.GetResponseHeaders(), hc, false)
	applyHeaders(def.GetResponseTrailers(), hc, true)
	if e := def.GetError(); e != nil {
		return nil, toRPCError(e, info)
	}
	if d := def.GetResponseDelayMs(); d > 0 {
		time.Sleep(time.Duration(d) * time.Millisecond)
	}
	return &cv1.UnaryResponse{Payload: &cv1.ConformancePayload{RequestInfo: info, Data: def.GetResponseData()}}, nil
}

func (impl) IdempotentUnary(ctx context.Context, in *cv1.IdempotentUnaryRequest) (*cv1.IdempotentUnaryResponse, error) {
	return &cv1.IdempotentUnaryResponse{Payload: &cv1.ConformancePayload{
		RequestInfo: requestInfo(ctx, in, nil, ""),
	}}, nil
}

func (impl) ServerStream(ctx context.Context, in *cv1.ServerStreamRequest, emit func(*cv1.ServerStreamResponse) error) error {
	hc := easyrpc.HandlerContextFromContext(ctx)
	info := requestInfo(ctx, in, nil, "")
	def := in.GetResponseDefinition()
	if def == nil {
		return nil
	}
	applyHeaders(def.GetResponseHeaders(), hc, false)
	applyHeaders(def.GetResponseTrailers(), hc, true)
	first := true
	for _, data := range def.GetResponseData() {
		if d := def.GetResponseDelayMs(); d > 0 {
			time.Sleep(time.Duration(d) * time.Millisecond)
		}
		payload := &cv1.ConformancePayload{Data: data}
		if first {
			payload.RequestInfo = info
		}
		if err := emit(&cv1.ServerStreamResponse{Payload: payload}); err != nil {
			return err
		}
		first = false
	}
	if e := def.GetError(); e != nil {
		var i *cv1.ConformancePayload_RequestInfo
		if first {
			i = info
		}
		return toRPCError(e, i)
	}
	return nil
}

// Unsupported by design: easy-rpc has no client/bidi streaming.
func (impl) ClientStream(context.Context, *cv1.ClientStreamRequest) (*cv1.ClientStreamResponse, error) {
	return nil, &easyrpc.RPCError{Code: 12, Message: "client streaming is not supported"}
}

func (impl) BidiStream(context.Context, *cv1.BidiStreamRequest, func(*cv1.BidiStreamResponse) error) error {
	return &easyrpc.RPCError{Code: 12, Message: "bidi streaming is not supported"}
}

func (impl) Unimplemented(context.Context, *cv1.UnimplementedRequest) (*cv1.UnimplementedResponse, error) {
	return nil, &easyrpc.RPCError{Code: 12, Message: "unimplemented"}
}

var _ = fmt.Sprintf
