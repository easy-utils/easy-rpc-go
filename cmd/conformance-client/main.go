// easy-rpc Go client re-almed via NewTransport(realm). This test runs against
// the Go conformance server (h1 + h2c) and supports both std and auto realms.
package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"

	"google.golang.org/protobuf/proto"

	easyrpc "github.com/easy-utils/easy-rpc-go"
	cv1 "github.com/easy-utils/easy-rpc-go/easyrpc/conformance/v1"
)

func main() {
	base := os.Getenv("EASY_RPC_BASE")
	if base == "" {
		base = "http://127.0.0.1:18888"
	}
	realm := easyrpc.Realm(os.Getenv("EASY_RPC_REALM"))
	if realm == "" {
		realm = easyrpc.RealmStd
	}

	rt := &baseTransport{rt: easyrpc.NewTransport(realm), base: base}
	c := cv1.NewConformanceServiceClient(rt)
	ctx := context.Background()

	out, err := c.Echo(ctx, &cv1.EchoRequest{Input: "hi"})
	if err != nil {
		fmt.Println("ECHO_FAIL", err)
		os.Exit(1)
	}
	if out.Output != "echo:hi" {
		fmt.Println("ECHO_WRONG", out.Output)
		os.Exit(1)
	}

	stream, err := c.Count(ctx, &cv1.CountRequest{Count: 3})
	if err != nil {
		fmt.Println("COUNT_OPEN_FAIL", err)
		os.Exit(1)
	}
	var idx []int32
	for {
		payload, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			fmt.Println("COUNT_RECV_FAIL", err)
			os.Exit(1)
		}
		resp := &cv1.CountResponse{}
		if err := proto.Unmarshal(payload, resp); err != nil {
			fmt.Println("COUNT_DECODE_FAIL", err)
			os.Exit(1)
		}
		idx = append(idx, resp.Index)
	}
	if len(idx) != 3 || idx[0] != 0 || idx[1] != 1 || idx[2] != 2 {
		fmt.Println("COUNT_WRONG", idx)
		os.Exit(1)
	}

	// ---- error details conformance (spec §4.1) ----
	if _, err := c.FailDetails(ctx, &cv1.FailDetailsRequest{Code: 8, Message: "limited", DetailType: "type.googleapis.com/google.rpc.RetryInfo", DetailText: "retry:5s"}); err == nil {
		fmt.Println("FAILDETAILS_NO_ERROR")
		os.Exit(1)
	} else if re, ok := err.(*easyrpc.RPCError); !ok || re.Code != 8 || len(re.Details) != 1 ||
		re.Details[0].Type != "type.googleapis.com/google.rpc.RetryInfo" || string(re.Details[0].Value) != "retry:5s" {
		fmt.Println("FAILDETAILS_WRONG", err)
		os.Exit(1)
	}

	sfd, err := c.StreamFailDetails(ctx, &cv1.StreamFailDetailsRequest{EmitBefore: 2, Code: 13, Message: "boom", DetailType: "t/stream", DetailText: "sd"})
	if err != nil {
		fmt.Println("SFD_OPEN_FAIL", err)
		os.Exit(1)
	}
	var sfdIdx []int32
	for {
		payload, err := sfd.Recv()
		if err == io.EOF {
			fmt.Println("SFD_NO_ERROR")
			os.Exit(1)
		}
		if err != nil {
			if re, ok := err.(*easyrpc.RPCError); ok && re.Code == 13 && len(re.Details) == 1 && string(re.Details[0].Value) == "sd" {
				break
			}
			fmt.Println("SFD_WRONG_ERR", err)
			os.Exit(1)
		}
		r := &cv1.StreamFailDetailsResponse{}
		if err := proto.Unmarshal(payload, r); err != nil {
			fmt.Println("SFD_DECODE_FAIL", err)
			os.Exit(1)
		}
		sfdIdx = append(sfdIdx, r.Index)
	}
	if len(sfdIdx) != 2 {
		fmt.Println("SFD_WRONG_FRAMES", sfdIdx)
		os.Exit(1)
	}

	fmt.Println("GO_CLIENT_OK", string(realm))
}

type baseTransport struct {
	rt   easyrpc.Transport
	base string
}

func (b *baseTransport) prepend(u string) string {
	if strings.HasPrefix(u, "http://") || strings.HasPrefix(u, "https://") {
		return u
	}
	return strings.TrimRight(b.base, "/") + u
}
func (b *baseTransport) Send(ctx context.Context, req easyrpc.Request) (easyrpc.Response, error) {
	req.URL = b.prepend(req.URL)
	return b.rt.Send(ctx, req)
}
func (b *baseTransport) OpenStream(ctx context.Context, req easyrpc.Request) (easyrpc.Stream, error) {
	req.URL = b.prepend(req.URL)
	return b.rt.OpenStream(ctx, req)
}
