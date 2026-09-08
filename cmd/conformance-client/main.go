// easy-rpc Go conformance client (used by the 8x8 matrix). Reads EASY_RPC_BASE.
package main

import (
	"context"
	"io"
	"os"
	"fmt"
	"strings"

	"google.golang.org/protobuf/proto"

	easyrpc "github.com/easy-utils/easy-rpc-go"
	cv1 "github.com/easy-utils/easy-rpc-go/easyrpc/conformance/v1"
)

// baseTransport prefixes a base URL onto relative paths (like other languages).
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

func main() {
	base := os.Getenv("EASY_RPC_BASE")
	if base == "" { base = "http://127.0.0.1:18888" }
	rt := &baseTransport{rt: easyrpc.NewNetHTTP(nil), base: base}
	c := cv1.NewConformanceServiceClient(rt)
	ctx := context.Background()

	out, err := c.Echo(ctx, &cv1.EchoRequest{Input: "hi"})
	if err != nil { fmt.Println("ECHO_FAIL", err); os.Exit(1) }
	if out.Output != "echo:hi" { fmt.Println("ECHO_WRONG", out.Output); os.Exit(1) }

	stream, err := c.Count(ctx, &cv1.CountRequest{Count: 3})
	if err != nil { fmt.Println("COUNT_OPEN_FAIL", err); os.Exit(1) }
	var idx []int32
	for {
		payload, err := stream.Recv()
		if err == io.EOF { break }
		if err != nil { fmt.Println("COUNT_RECV_FAIL", err); os.Exit(1) }
		resp := &cv1.CountResponse{}
		if err := proto.Unmarshal(payload, resp); err != nil { fmt.Println("COUNT_DECODE_FAIL", err); os.Exit(1) }
		idx = append(idx, resp.Index)
	}
	if len(idx) != 3 || idx[0] != 0 || idx[1] != 1 || idx[2] != 2 {
		fmt.Println("COUNT_WRONG", idx); os.Exit(1)
	}
	fmt.Println("GO_CLIENT_OK")
}
