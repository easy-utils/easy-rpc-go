# easy-rpc-go

Zero-runtime-bindings easy-rpc core for Go. It provides:
- a `Transport` interface (unary + server-stream),
- the Connect wire protocol (framing, error codes, Content-Type),
- a reference `net/http` bridge (`NewNetHTTP`),
- server-side frame helpers,
- self-generated `.easyrpc.go` (MethodSpecs) via `protoc-gen-easyrpc-go`.

Messages come from official `protoc-gen-go`; this package does not depend on
connectrpc. HTTP runtimes are pluggable via bridges.

```go
package main

import (
    "context"
    easyrpc "github.com/easy-utils/easy-rpc-go"
)

func main() {
    rt := easyrpc.NewNetHTTP(nil)
    resp, err := rt.Send(context.Background(), easyrpc.Request{
        URL: "https://agent.example.com/v1/echo", Method: "POST", Body: []byte("hi"),
    })
    _ = resp
    _ = err
}
```
