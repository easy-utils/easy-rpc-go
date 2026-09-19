package easyrpc_test

import (
	"context"
	"testing"

	easyrpc "github.com/easy-utils/easy-rpc-go"
	cv1 "github.com/easy-utils/easy-rpc-go/easyrpc/conformance/v1"
)

// inProc is a Transport that routes requests straight into Dispatch (no
// sockets), so the JSON codec path is exercised end-to-end.
type inProc struct {
	methods []easyrpc.MethodSpec
	reg     *easyrpc.ServiceRegistry
}

func (p *inProc) Send(ctx context.Context, req easyrpc.Request) (easyrpc.Response, error) {
	status := 200
	header := easyrpc.Headers{}
	var body []byte
	w := &capWriter{status: &status, header: &header, body: &body}
	_ = easyrpc.Dispatch(ctx, req, p.methods, p.reg, w)
	resp := easyrpc.Response{Status: status, Headers: header, Body: body}
	if status >= 300 {
		code, msg, details := easyrpc.DecodeErrorJSON(body)
		resp.Error = &easyrpc.RPCError{Code: code, Message: msg, Details: details}
	}
	return resp, nil
}

func (p *inProc) OpenStream(ctx context.Context, req easyrpc.Request) (easyrpc.Stream, error) {
	_, err := p.Send(ctx, req)
	return nil, err
}

type capWriter struct {
	status *int
	header *easyrpc.Headers
	body   *[]byte
}

func (w *capWriter) Status(c int)                 { *w.status = c }
func (w *capWriter) Header(h easyrpc.Headers)     { *w.header = h }
func (w *capWriter) WriteFrame(p []byte) error    { *w.body = append(*w.body, p...); return nil }

// jsonImpl implements the conformance service subset used by the codec tests.
type jsonImpl struct{ cv1.ConformanceServiceService }

func (jsonImpl) Echo(_ context.Context, in *cv1.EchoRequest) (*cv1.EchoResponse, error) {
	return &cv1.EchoResponse{Output: "echo:" + in.Input}, nil
}
func (jsonImpl) EchoBytes(_ context.Context, in *cv1.EchoBytesRequest) (*cv1.EchoBytesResponse, error) {
	return &cv1.EchoBytesResponse{Data: in.Data}, nil
}
func (jsonImpl) FailDetails(_ context.Context, in *cv1.FailDetailsRequest) (*cv1.FailDetailsResponse, error) {
	return nil, &easyrpc.RPCError{Code: int(in.Code), Message: in.Message, Details: []easyrpc.ErrorDetail{{Type: in.DetailType, Value: []byte(in.DetailText)}}}
}

func newJSONClient() *cv1.ConformanceServiceClient {
	reg := cv1.RegisterConformanceServiceService(jsonImpl{})
	return cv1.NewConformanceServiceClient(&inProc{methods: cv1.ConformanceService_Methods(), reg: reg})
}

func TestJSONCodecUnary(t *testing.T) {
	c := newJSONClient()
	out, err := c.Echo(context.Background(), &cv1.EchoRequest{Input: "hi"}, easyrpc.WithKind(easyrpc.KindJSON))
	if err != nil {
		t.Fatal(err)
	}
	if out.Output != "echo:hi" {
		t.Fatalf("echo %q", out.Output)
	}
}

func TestJSONCodecBytes(t *testing.T) {
	c := newJSONClient()
	data := []byte{0, 1, 2, 0xff, 0xfe, 0x80}
	out, err := c.EchoBytes(context.Background(), &cv1.EchoBytesRequest{Data: data}, easyrpc.WithKind(easyrpc.KindJSON))
	if err != nil {
		t.Fatal(err)
	}
	if string(out.Data) != string(data) {
		t.Fatalf("bytes %v", out.Data)
	}
}

func TestJSONCodecError(t *testing.T) {
	c := newJSONClient()
	_, err := c.FailDetails(context.Background(), &cv1.FailDetailsRequest{Code: 8, Message: "limited", DetailType: "t/x", DetailText: "d"}, easyrpc.WithKind(easyrpc.KindJSON))
	if err == nil {
		t.Fatal("expected error")
	}
	re, ok := err.(*easyrpc.RPCError)
	if !ok || re.Code != 8 || len(re.Details) != 1 || re.Details[0].Type != "t/x" {
		t.Fatalf("bad error: %#v", err)
	}
}

func TestJSONContentTypeShapeMismatch(t *testing.T) {
	reg := cv1.RegisterConformanceServiceService(jsonImpl{})
	p := &inProc{methods: cv1.ConformanceService_Methods(), reg: reg}
	status := 200
	header := easyrpc.Headers{}
	var body []byte
	w := &capWriter{status: &status, header: &header, body: &body}
	req := easyrpc.Request{
		URL:     "/easyrpc.conformance.v1.ConformanceService/Echo",
		Headers: easyrpc.Headers{"Content-Type": []string{"application/connect+json"}},
		Body:    []byte(`{"input":"hi"}`),
	}
	_ = easyrpc.Dispatch(context.Background(), req, p.methods, p.reg, w)
	if status != 415 {
		t.Fatalf("want 415 got %d", status)
	}
}
