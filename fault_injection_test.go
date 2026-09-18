package easyrpc

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/binary"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

// Fault injection (spec §4.2 M8/M10 end-to-end): a mock server emits
// malformed stream bodies; the client MUST surface errors, never partial
// payloads, never raw compressed bytes. Mirrored in TS/Rust/Python.

func faultServer(t *testing.T, body []byte) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("content-type", "application/connect+proto")
		w.WriteHeader(200)
		_, _ = w.Write(body)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func openAndCollect(t *testing.T, base string) (idx []byte, err error) {
	t.Helper()
	rt := NewNetHTTP(nil)
	st, oerr := rt.OpenStream(context.Background(), Request{URL: base + "/x"})
	if oerr != nil {
		return nil, oerr
	}
	defer st.Close()
	for {
		p, rerr := st.Recv()
		if rerr == io.EOF {
			return idx, nil
		}
		if rerr != nil {
			return idx, rerr
		}
		idx = append(idx, p[0])
	}
}

func frameBytes(payload []byte, end bool, compressed bool) []byte {
	var flags byte
	if end {
		flags |= 0x02
	}
	if compressed {
		flags |= 0x01
	}
	out := []byte{flags}
	var lenb [4]byte
	binary.BigEndian.PutUint32(lenb[:], uint32(len(payload)))
	out = append(out, lenb[:]...)
	return append(out, payload...)
}

func TestFaultMidFrameTruncation(t *testing.T) {
	full := append(frameBytes([]byte{0}, false, false), frameBytes([]byte{1}, false, false)...)
	srv := faultServer(t, full[:len(full)-4])
	_, err := openAndCollect(t, srv.URL)
	if err == nil {
		t.Fatal("F1: truncated frame must error")
	}
}

func TestFaultMissingEndFrame(t *testing.T) {
	srv := faultServer(t, append(frameBytes([]byte{0}, false, false), frameBytes([]byte{1}, false, false)...))
	idx, err := openAndCollect(t, srv.URL)
	if len(idx) != 2 {
		t.Fatalf("F2: frames before close: %v", idx)
	}
	if err == nil {
		t.Fatal("F2: stream ending without an END frame must error")
	}
	if e, ok := err.(*RPCError); !ok || e.Code != 13 {
		t.Fatalf("F2: want code 13, got %v", err)
	}
}

func TestFaultGarbageEndIsClean(t *testing.T) {
	body := append(frameBytes([]byte{0}, false, false), frameBytes([]byte{0xff, 0xfe, 0x42}, true, false)...)
	srv := faultServer(t, body)
	idx, err := openAndCollect(t, srv.URL)
	if err != nil {
		t.Fatalf("F3: garbage END must be a clean end, got %v", err)
	}
	if len(idx) != 1 {
		t.Fatalf("F3: want the data frame, got %v", idx)
	}
}

func TestFaultCorruptGzip(t *testing.T) {
	corrupt := []byte{0x1f, 0x8b, 0x08, 0x00, 0xde, 0xad, 0xbe, 0xef}
	body := append(frameBytes(corrupt, false, true), frameBytes(nil, true, false)...)
	srv := faultServer(t, body)
	idx, err := openAndCollect(t, srv.URL)
	if err == nil {
		t.Fatal("F4: corrupt gzip must error")
	}
	if len(idx) != 0 {
		t.Fatalf("F4: must never yield raw compressed bytes, got %v", idx)
	}
}

func TestFaultValidGzipDecodes(t *testing.T) {
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	_, _ = zw.Write([]byte{7})
	_ = zw.Close()
	body := append(frameBytes(buf.Bytes(), false, true), frameBytes(nil, true, false)...)
	srv := faultServer(t, body)
	idx, err := openAndCollect(t, srv.URL)
	if err != nil {
		t.Fatalf("F6: valid gzip must decode: %v", err)
	}
	if len(idx) != 1 || idx[0] != 7 {
		t.Fatalf("F6: want [7], got %v", idx)
	}
}
