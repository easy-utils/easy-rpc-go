package easyrpc

import (
	"bytes"
	"encoding/binary"
	"strings"
	"testing"
)

// Error-path matrix (spec §4.2 M1–M13) + Error Details round-trip (§4.1).
// Mirrored in every language implementation; inputs are constructed directly
// against the protocol functions — no server needed.

var matrixDetail = ErrorDetail{
	Type:  "type.googleapis.com/google.rpc.RetryInfo",
	Value: []byte{1, 2, 3, 250},
}

func TestMatrixEndStreamDecode(t *testing.T) {
	// M1: empty payload => clean end
	if m := DecodeEndStream(nil); m.Code != 0 {
		t.Errorf("M1: want clean end, got %+v", m)
	}
	// M2: garbage bytes => clean end, no panic
	if m := DecodeEndStream([]byte{0xff, 0xfe, 0x00, 0x42}); m.Code != 0 {
		t.Errorf("M2: want clean end for garbage, got %+v", m)
	}
	// M3: error without code/message => unknown code, empty message
	if m := DecodeEndStream([]byte(`{"error":{}}`)); m.Code != 2 || m.Message != "" {
		t.Errorf("M3: want 2/empty, got %d/%q", m.Code, m.Message)
	}
	// M4: unknown code name => 2
	if m := DecodeEndStream([]byte(`{"error":{"code":"nope","message":"m"}}`)); m.Code != 2 || m.Message != "m" {
		t.Errorf("M4: want 2, got %d", m.Code)
	}
	// M5: unknown fields ignored
	if m := DecodeEndStream([]byte(`{"error":{"code":"not_found","message":"m"},"x":1}`)); m.Code != 5 {
		t.Errorf("M5: want 5, got %d", m.Code)
	}
	// M6: details round-trip (base64 -> bytes)
	payload := EncodeEndStream(EndStreamMessage{Code: 8, Message: "rate limited", Details: []ErrorDetail{matrixDetail}})
	m := DecodeEndStream(payload)
	if m.Code != 8 || len(m.Details) != 1 || m.Details[0].Type != matrixDetail.Type || !bytes.Equal(m.Details[0].Value, matrixDetail.Value) {
		t.Errorf("M6: details mismatch: %+v", m)
	}
	// M7: malformed detail entries skipped
	m = DecodeEndStream([]byte(`{"error":{"code":"resource_exhausted","details":[{"type":"t","value":"!!!"},{"value":"x"},{"type":"ok"},{"type":"t2","value":"AQID"}]}}`))
	if len(m.Details) != 1 || m.Details[0].Type != "t2" || !bytes.Equal(m.Details[0].Value, []byte{1, 2, 3}) {
		t.Errorf("M7: want only t2 kept, got %+v", m.Details)
	}
	// details omitted when empty (v1.0 byte-for-byte)
	payload = EncodeEndStream(EndStreamMessage{Code: 5, Message: "gone"})
	if strings.Contains(string(payload), "details") {
		t.Errorf("details must be omitted when empty: %s", payload)
	}
}

func TestMatrixUnaryErrorDecode(t *testing.T) {
	// M11: legacy header path handled by bridges; plain-text body falls back
	// to the status mapping.
	if c, _, _ := DecodeErrorJSON([]byte("busy")); c != 0 {
		t.Errorf("M11: plain text is not a JSON error body, want 0, got %d", c)
	}
	// details round-trip
	b := EncodeErrorJSON(8, "limited", []ErrorDetail{matrixDetail})
	c, msg, ds := DecodeErrorJSON(b)
	if c != 8 || msg != "limited" || len(ds) != 1 || !bytes.Equal(ds[0].Value, matrixDetail.Value) {
		t.Errorf("unary details round-trip failed: %d %q %+v", c, msg, ds)
	}
	// no-details body stays v1.0-shaped
	if b := EncodeErrorJSON(5, "x", nil); strings.Contains(string(b), "details") {
		t.Errorf("unary details must be omitted when empty: %s", b)
	}
	// M12/M13: deadline code mapping
	if c, _, _ := DecodeErrorJSON(EncodeErrorJSON(4, "deadline exceeded", nil)); c != 4 {
		t.Errorf("M12/M13: want code 4, got %d", c)
	}
}

func TestMatrixFraming(t *testing.T) {
	// M8: truncated frame must error, not silently end.
	full := Frame([]byte("0123456789"), false)
	if _, _, err := ReadFrame(bytes.NewReader(full[:len(full)-4])); err == nil {
		t.Errorf("M8: truncated frame must error")
	}
	// M9: oversized frame rejected with resource_exhausted.
	huge := make([]byte, 5)
	binary.BigEndian.PutUint32(huge[1:], 4*1024*1024+1)
	_, _, err := ReadFrame(bytes.NewReader(huge))
	if err == nil {
		t.Fatalf("M9: want error")
	}
	if e, ok := err.(*RPCError); !ok || e.Code != 8 {
		t.Errorf("M9: want code 8, got %v", err)
	}
}

func TestMatrixDeadlineCodes(t *testing.T) {
	// M12/M13: deadline maps to code 4 both directions.
	if HTTPStatus(4) != 504 || ConnectFromStatus(504) != 4 {
		t.Errorf("deadline status mapping broken")
	}
}
