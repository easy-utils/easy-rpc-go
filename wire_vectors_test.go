package easyrpc

// Wire golden-vector conformance (transport-independent): the protocol layer
// must reproduce easy-rpc-spec/conformance/wire-vectors.json byte-for-byte.
// Vectors are shared verbatim with every other language.
import (
	"encoding/hex"
	"encoding/json"
	"os"
	"testing"
)

type wireVectors struct {
	Frames []struct {
		Name  string `json:"name"`
		Enc   struct {
			PayloadHex string `json:"payloadHex"`
			End        bool   `json:"end"`
			Compressed bool   `json:"compressed"`
		} `json:"encode"`
		BytesHex string `json:"bytesHex"`
	} `json:"frames"`
	EndStream []struct {
		Name     string `json:"name"`
		Encode   *struct {
			Code     int                 `json:"code"`
			Message  string              `json:"message"`
			Metadata map[string][]string `json:"metadata"`
		} `json:"encode"`
		Decode   struct {
			BytesHex string `json:"bytesHex"`
		} `json:"decode"`
		BytesHex string              `json:"bytesHex"`
		Code     int                 `json:"code"`
		Message  string              `json:"message"`
		Metadata map[string][]string `json:"metadata"`
	} `json:"endStream"`
	UnaryError []struct {
		Name  string `json:"name"`
		Enc   struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
			Details []struct {
				Type    string `json:"type"`
				ValueHx string `json:"valueHex"`
			} `json:"details"`
		} `json:"encode"`
		BytesHex string `json:"bytesHex"`
	} `json:"unaryError"`
	TrailerHeaders []struct {
		Name    string              `json:"name"`
		Demux   map[string][]string `json:"demux"`
		Headers map[string][]string `json:"headers"`
		Trail   map[string][]string `json:"trailers"`
		Mux     *struct {
			Headers  map[string][]string `json:"headers"`
			Trailers map[string][]string `json:"trailers"`
		} `json:"mux"`
		Result map[string][]string `json:"result"`
	} `json:"trailerHeaders"`
	CodeNames []struct {
		Code int    `json:"code"`
		Name string `json:"name"`
		HTTP int    `json:"http"`
	} `json:"codeNames"`
}

func loadVectors(t *testing.T) wireVectors {
	t.Helper()
	path := "testdata/wire-vectors.json"
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read vectors: %v", err)
	}
	var v wireVectors
	if err := json.Unmarshal(b, &v); err != nil {
		t.Fatalf("parse vectors: %v", err)
	}
	return v
}

func jsonEqual(t *testing.T, a, b []byte) bool {
	t.Helper()
	var va, vb any
	if err := json.Unmarshal(a, &va); err != nil {
		return false
	}
	if err := json.Unmarshal(b, &vb); err != nil {
		return false
	}
	ja, _ := json.Marshal(va)
	jb, _ := json.Marshal(vb)
	return string(ja) == string(jb)
}

func mustHex(t *testing.T, h string) []byte {
	t.Helper()
	b, err := hex.DecodeString(h)
	if err != nil {
		t.Fatalf("bad hex %q: %v", h, err)
	}
	return b
}

func TestWireVectorsFrames(t *testing.T) {
	for _, f := range loadVectors(t).Frames {
		raw := Frame(mustHex(t, f.Enc.PayloadHex), f.Enc.End)
		if f.Enc.Compressed {
			// Vectors specify raw framing with the compressed bit set; the
			// payload bytes are already-compressed in the vector.
			raw[0] |= 0x01
		}
		got := hex.EncodeToString(raw)
		if got != f.BytesHex {
			t.Errorf("%s: got %s want %s", f.Name, got, f.BytesHex)
		}
	}
}

func TestWireVectorsEndStream(t *testing.T) {
	for _, e := range loadVectors(t).EndStream {
		m := DecodeEndStream(mustHex(t, e.Decode.BytesHex))
		if m.Code != e.Code {
			t.Errorf("%s: code got %d want %d", e.Name, m.Code, e.Code)
		}
		if m.Message != e.Message {
			t.Errorf("%s: message got %q want %q", e.Name, m.Message, e.Message)
		}
		if e.Metadata != nil {
			if len(m.Metadata) == 0 {
				t.Errorf("%s: metadata missing", e.Name)
			} else {
				for k, want := range e.Metadata {
					got := m.Metadata[k]
					if len(got) != len(want) {
						t.Errorf("%s: metadata[%s] got %v want %v", e.Name, k, got, want)
					}
				}
			}
		}
		if e.Encode != nil && e.BytesHex != "" {
			got := EncodeEndStream(EndStreamMessage{Code: e.Encode.Code, Message: e.Encode.Message, Metadata: e.Encode.Metadata})
			if !jsonEqual(t, got, mustHex(t, e.BytesHex)) {
				t.Errorf("%s encode: got %s want %s", e.Name, string(got), string(mustHex(t, e.BytesHex)))
			}
		}
	}
}

func TestWireVectorsUnaryError(t *testing.T) {
	for _, u := range loadVectors(t).UnaryError {
		var details []ErrorDetail
		for _, d := range u.Enc.Details {
			details = append(details, ErrorDetail{Type: d.Type, Value: mustHex(t, d.ValueHx)})
		}
		got := EncodeErrorJSON(u.Enc.Code, u.Enc.Message, details)
		// JSON payloads compare semantically (key order is not significant).
		if !jsonEqual(t, got, mustHex(t, u.BytesHex)) {
			t.Errorf("%s: got %s want %s", u.Name, string(got), string(mustHex(t, u.BytesHex)))
		}
	}
}

func TestWireVectorsTrailers(t *testing.T) {
	for _, tr := range loadVectors(t).TrailerHeaders {
		if tr.Demux != nil {
			h, tl := DemuxTrailers(tr.Demux)
			if len(h) != len(tr.Headers) || len(tl) != len(tr.Trail) {
				t.Errorf("%s: demux sizes got (%d,%d) want (%d,%d)", tr.Name, len(h), len(tl), len(tr.Headers), len(tr.Trail))
			}
			for k, want := range tr.Trail {
				got := tl[k]
				if len(got) != len(want) {
					t.Errorf("%s: trailer[%s] got %v want %v", tr.Name, k, got, want)
				}
			}
		}
		if tr.Mux != nil {
			got := MuxTrailers(tr.Mux.Headers, tr.Mux.Trailers)
			if len(got) != len(tr.Result) {
				t.Errorf("%s: mux size got %d want %d", tr.Name, len(got), len(tr.Result))
			}
		}
	}
}

func TestWireVectorsCodeMap(t *testing.T) {
	for _, c := range loadVectors(t).CodeNames {
		if got := CodeToString(c.Code); got != c.Name {
			t.Errorf("code %d: got %q want %q", c.Code, got, c.Name)
		}
		if got := CodeFromString(c.Name); got != c.Code {
			t.Errorf("name %q: got %d want %d", c.Name, got, c.Code)
		}
		if c.Code != 0 && HTTPStatus(c.Code) != c.HTTP {
			t.Errorf("http %d: got %d want %d", c.Code, HTTPStatus(c.Code), c.HTTP)
		}
	}
}
