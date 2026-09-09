package easyrpc

// Realm selects how much protocol coverage a client Transport wants.
type Realm string

const (
	// RealmStd is the minimal-dependency realm: HTTP/1 + HTTP/2 (h2, h2c) via
	// net/http. No QUIC/HTTP3 dependency is linked.
	RealmStd Realm = "std"
	// RealmAuto requests h3->h2->h1 negotiation. It requires the separate
	// submodule `github.com/easy-utils/easy-rpc-go/h3` (which links quic-go).
	// Without importing that submodule, RealmAuto falls back to net/http.
	RealmAuto Realm = "auto"
)

// NewTransport returns a Transport for the given realm. RealmStd only links
// net/http. RealmAuto tries the h3 bridge only when it has been registered by
// importing the optional `.../h3` submodule; otherwise it falls back to std.
func NewTransport(realm Realm) Transport {
	if realm == RealmAuto && autoFactory() != nil {
		if at := autoFactory(); at != nil {
			return at
		}
	}
	return NewNetHTTP(nil)
}

// autoFactoryFn is set by the optional `easy-rpc-go/h3` submodule (init) to
// provide the h3->h2->h1 Transport. The std build leaves it nil so RealmAuto
// yields the net/http bridge.
var autoFactoryFn func() Transport

// RegisterAuto lets an optional submodule (easy-rpc-go/h3) register an
// h3->h2->h1 Transport factory. Called from an init(); the std module leaves it
// unregistered so RealmAuto falls back to net/http.
func RegisterAuto(fn func() Transport) { autoFactoryFn = fn }


func autoFactory() Transport {
	if autoFactoryFn != nil {
		return autoFactoryFn()
	}
	return nil
}
