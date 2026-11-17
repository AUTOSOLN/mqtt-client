// Package capture reads packet capture files, as written by tcpdump, and
// returns each TCP connection in them as two reassembled byte streams, one
// per direction, with timestamps. It knows nothing about MQTT: package
// replay decodes the streams.
//
// Supported so far: classic pcap (either byte order, microsecond or
// nanosecond timestamps) with Ethernet framing (802.1Q VLAN tags allowed),
// IPv4 and TCP whose segments arrive in order. A retransmitted segment is
// dropped as a duplicate. A segment the capture missed ends the
// connection's streams with ErrGap; bytes from either side of a gap are
// never joined.
//
// A capture file is untrusted input: Read never panics on its contents and
// limits each record to 256 KiB.
package capture

import (
	"errors"
	"net/netip"
	"time"
)

// Dir is the direction of a segment.
type Dir uint8

// Directions.
const (
	FromClient Dir = iota
	FromServer
)

func (d Dir) String() string {
	if d == FromClient {
		return "client"
	}
	return "server"
}

// Transport says what a connection's bytes are.
type Transport uint8

// Transports.
const (
	Plain Transport = iota // the bytes are MQTT
	TLS                    // the bytes are TLS records; see ErrEncrypted
)

// Segment is bytes seen in one direction. Segment boundaries carry no
// meaning: one MQTT packet may span several segments, and one segment may
// hold several packets.
type Segment struct {
	Dir  Dir
	Time time.Time
	Data []byte
}

// Conn is one TCP connection in capture order. Fields are exported so that
// a test can build a Conn by hand, without a capture file.
type Conn struct {
	Client, Server netip.AddrPort
	Transport      Transport
	Segments       []Segment
	ClientClose    *time.Time // FIN or RST from the client; nil if none was captured
	ServerClose    *time.Time // FIN or RST from the server; nil if none was captured
	Err            error      // why the streams end early (ErrGap, ErrTruncated, ErrDirection), or nil
}

// Capture is the content of a capture file.
type Capture struct {
	Conns []Conn // in the order their first packet was captured

	// Ignored counts frames that weren't IPv4 TCP (ARP, IPv6, UDP, a later
	// IP fragment), so that a reader can tell an empty result from a
	// capture of something else.
	Ignored int
}

// Errors. They are returned wrapped; test for them with errors.Is.
var (
	ErrFormat    = errors.New("capture: unsupported or invalid capture file")
	ErrEncrypted = errors.New("capture: connection is TLS encrypted")
	ErrGap       = errors.New("capture: bytes missing from TCP stream")
	ErrTruncated = errors.New("capture: packet truncated")
	ErrDirection = errors.New("capture: cannot tell client from server")
)
