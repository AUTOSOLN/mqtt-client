// Package capture reads packet capture files, as written by tcpdump,
// dumpcap and Wireshark, and returns each TCP connection in them as two
// reassembled byte streams, one per direction, with timestamps. It knows
// nothing about MQTT: package replay decodes the streams.
//
// Read accepts:
//
//   - classic pcap, in either byte order, with microsecond or nanosecond
//     timestamps;
//   - pcapng, with several sections and several interfaces, each with its
//     own link type and timestamp resolution;
//   - either of those compressed with gzip.
//
// The link types read are Ethernet (with 802.1Q VLAN tags), Linux cooked
// capture v1 and v2 (tcpdump -i any), raw IP, and null/loopback (macOS
// lo0, Windows Npcap loopback), carrying IPv4 or IPv6. Packets on other
// link types, and frames that aren't TCP, are counted in Capture.Ignored.
//
// TCP segments are reassembled in sequence order: a retransmitted or
// duplicated segment is dropped, and one that arrives early waits for the
// bytes in front of it. A segment the capture missed ends the connection's
// streams with ErrGap at the point the gap opened, in both directions:
// bytes from either side of a gap are never joined.
//
// A capture file is untrusted input. Read never panics on its contents,
// limits each frame to 256 KiB, each pcapng block to 16 MiB, and the bytes
// waiting for a missing segment to 4 MiB per direction.
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
