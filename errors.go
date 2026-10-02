package mqttclient

import (
	"errors"
	"fmt"
)

// Sentinel errors, roughly following libmosquitto's MOSQ_ERR_* values. Errors
// returned by this package wrap one of these, so test with errors.Is.
var (
	ErrInvalid          = errors.New("mqttclient: invalid argument") // MOSQ_ERR_INVAL
	ErrNotSupported     = errors.New("mqttclient: not supported")    // MOSQ_ERR_NOT_SUPPORTED
	ErrNoConn           = errors.New("mqttclient: not connected")    // MOSQ_ERR_NO_CONN
	ErrAlreadyConnected = errors.New("mqttclient: already connected or connecting")
	ErrConnectionLost   = errors.New("mqttclient: connection lost")                    // MOSQ_ERR_CONN_LOST / MOSQ_ERR_ERRNO
	ErrKeepalive        = errors.New("mqttclient: keepalive timeout")                  // MOSQ_ERR_KEEPALIVE
	ErrProtocol         = errors.New("mqttclient: protocol error")                     // MOSQ_ERR_PROTOCOL
	ErrMalformedPacket  = errors.New("mqttclient: malformed packet")                   // MOSQ_ERR_MALFORMED_PACKET
	ErrOversizePacket   = errors.New("mqttclient: packet exceeds maximum packet size") // MOSQ_ERR_OVERSIZE_PACKET
	ErrServerDisconnect = errors.New("mqttclient: server sent DISCONNECT")
	ErrDisconnected     = errors.New("mqttclient: disconnected by the application")
)

// ConnRefusedError is returned by Connect, and reported to OnDisconnect, when
// the server answers CONNECT with a non-zero CONNACK return/reason code.
type ConnRefusedError struct {
	ReasonCode      byte
	ProtocolVersion byte
}

func (e *ConnRefusedError) Error() string {
	return fmt.Sprintf("mqttclient: connection refused: %s (0x%02x)", connackReasonString(e.ProtocolVersion, e.ReasonCode), e.ReasonCode)
}

// connackReasonString returns readable text for a CONNACK code. MQTT 3.x uses
// its own return codes 1-5; MQTT 5 uses the shared reason code table.
func connackReasonString(version, code byte) string {
	if version < 5 {
		switch code {
		case 0:
			return "connection accepted"
		case 1:
			return "unacceptable protocol version"
		case 2:
			return "identifier rejected"
		case 3:
			return "server unavailable"
		case 4:
			return "bad user name or password"
		case 5:
			return "not authorized"
		}
		return "unknown return code"
	}
	return ReasonCodeString(code)
}

// ReasonCodeString returns the MQTT 5 specification text for a reason code.
// Codes 0x00-0x02 have several meanings depending on the packet type; the
// most general one is returned.
func ReasonCodeString(code byte) string {
	if s, ok := reasonCodes[code]; ok {
		return s
	}
	return "unknown reason code"
}

var reasonCodes = map[byte]string{
	0x00: "success",
	0x01: "granted qos 1",
	0x02: "granted qos 2",
	0x04: "disconnect with will message",
	0x10: "no matching subscribers",
	0x11: "no subscription existed",
	0x18: "continue authentication",
	0x19: "re-authenticate",
	0x80: "unspecified error",
	0x81: "malformed packet",
	0x82: "protocol error",
	0x83: "implementation specific error",
	0x84: "unsupported protocol version",
	0x85: "client identifier not valid",
	0x86: "bad user name or password",
	0x87: "not authorized",
	0x88: "server unavailable",
	0x89: "server busy",
	0x8A: "banned",
	0x8B: "server shutting down",
	0x8C: "bad authentication method",
	0x8D: "keep alive timeout",
	0x8E: "session taken over",
	0x8F: "topic filter invalid",
	0x90: "topic name invalid",
	0x91: "packet identifier in use",
	0x92: "packet identifier not found",
	0x93: "receive maximum exceeded",
	0x94: "topic alias invalid",
	0x95: "packet too large",
	0x96: "message rate too high",
	0x97: "quota exceeded",
	0x98: "administrative action",
	0x99: "payload format invalid",
	0x9A: "retain not supported",
	0x9B: "qos not supported",
	0x9C: "use another server",
	0x9D: "server moved",
	0x9E: "shared subscriptions not supported",
	0x9F: "connection rate exceeded",
	0xA0: "maximum connect time",
	0xA1: "subscription identifiers not supported",
	0xA2: "wildcard subscriptions not supported",
}

// Reason codes the client itself sends in DISCONNECT (MQTT 5).
const (
	reasonNormalDisconnection = 0x00
	reasonMalformedPacket     = 0x81
	reasonProtocolError       = 0x82
)
