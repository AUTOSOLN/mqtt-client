package main

import (
	"bufio"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/url"
	"strings"
	"time"

	"github.com/wind-c/comqtt/v2/mqtt/packets"
)

// rawConn is a bare MQTT connection: a socket, a read buffer and the run's
// protocol version. It applies none of the protocol state machine, so the
// caller controls exactly which bytes go out and in what order.
type rawConn struct {
	nc      net.Conn
	br      *bufio.Reader
	version byte
}

// dial opens a TCP (or TLS) connection to server. tc may be nil for plain
// TCP. The scheme picks the default port: 1883 for mqtt/tcp, 8883 for
// mqtts/ssl/tls.
func dial(ctx context.Context, server string, tc *tls.Config, timeout time.Duration, version byte) (*rawConn, error) {
	u, err := url.Parse(server)
	if err != nil {
		return nil, err
	}
	useTLS, port := false, "1883"
	switch strings.ToLower(u.Scheme) {
	case "mqtt", "tcp":
	case "mqtts", "ssl", "tls":
		useTLS, port = true, "8883"
	default:
		return nil, fmt.Errorf("unsupported scheme %q", u.Scheme)
	}
	if p := u.Port(); p != "" {
		port = p
	}
	addr := net.JoinHostPort(u.Hostname(), port)

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	var d net.Dialer
	nc, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("tcp dial %s: %w", addr, err)
	}
	if useTLS {
		if tc == nil {
			tc = &tls.Config{}
		}
		if tc.ServerName == "" {
			tc.ServerName = u.Hostname()
		}
		tconn := tls.Client(nc, tc)
		if err := tconn.HandshakeContext(ctx); err != nil {
			nc.Close()
			return nil, fmt.Errorf("tls handshake: %w", err)
		}
		nc = tconn
	}
	return &rawConn{nc: nc, br: bufio.NewReader(nc), version: version}, nil
}

func (rc *rawConn) close() { _ = rc.nc.Close() }

// writeBytes sends b verbatim within timeout.
func (rc *rawConn) writeBytes(b []byte, timeout time.Duration) error {
	_ = rc.nc.SetWriteDeadline(time.Now().Add(timeout))
	_, err := rc.nc.Write(b)
	return err
}

// inPacket is a summarized inbound packet for logging and expectations.
type inPacket struct {
	Type      byte
	Name      string
	Flags     byte
	Raw       []byte // the whole frame, header included
	PacketID  uint16
	Reason    byte
	ReasonSet bool
	Reasons   []byte // SUBACK/UNSUBACK per-filter codes
	Topic     string
	Payload   []byte // PUBLISH
	Session   bool
	Err       error // decode error, if the body did not parse
}

// readPacket reads one whole control packet within timeout. A read past the
// deadline returns a timeout error; a closed connection returns io.EOF (or
// io.ErrUnexpectedEOF mid-frame). The frame is decoded best-effort: a body
// that fails to parse still yields the type, flags and raw bytes.
func (rc *rawConn) readPacket(timeout time.Duration) (*inPacket, error) {
	_ = rc.nc.SetReadDeadline(time.Now().Add(timeout))

	hb, err := rc.br.ReadByte()
	if err != nil {
		return nil, err
	}
	n, _, err := packets.DecodeLength(rc.br)
	if err != nil {
		return nil, err
	}
	body := make([]byte, n)
	if _, err := io.ReadFull(rc.br, body); err != nil {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		return nil, err
	}

	raw := append([]byte{hb}, encodeLen(n)...)
	raw = append(raw, body...)

	in := &inPacket{
		Type:  hb >> 4,
		Flags: hb & 0x0F,
		Raw:   raw,
	}
	in.Name = packets.PacketNames[in.Type]
	if in.Name == "" {
		in.Name = fmt.Sprintf("type%d", in.Type)
	}

	pk := packets.Packet{
		FixedHeader:     packets.FixedHeader{Type: in.Type, Remaining: n},
		ProtocolVersion: rc.version,
	}
	in.Err = decodeInbound(&pk, body)
	in.PacketID = pk.PacketID
	in.Topic = pk.TopicName
	in.Payload = pk.Payload
	in.Session = pk.SessionPresent
	switch in.Type {
	case packets.Connack, packets.Disconnect, packets.Auth,
		packets.Puback, packets.Pubrec, packets.Pubrel, packets.Pubcomp:
		in.Reason, in.ReasonSet = pk.ReasonCode, true
	case packets.Suback, packets.Unsuback:
		in.Reasons = pk.ReasonCodes
	}
	return in, nil
}

// decodeInbound runs the body decoder for a server-origin packet, tolerating
// anything it does not recognise.
func decodeInbound(pk *packets.Packet, body []byte) error {
	switch pk.FixedHeader.Type {
	case packets.Connack:
		return pk.ConnackDecode(body)
	case packets.Publish:
		return pk.PublishDecode(body)
	case packets.Puback:
		return pk.PubackDecode(body)
	case packets.Pubrec:
		return pk.PubrecDecode(body)
	case packets.Pubrel:
		return pk.PubrelDecode(body)
	case packets.Pubcomp:
		return pk.PubcompDecode(body)
	case packets.Suback:
		return pk.SubackDecode(body)
	case packets.Unsuback:
		return pk.UnsubackDecode(body)
	case packets.Disconnect:
		if pk.ProtocolVersion == 5 && len(body) == 1 {
			pk.ReasonCode = body[0]
			return nil
		}
		return pk.DisconnectDecode(body)
	case packets.Auth:
		return pk.AuthDecode(body)
	case packets.Pingreq, packets.Pingresp:
		return nil
	}
	return nil
}

// encodeLen encodes a remaining-length as the MQTT variable byte integer.
func encodeLen(n int) []byte {
	var out []byte
	for {
		b := byte(n % 128)
		n /= 128
		if n > 0 {
			b |= 0x80
		}
		out = append(out, b)
		if n == 0 {
			return out
		}
	}
}
