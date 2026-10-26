package fuzz

import (
	"bufio"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
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

// dialFunc opens a network connection, as net.Dialer.DialContext does.
type dialFunc func(ctx context.Context, network, address string) (net.Conn, error)

// dial opens a TCP connection to addr (host:port) with dialFn, or a
// net.Dialer when it is nil, then a TLS handshake when useTLS is set. tc
// may be nil; it is not modified.
func dial(ctx context.Context, dialFn dialFunc, addr string, useTLS bool, tc *tls.Config, timeout time.Duration, version byte) (*rawConn, error) {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, err // already names the address
	}

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	if dialFn == nil {
		var d net.Dialer
		dialFn = d.DialContext
	}
	nc, err := dialFn(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("tcp dial %s: %w", addr, err)
	}
	if useTLS {
		if tc == nil {
			tc = &tls.Config{}
		}
		tc = tc.Clone()
		if tc.ServerName == "" {
			tc.ServerName = host
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

// readPacket reads one whole control packet within timeout. A read past the
// deadline returns a timeout error; a closed connection returns io.EOF (or
// io.ErrUnexpectedEOF mid-frame). The frame is decoded best-effort: a body
// that fails to parse still yields the type, flags and raw bytes.
func (rc *rawConn) readPacket(timeout time.Duration) (*Packet, error) {
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

	typ := hb >> 4
	in := &Packet{
		Type:  packets.PacketNames[typ],
		Flags: hb & 0x0F,
		Size:  len(raw),
		Raw:   raw,
		typ:   typ,
	}
	if in.Type == "" {
		in.Type = fmt.Sprintf("type%d", typ)
	}

	pk := packets.Packet{
		FixedHeader:     packets.FixedHeader{Type: typ, Remaining: n},
		ProtocolVersion: rc.version,
	}
	if err := decodeInbound(&pk, body); err != nil {
		in.DecodeError = err.Error()
	}
	in.PacketID = pk.PacketID
	in.Topic = pk.TopicName
	in.Payload = pk.Payload
	in.PayloadSize = len(pk.Payload)
	in.SessionPresent = pk.SessionPresent
	switch typ {
	case packets.Connack, packets.Disconnect, packets.Auth,
		packets.Puback, packets.Pubrec, packets.Pubrel, packets.Pubcomp:
		code := pk.ReasonCode
		in.ReasonCode = &code
	case packets.Suback, packets.Unsuback:
		in.ReasonCodes = pk.ReasonCodes
	}
	if rc.version == 5 {
		in.Properties = summarize(&pk.Properties)
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
