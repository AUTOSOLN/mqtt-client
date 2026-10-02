package mqttclient

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"errors"
	"io"
	"testing"

	"github.com/wind-c/comqtt/v2/mqtt/packets"
)

func tlsConfig() *tls.Config { return &tls.Config{} }

func read(t *testing.T, version byte, maxSize uint32, b ...byte) (packets.Packet, error) {
	t.Helper()
	return readPacket(bufio.NewReader(bytes.NewReader(b)), version, maxSize)
}

func TestReadPacket(t *testing.T) {
	t.Run("v5 disconnect reason code only", func(t *testing.T) {
		pk, err := read(t, 5, 0, 0xE0, 0x01, 0x8B)
		if err != nil || pk.ReasonCode != 0x8B {
			t.Fatalf("pk %+v err %v", pk.ReasonCode, err)
		}
	})
	t.Run("v3 connack on v5 connection", func(t *testing.T) {
		pk, err := read(t, 5, 0, 0x20, 0x02, 0x00, 0x01)
		if err != nil || pk.ProtocolVersion != 4 || pk.ReasonCode != 1 {
			t.Fatalf("pk v%d rc %d err %v", pk.ProtocolVersion, pk.ReasonCode, err)
		}
	})
	t.Run("v311 connack wrong length", func(t *testing.T) {
		if _, err := read(t, 4, 0, 0x20, 0x03, 0x00, 0x00, 0x00); !errors.Is(err, ErrMalformedPacket) {
			t.Fatal(err)
		}
	})
	t.Run("connack invalid flags", func(t *testing.T) {
		if _, err := read(t, 4, 0, 0x20, 0x02, 0x04, 0x00); !errors.Is(err, ErrProtocol) {
			t.Fatal(err)
		}
	})
	t.Run("connect from server", func(t *testing.T) {
		if _, err := read(t, 4, 0, 0x10, 0x00); !errors.Is(err, ErrProtocol) {
			t.Fatal(err)
		}
	})
	t.Run("bad fixed header flags", func(t *testing.T) {
		if _, err := read(t, 4, 0, 0xD1, 0x00); !errors.Is(err, ErrMalformedPacket) {
			t.Fatal(err)
		}
	})
	t.Run("malformed remaining length", func(t *testing.T) {
		if _, err := read(t, 4, 0, 0xD0, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F); !errors.Is(err, ErrMalformedPacket) {
			t.Fatal(err)
		}
	})
	t.Run("oversize", func(t *testing.T) {
		if _, err := read(t, 5, 4, 0xE0, 0x03, 0x00, 0x00, 0x00); !errors.Is(err, ErrOversizePacket) {
			t.Fatal(err)
		}
	})
	t.Run("truncated body", func(t *testing.T) {
		if _, err := read(t, 4, 0, 0x20, 0x02, 0x00); !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatal(err)
		}
	})
	t.Run("eof between packets", func(t *testing.T) {
		if _, err := read(t, 4, 0); err != io.EOF {
			t.Fatal(err)
		}
	})
}

// The CONNECT bytes match libmosquitto's for the same settings (these are
// the packets mosquitto's test/lib expects).
func TestEncodeConnect(t *testing.T) {
	cases := []struct {
		name string
		p    connectParams
		want []byte
	}{
		{"v311 clean", connectParams{version: 4, clientID: "id", cleanStart: true, keepAlive: 60},
			[]byte{0x10, 0x0E, 0, 4, 'M', 'Q', 'T', 'T', 4, 0x02, 0, 60, 0, 2, 'i', 'd'}},
		{"v5 default receive maximum", connectParams{version: 5, clientID: "id", cleanStart: true, keepAlive: 60, receiveMaximum: 20},
			[]byte{0x10, 0x12, 0, 4, 'M', 'Q', 'T', 'T', 5, 0x02, 0, 60, 3, 0x21, 0, 20, 0, 2, 'i', 'd'}},
		{"will retain needs retain available", connectParams{version: 4, clientID: "id", keepAlive: 60,
			will: &Message{Topic: "t", Payload: []byte("p"), QoS: 1, Retain: true}},
			[]byte{0x10, 0x14, 0, 4, 'M', 'Q', 'T', 'T', 4, 0x0C, 0, 60, 0, 2, 'i', 'd', 0, 1, 't', 0, 1, 'p'}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pk := newConnect(tc.p)
			got, err := encodePacket(&pk)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, tc.want) {
				t.Fatalf("got  % x\nwant % x", got, tc.want)
			}
		})
	}
}
