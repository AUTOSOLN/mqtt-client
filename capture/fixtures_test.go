package capture_test

import (
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"net/netip"
	"os"
	"path/filepath"
	"testing"

	"github.com/AUTOSOLN/mqtt-client/capture"
	"github.com/AUTOSOLN/mqtt-client/capture/capturetest"
	"github.com/AUTOSOLN/mqtt-client/replay"
)

// TestRead_RealFixtures reads the captures in testdata, recorded by real
// tools (see testdata/README.md), and checks the exchange they all hold.
func TestRead_RealFixtures(t *testing.T) {
	for _, name := range []string{
		"any.pcap",
		"any-sll2.pcap",
		"lo-nano.pcap",
		"lo-nano.pcap.gz",
		"dumpcap-2if.pcapng",
		"windows-npcap.pcapng",
	} {
		t.Run(name, func(t *testing.T) {
			f, err := os.Open(filepath.Join("testdata", name))
			if errors.Is(err, fs.ErrNotExist) {
				t.Skipf("capture/testdata/%s is missing; record it as capture/testdata/README.md says", name)
			}
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()
			c, err := capture.Read(f)
			if err != nil {
				t.Fatalf("Read: %v", err)
			}
			if err := checkFixtureExchange(c); err != nil {
				t.Error(err)
			}
		})
	}
}

// checkFixtureExchange checks that a capture holds the fixture exchange:
// fixture-pub publishes three QoS 1 "hello" messages to fixture/a, and
// fixture-sub receives three.
func checkFixtureExchange(c *capture.Capture) error {
	published, received := -1, -1
	for i, conn := range c.Conns {
		if conn.Err != nil || conn.Transport != capture.Plain {
			return errors.Join(conn.Err, errors.New("connection is not plain and complete"))
		}
		tl, err := replay.Decode(conn, 0)
		if err != nil {
			return err
		}
		if len(tl.Packets) == 0 || tl.Packets[0].Connect == nil {
			return fmt.Errorf("connection %d doesn't start with CONNECT", i)
		}
		n := 0
		for _, p := range tl.Packets {
			if p.Err != nil {
				return p.Err
			}
			if p.Type == replay.Publish && p.Topic == "fixture/a" && string(p.Payload) == "hello" && p.QoS == 1 {
				n++
			}
		}
		switch tl.Packets[0].Connect.ClientID {
		case "fixture-pub":
			published = n
		case "fixture-sub":
			received = n
		}
	}
	if published != 3 || received != 3 {
		return fmt.Errorf("fixture-pub published %d and fixture-sub received %d; want 3 and 3 (-1: no such client)", published, received)
	}
	return nil
}

// connectPacket is an MQTT 3.1.1 CONNECT with a clean session.
func connectPacket(id string) []byte {
	body := append([]byte{0, 4, 'M', 'Q', 'T', 'T', 4, 2, 0, 60, 0, byte(len(id))}, id...)
	return append([]byte{0x10, byte(len(body))}, body...)
}

// publishPacket is a QoS 1 PUBLISH.
func publishPacket(topic string, id byte, payload string) []byte {
	body := append([]byte{0, byte(len(topic))}, topic...)
	body = append(body, 0, id)
	body = append(body, payload...)
	return append([]byte{0x32, byte(len(body))}, body...)
}

// The checker the real fixtures go through, on a capture of the same
// exchange built here.
func TestCheckFixtureExchange_BuiltExchange_Passes(t *testing.T) {
	broker := server
	pub := capture.Conn{Client: client, Server: broker, Segments: []capture.Segment{
		{Dir: capture.FromClient, Time: at(1), Data: connectPacket("fixture-pub")},
		{Dir: capture.FromServer, Time: at(2), Data: []byte{0x20, 2, 0, 0}},
	}}
	sub := capture.Conn{Client: netip.MustParseAddrPort("10.0.0.3:40000"), Server: broker, Segments: []capture.Segment{
		{Dir: capture.FromClient, Time: at(1), Data: connectPacket("fixture-sub")},
		{Dir: capture.FromServer, Time: at(2), Data: []byte{0x20, 2, 0, 0}},
	}}
	for i := byte(1); i <= 3; i++ {
		pub.Segments = append(pub.Segments,
			capture.Segment{Dir: capture.FromClient, Time: at(int(i) + 10), Data: publishPacket("fixture/a", i, "hello")},
			capture.Segment{Dir: capture.FromServer, Time: at(int(i) + 11), Data: []byte{0x40, 2, 0, i}})
		sub.Segments = append(sub.Segments,
			capture.Segment{Dir: capture.FromServer, Time: at(int(i) + 12), Data: publishPacket("fixture/a", i, "hello")},
			capture.Segment{Dir: capture.FromClient, Time: at(int(i) + 13), Data: []byte{0x40, 2, 0, i}})
	}
	var buf bytes.Buffer
	must(t, capturetest.Write(&buf, []capture.Conn{sub, pub}))
	c, err := capture.Read(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if err := checkFixtureExchange(c); err != nil {
		t.Errorf("got %v, want nil", err)
	}
	c.Conns = c.Conns[:1] // without fixture-pub
	if err := checkFixtureExchange(c); err == nil {
		t.Errorf("without the publisher: got nil, want an error")
	}
}
