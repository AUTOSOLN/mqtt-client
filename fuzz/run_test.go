package fuzz

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/wind-c/comqtt/v2/mqtt/packets"
)

// fakeServer accepts one connection and hands it to script.
func fakeServer(t *testing.T, script func(c *serverConn)) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		nc, err := ln.Accept()
		if err != nil {
			return
		}
		defer nc.Close()
		script(&serverConn{t: t, nc: nc, br: bufio.NewReader(nc)})
	}()
	return ln.Addr().String()
}

type serverConn struct {
	t  *testing.T
	nc net.Conn
	br *bufio.Reader
}

// read returns the next frame from the client: its first byte and body.
func (c *serverConn) read() (byte, []byte) {
	_ = c.nc.SetReadDeadline(time.Now().Add(5 * time.Second))
	hb, err := c.br.ReadByte()
	if err != nil {
		return 0, nil
	}
	n, _, err := packets.DecodeLength(c.br)
	if err != nil {
		return 0, nil
	}
	body := make([]byte, n)
	if _, err := io.ReadFull(c.br, body); err != nil {
		return 0, nil
	}
	return hb, body
}

func (c *serverConn) readConnect(version byte) packets.Packet {
	hb, body := c.read()
	pk := packets.Packet{FixedHeader: packets.FixedHeader{Type: hb >> 4}, ProtocolVersion: version}
	if err := pk.ConnectDecode(body); err != nil {
		c.t.Errorf("CONNECT decode: %v", err)
	}
	return pk
}

func (c *serverConn) send(b ...byte) { _, _ = c.nc.Write(b) }

func parse(t *testing.T, src string) *Scenario {
	t.Helper()
	sc, err := Parse("test.yaml", []byte(src), nil)
	if err != nil {
		t.Fatal(err)
	}
	return sc
}

func TestRunConnectWithDefaultCredentials(t *testing.T) {
	got := make(chan packets.Packet, 1)
	addr := fakeServer(t, func(c *serverConn) {
		got <- c.readConnect(5)
		c.send(0x20, 0x03, 0x00, 0x00, 0x00)
	})
	sc := parse(t, `
version: 5
steps:
  - {type: connect, client-id: c1}
  - {type: recv, expect: connack, reason: 0}
`)
	res, err := sc.Run(context.Background(), RunOptions{Address: addr, Username: "u", Password: "p"})
	if err != nil {
		t.Fatalf("%v\n%s", err, res.JSON())
	}
	pk := <-got
	if string(pk.Connect.Username) != "u" || string(pk.Connect.Password) != "p" || pk.Connect.ClientIdentifier != "c1" {
		t.Fatalf("CONNECT %+v", pk.Connect)
	}
	if rc := res.Steps[1].Received[0].ReasonCode; rc == nil || *rc != 0 {
		t.Fatalf("CONNACK reason %v", rc)
	}
}

func TestRunExplicitEmptyUsername(t *testing.T) {
	got := make(chan packets.Packet, 1)
	addr := fakeServer(t, func(c *serverConn) { got <- c.readConnect(4) })
	sc := parse(t, `
steps:
  - {type: connect, client-id: c1, username: ""}
`)
	if _, err := sc.Run(context.Background(), RunOptions{Address: addr, Username: "u", Password: "p"}); err != nil {
		t.Fatal(err)
	}
	if pk := <-got; pk.Connect.UsernameFlag || pk.Connect.PasswordFlag {
		t.Fatalf("CONNECT sent credentials: %+v", pk.Connect)
	}
}

func TestRunReasons(t *testing.T) {
	script := func(c *serverConn) {
		c.readConnect(5)
		c.send(0x20, 0x03, 0x00, 0x00, 0x00)
		c.read()
		c.send(0x90, 0x05, 0x00, 0x01, 0x00, 0x00, 0x8F) // SUBACK 00 8F
		time.Sleep(time.Second)
	}
	const src = `
version: 5
steps:
  - {type: connect, client-id: c1}
  - {type: recv, expect: connack}
  - {type: subscribe, packet-id: 1, filters: [a, "a/#/b"]}
  - {type: recv, expect: suback, reasons: [%s]}
`
	sc := parse(t, strings.Replace(src, "%s", "0x00, 0x8F", 1))
	if res, err := sc.Run(context.Background(), RunOptions{Address: fakeServer(t, script)}); err != nil {
		t.Fatalf("%v\n%s", err, res.JSON())
	}

	sc = parse(t, strings.Replace(src, "%s", "0x00, 0x80", 1))
	res, err := sc.Run(context.Background(), RunOptions{Address: fakeServer(t, script)})
	if !errors.Is(err, ErrExpectation) || errors.Is(err, ErrSetup) {
		t.Fatalf("err %v, want ErrExpectation", err)
	}
	if f := res.Steps[3].Failure; !strings.Contains(f, "00 80") || !strings.Contains(f, "00 8f") {
		t.Fatalf("failure %q", f)
	}
}

func TestRunExpectCloseReason(t *testing.T) {
	withDisconnect := func(c *serverConn) {
		c.readConnect(5)
		c.send(0x20, 0x03, 0x00, 0x00, 0x00)
		c.send(0xE0, 0x01, 0x8D)
	}
	plainClose := func(c *serverConn) {
		c.readConnect(5)
		c.send(0x20, 0x03, 0x00, 0x00, 0x00)
	}
	refused := func(c *serverConn) {
		c.readConnect(5)
		c.send(0x20, 0x03, 0x00, 0x84, 0x00)
	}
	cases := []struct {
		name   string
		script func(*serverConn)
		step   string
		ok     bool
	}{
		{"disconnect reason matches", withDisconnect, "{type: expect-close, reason: 0x8D}", true},
		{"disconnect reason differs", withDisconnect, "{type: expect-close, reason: 0x81}", false},
		{"recv expect close drains the disconnect", withDisconnect, "{type: recv, expect: close, reason: 0x8D}", true},
		{"plain close, reason required", plainClose, "{type: expect-close, reason: 0x8D}", false},
		{"plain close, reason optional", plainClose, "{type: expect-close, reason: 0x8D, reason-optional: true}", true},
		{"disconnect, reason optional differs", withDisconnect, "{type: expect-close, reason: 0x81, reason-optional: true}", false},
		{"refusing connack", refused, "{type: expect-close, reason: 0x84}", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sc := parse(t, "version: 5\nsteps:\n  - {type: connect, client-id: c1}\n  - "+tc.step+"\n")
			res, err := sc.Run(context.Background(), RunOptions{Address: fakeServer(t, tc.script)})
			if tc.ok != (err == nil) {
				t.Fatalf("err %v, want ok=%t\n%s", err, tc.ok, res.JSON())
			}
			if cl := res.Closed(); cl == nil || cl.Step != 2 {
				t.Fatalf("Closed() = %+v, want step 2", cl)
			}
		})
	}
}

// After the server closes, sending steps are skipped and the run still
// completes with the close recorded.
func TestRunSkipsSendsAfterClose(t *testing.T) {
	addr := fakeServer(t, func(c *serverConn) { c.read() })
	sc := parse(t, `
steps:
  - {type: disconnect}
  - {type: expect-close, timeout: 3s}
  - {type: pingreq}
  - {type: recv, expect: close}
`)
	res, err := sc.Run(context.Background(), RunOptions{Address: addr})
	if err != nil {
		t.Fatalf("%v\n%s", err, res.JSON())
	}
	if !res.Steps[2].Skipped || res.Steps[2].Sent != 0 {
		t.Fatalf("pingreq step %+v, want skipped", res.Steps[2])
	}
	if res.Closed().Step != 2 || res.Steps[1].CloseReason != nil {
		t.Fatalf("closed at %+v", res.Closed())
	}
}

func TestRunSetupError(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()
	sc := parse(t, "steps:\n  - {type: pingreq}\n")
	res, err := sc.Run(context.Background(), RunOptions{Address: addr, DialTimeout: time.Second})
	if !errors.Is(err, ErrSetup) || errors.Is(err, ErrExpectation) {
		t.Fatalf("err %v, want ErrSetup", err)
	}
	if res.Error == "" || res.OK() {
		t.Fatalf("result %+v", res)
	}
}

func TestRunDialHook(t *testing.T) {
	addr := fakeServer(t, func(c *serverConn) { c.read() })
	var dialled string
	sc := parse(t, "steps:\n  - {type: pingreq}\n")
	_, err := sc.Run(context.Background(), RunOptions{Address: "broker.invalid:1883", Dial: func(ctx context.Context, network, a string) (net.Conn, error) {
		dialled = a
		var d net.Dialer
		return d.DialContext(ctx, network, addr)
	}})
	if err != nil || dialled != "broker.invalid:1883" {
		t.Fatalf("err %v, dialled %q", err, dialled)
	}
}

func TestRunVarsAndRepeat(t *testing.T) {
	ids := make(chan uint16, 10)
	addr := fakeServer(t, func(c *serverConn) {
		for range 3 {
			hb, body := c.read()
			pk := packets.Packet{FixedHeader: packets.FixedHeader{Type: hb >> 4, Qos: (hb >> 1) & 3}, ProtocolVersion: 4}
			if err := pk.PublishDecode(body); err != nil {
				t.Error(err)
			}
			if pk.TopicName != "t/run" {
				t.Errorf("topic %q", pk.TopicName)
			}
			ids <- pk.PacketID
		}
	})
	sc, err := Parse("test.yaml", []byte(`
vars: {topic: t/load}
steps:
  - {type: publish, topic: "${topic}", qos: 1, packet-id: 65535, repeat: 3}
`), nil)
	if err != nil {
		t.Fatal(err)
	}
	res, err := sc.Run(context.Background(), RunOptions{Address: addr, Vars: map[string]string{"topic": "t/run"}})
	if err != nil {
		t.Fatal(err)
	}
	if a, b, c := <-ids, <-ids, <-ids; a != 65535 || b != 1 || c != 2 {
		t.Fatalf("packet ids %d %d %d, want 65535 1 2", a, b, c)
	}
	if res.Steps[0].Sent == 0 {
		t.Fatal("Sent not counted")
	}
}

func TestResultJSON(t *testing.T) {
	code := byte(0x8F)
	res := Result{Scenario: "s", Steps: []StepResult{{
		Step: 1, Type: "recv", Start: 1500 * time.Millisecond, Elapsed: 20 * time.Millisecond,
		Received: []Packet{{Type: "Suback", ReasonCodes: Codes{0, 0x8F}}, {Type: "Disconnect", ReasonCode: &code}},
	}}}
	var back map[string]any
	if err := json.Unmarshal([]byte(res.JSON()), &back); err != nil {
		t.Fatal(err)
	}
	step := back["steps"].([]any)[0].(map[string]any)
	if step["start_ms"] != 1500.0 || step["elapsed_ms"] != 20.0 {
		t.Fatalf("timings %v %v", step["start_ms"], step["elapsed_ms"])
	}
	recv := step["received"].([]any)
	if codes := recv[0].(map[string]any)["reason_codes"].([]any); codes[1] != 143.0 {
		t.Fatalf("reason_codes %v", codes)
	}
	if rc := recv[1].(map[string]any)["reason_code"]; rc != 143.0 {
		t.Fatalf("reason_code %v", rc)
	}
}

func TestPatchConnect(t *testing.T) {
	level, flags := uint8(6), uint8(0x03)
	b, err := encodeStep(&Step{Type: "connect", ClientID: "c", ProtocolLevel: &level, ConnectFlags: &flags}, 5)
	if err != nil {
		t.Fatal(err)
	}
	// 10 len 00 04 'M' 'Q' 'T' 'T' level flags ...
	if b[7] != 'T' || b[8] != 6 || b[9] != 0x03 {
		t.Fatalf("CONNECT % x", b)
	}
}

func TestParseRejects(t *testing.T) {
	for _, src := range []string{
		"steps: []",
		"steps:\n  - {type: nope}",
		"steps:\n  - {type: recv, expect: nope}",
		"steps:\n  - {type: publish, topic: a, reasons: [1]}",
		"steps:\n  - {type: recv, reason-optional: true}",
		"steps:\n  - {type: sleep, repeat: 2}",
		"steps:\n  - {type: publish, topic: a, protocol-level: 6}",
		"version: 6\nsteps:\n  - {type: pingreq}",
		"steps:\n  - {type: publish, topic: ${missing}}",
	} {
		if _, err := Parse("x", []byte(src), nil); err == nil {
			t.Errorf("parsed %q", src)
		}
	}
}
