package main

import (
	"bytes"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/AUTOSOLN/mqtt-client/capture/capturetest"
	"github.com/AUTOSOLN/mqtt-client/cmd/internal/clitest"
)

func runTool(args ...string) (code int, stdout, stderr string) {
	var out, errOut clitest.Buffer
	code = run(args, &out, &errOut)
	return code, out.String(), errOut.String()
}

// hostCapture has one MQTT session, its CONNECT carrying a password.
func hostCapture(t *testing.T, tls bool) string {
	t.Helper()
	c := clitest.NewCapture(t)
	s := c.Session("10.0.0.1:50000")
	s.Client(clitest.ConnectAuth("sub1", "user", "s3cret-pw")).Server(clitest.Connack())
	s.Client(clitest.Subscribe(1, "t/#", 1)).Server(clitest.Suback(1, 1))
	s.Server(clitest.Publish("t/a", 1, 1, []byte("one"))).Client(clitest.Puback(1))
	s.Client(clitest.Disconnect()).ClientClose()
	if tls {
		c.Session("10.0.0.3:40000").Client([]byte{0x16, 0x03, 0x01, 0x00, 0x02, 1, 2})
	}
	return c.Write()
}

func TestList_PlainConnection_Exit0(t *testing.T) {
	code, stdout, stderr := runTool("-list", hostCapture(t, false))
	if code != 0 {
		t.Fatalf("exit %d, want 0\n%s", code, stderr)
	}
	want := `0  10.0.0.1:50000 -> 10.0.0.2:1883  client`
	if !strings.Contains(stdout, want) || !strings.Contains(stdout, `client_id="sub1" v=4  7 packets`) {
		t.Errorf("got\n%s\nwant a line for connection 0 with client_id=\"sub1\" v=4 and 7 packets", stdout)
	}
}

func TestList_TLSConnection_ListedAndExit1(t *testing.T) {
	code, stdout, _ := runTool("-list", hostCapture(t, true))
	if code != 1 || !strings.Contains(stdout, "1  10.0.0.3:40000 -> 10.0.0.2:1883") || !strings.Contains(stdout, "TLS (encrypted, not decoded)") {
		t.Errorf("exit %d\n%s\nwant 1 and connection 1 listed as TLS", code, stdout)
	}
}

func TestConn_Timeline_HeadersPayloadsNoPassword(t *testing.T) {
	code, stdout, stderr := runTool("-conn", "0", "-payload", hostCapture(t, false))
	if code != 0 {
		t.Fatalf("exit %d, want 0\n%s", code, stderr)
	}
	for _, want := range []string{
		`client CONNECT v=4 client_id="sub1" clean=true keepalive=60`,
		`server CONNACK reason=00`,
		`client SUBSCRIBE id=1 "t/#":qos1`,
		`server PUBLISH id=1 qos=1 retain=false dup=false topic="t/a" bytes=3 payload="one"`,
		`client PUBACK id=1`,
		`client close`,
	} {
		if !strings.Contains(stdout, want) {
			t.Errorf("output lacks %q:\n%s", want, stdout)
		}
	}
	if strings.Contains(stdout, "s3cret-pw") || strings.Contains(stderr, "s3cret-pw") {
		t.Errorf("the password was printed:\n%s", stdout)
	}
}

func TestConn_WithoutPayloadFlag_NoPayload(t *testing.T) {
	_, stdout, _ := runTool("-conn", "0", hostCapture(t, false))
	if strings.Contains(stdout, `"one"`) {
		t.Errorf("payload printed without -payload:\n%s", stdout)
	}
}

func TestConn_GapInCapture_Exit1(t *testing.T) {
	c := netip.MustParseAddrPort("10.0.0.1:50000")
	s := netip.MustParseAddrPort("10.0.0.2:1883")
	at := time.Date(2026, 11, 17, 12, 0, 0, 0, time.UTC)
	var buf bytes.Buffer
	w, err := capturetest.NewWriter(&buf)
	if err != nil {
		t.Fatal(err)
	}
	for _, seg := range []struct {
		src, dst netip.AddrPort
		seq      uint32
		flags    byte
		data     []byte
	}{
		{c, s, 1000, capturetest.SYN, nil},
		{s, c, 5000, capturetest.SYN | capturetest.ACK, nil},
		{c, s, 1001, capturetest.ACK, clitest.Connect("sub1")},
		{c, s, 1100, capturetest.ACK, clitest.Disconnect()}, // bytes in between never captured
	} {
		if err := w.TCP(at, seg.src, seg.dst, seg.seq, 0, seg.flags, seg.data); err != nil {
			t.Fatal(err)
		}
	}
	path := filepath.Join(t.TempDir(), "gap.pcap")
	if err := os.WriteFile(path, buf.Bytes(), 0o644); err != nil {
		t.Fatal(err)
	}

	code, stdout, _ := runTool("-conn", "0", path)
	if code != 1 || !strings.Contains(stdout, "bytes missing") {
		t.Errorf("exit %d\n%s\nwant 1 and an error about missing bytes", code, stdout)
	}
}

func TestArgs_Invalid_Exit2(t *testing.T) {
	path := hostCapture(t, false)
	notCapture := filepath.Join(t.TempDir(), "notes.txt")
	if err := os.WriteFile(notCapture, []byte("not a capture"), 0o644); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{
		{"-list"},
		{"-list", "-conn", "0", path},
		{path},
		{"-conn", "0", "-v", "6", path},
		{"-list", "-payload", path},
		{"-conn", "5", path},
		{"-list", notCapture},
	} {
		if code, _, _ := runTool(args...); code != 2 {
			t.Errorf("%v: exit %d, want 2", args, code)
		}
	}
}
