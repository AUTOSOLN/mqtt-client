// Package integration tests the client against a real mosquitto broker that
// the tests start, kill and restart. The broker is a private process on a
// free port; tests skip when no mosquitto binary is found (set MOSQUITTO to
// its path, or put it on PATH).
package integration

import (
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

type broker struct {
	t    *testing.T
	bin  string
	conf string
	addr string
	cmd  *exec.Cmd
}

func mosquittoBinary(t *testing.T) string {
	t.Helper()
	if p := os.Getenv("MOSQUITTO"); p != "" {
		return p
	}
	if p, err := exec.LookPath("mosquitto"); err == nil {
		return p
	}
	for _, p := range []string{"/usr/sbin/mosquitto", "/usr/local/sbin/mosquitto"} {
		if _, err := os.Stat(p); err == nil {
			return p
		}
	}
	t.Skip("mosquitto not found; set MOSQUITTO")
	return ""
}

// newBroker configures a broker on a free port; persistence keeps sessions
// across a graceful stop.
func newBroker(t *testing.T, persistence bool) *broker {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().(*net.TCPAddr)
	ln.Close()

	dir := t.TempDir()
	conf := fmt.Sprintf("listener %d 127.0.0.1\nallow_anonymous true\n", addr.Port)
	if persistence {
		conf += fmt.Sprintf("persistence true\npersistence_location %s/\n", dir)
	}
	path := filepath.Join(dir, "mosquitto.conf")
	if err := os.WriteFile(path, []byte(conf), 0o644); err != nil {
		t.Fatal(err)
	}
	b := &broker{t: t, bin: mosquittoBinary(t), conf: path, addr: addr.String()}
	t.Cleanup(b.kill)
	return b
}

func (b *broker) url() string { return "mqtt://" + b.addr }

// start runs the broker and waits until it accepts connections.
func (b *broker) start() {
	b.t.Helper()
	b.cmd = exec.Command(b.bin, "-c", b.conf)
	if testing.Verbose() {
		b.cmd.Stderr = os.Stderr
	}
	if err := b.cmd.Start(); err != nil {
		b.t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if nc, err := net.Dial("tcp", b.addr); err == nil {
			nc.Close()
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	b.t.Fatalf("mosquitto did not start on %s", b.addr)
}

// kill stops the broker abruptly (SIGKILL): connections drop without
// DISCONNECT and nothing is saved.
func (b *broker) kill() {
	if b.cmd == nil {
		return
	}
	_ = b.cmd.Process.Kill()
	_ = b.cmd.Wait()
	b.cmd = nil
}

// stop shuts the broker down cleanly (SIGTERM), saving persistent sessions.
func (b *broker) stop() {
	b.t.Helper()
	if b.cmd == nil {
		return
	}
	_ = b.cmd.Process.Signal(syscall.SIGTERM)
	_ = b.cmd.Wait()
	b.cmd = nil
}
