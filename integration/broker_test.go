// Package integration tests the client against a real mosquitto broker that
// the tests start, kill and restart. The broker is a private process on a
// free port; tests skip when no mosquitto binary is found (set MOSQUITTO to
// its path, or put it on PATH).
package integration

import (
	"bytes"
	"fmt"
	"io"
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

	// The last process started; each start replaces them.
	stderr *bytes.Buffer // read only after exited is closed
	exited chan struct{} // closed when the process has exited
	err    error         // exit status, set before exited is closed
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

func (b *broker) address() string { return b.addr }

// start runs the broker and waits until it accepts connections. If mosquitto
// exits or does not listen within 5s, the test fails with its exit status and
// output.
func (b *broker) start() {
	b.t.Helper()
	b.stderr = new(bytes.Buffer)
	b.cmd = exec.Command(b.bin, "-c", b.conf)
	// mosquitto logs to stderr by default.
	b.cmd.Stderr = b.stderr
	if testing.Verbose() {
		b.cmd.Stderr = io.MultiWriter(b.stderr, os.Stderr)
	}
	if err := b.cmd.Start(); err != nil {
		b.t.Fatal(err)
	}
	// One goroutine owns Wait; kill and stop wait for it to finish.
	b.exited = make(chan struct{})
	go func(cmd *exec.Cmd, exited chan<- struct{}) {
		b.err = cmd.Wait()
		close(exited)
	}(b.cmd, b.exited)

	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	for {
		if nc, err := net.Dial("tcp", b.addr); err == nil {
			nc.Close()
			return
		}
		select {
		case <-tick.C:
		case <-b.exited:
			b.fail("mosquitto exited before listening on %s: %v", b.addr, b.err)
			return
		case <-deadline.C:
			b.kill()
			b.fail("mosquitto did not listen on %s within 5s", b.addr)
			return
		}
	}
}

// fail ends the test with mosquitto's output. The process must have exited.
func (b *broker) fail(format string, args ...any) {
	b.t.Helper()
	b.t.Fatalf(format+"\nconfig %s:\n%s\nmosquitto output:\n%s",
		append(args, b.conf, readFile(b.conf), b.stderr)...)
}

// readFile returns the file's contents, or the error if it can't be read.
func readFile(path string) string {
	data, err := os.ReadFile(path)
	if err != nil {
		return err.Error()
	}
	return string(data)
}

// kill stops the broker abruptly (SIGKILL): connections drop without
// DISCONNECT and nothing is saved.
func (b *broker) kill() {
	b.signal(os.Kill)
}

// stop shuts the broker down cleanly (SIGTERM), saving persistent sessions.
func (b *broker) stop() {
	b.signal(syscall.SIGTERM)
}

// signal sends sig and waits for the broker to exit. It does nothing if the
// broker isn't running.
func (b *broker) signal(sig os.Signal) {
	if b.cmd == nil {
		return
	}
	// Signal fails only if the process has already exited, which the
	// receive below handles.
	_ = b.cmd.Process.Signal(sig)
	<-b.exited
	b.cmd = nil
}
