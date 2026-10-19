// Package clitest is a scripted MQTT 3.1.1 broker for testing the tools
// under cmd without a real one: it accepts CONNECT, acknowledges PUBLISH at
// every QoS, grants every SUBSCRIBE and then sends its Messages.
package clitest

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"
	"net"
	"regexp"
	"slices"
	"strings"
	"sync"
	"testing"
)

// Broker answers every connection the same way.
type Broker struct {
	ConnackCode    byte     // CONNACK return code; non-zero refuses and closes
	SessionPresent bool     // CONNACK Session Present
	Messages       [][]byte // QoS 0 payloads sent on the first filter of a SUBSCRIBE, after the SUBACK
	Drop           bool     // close the connection after the Messages, without DISCONNECT

	ln net.Listener
}

// Start listens on a free local port until the test ends.
func (b *Broker) Start(t *testing.T) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	b.ln = ln
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			nc, err := ln.Accept()
			if err != nil {
				return
			}
			go b.serve(nc)
		}
	}()
}

// Addr is the host:port to connect to.
func (b *Broker) Addr() string { return b.ln.Addr().String() }

func (b *Broker) serve(nc net.Conn) {
	defer nc.Close()
	br := bufio.NewReader(nc)
	for {
		hdr, body, err := readPacket(br)
		if err != nil {
			return
		}
		switch hdr >> 4 {
		case 1: // CONNECT
			sp := byte(0)
			if b.SessionPresent {
				sp = 1
			}
			nc.Write([]byte{0x20, 2, sp, b.ConnackCode})
			if b.ConnackCode != 0 {
				return
			}
		case 3: // PUBLISH
			qos := hdr >> 1 & 3
			n := 2 + int(binary.BigEndian.Uint16(body))
			switch qos {
			case 1:
				nc.Write([]byte{0x40, 2, body[n], body[n+1]}) // PUBACK
			case 2:
				nc.Write([]byte{0x50, 2, body[n], body[n+1]}) // PUBREC
			}
		case 6: // PUBREL
			nc.Write([]byte{0x70, 2, body[0], body[1]}) // PUBCOMP
		case 8: // SUBSCRIBE
			b.suback(nc, body)
			if b.Drop {
				return
			}
		case 12: // PINGREQ
			nc.Write([]byte{0xd0, 0})
		case 14: // DISCONNECT
			return
		}
	}
}

// suback grants each filter its requested QoS, then sends Messages on the
// first filter.
func (b *Broker) suback(nc net.Conn, body []byte) {
	ack := []byte{body[0], body[1]}
	var first string
	for rest := body[2:]; len(rest) > 2; {
		n := 2 + int(binary.BigEndian.Uint16(rest))
		if first == "" {
			first = string(rest[2:n])
		}
		ack = append(ack, rest[n])
		rest = rest[n+1:]
	}
	nc.Write(packet(0x90, ack))
	for _, m := range b.Messages {
		pub := binary.BigEndian.AppendUint16(nil, uint16(len(first)))
		pub = append(append(pub, first...), m...)
		nc.Write(packet(0x30, pub))
	}
}

func packet(hdr byte, body []byte) []byte {
	b := []byte{hdr}
	for n := len(body); ; {
		c := byte(n % 128)
		n /= 128
		if n > 0 {
			c |= 0x80
		}
		b = append(b, c)
		if n == 0 {
			break
		}
	}
	return append(b, body...)
}

func readPacket(br *bufio.Reader) (byte, []byte, error) {
	hdr, err := br.ReadByte()
	if err != nil {
		return 0, nil, err
	}
	n, shift := 0, 0
	for {
		c, err := br.ReadByte()
		if err != nil {
			return 0, nil, err
		}
		n |= int(c&0x7f) << shift
		if c&0x80 == 0 {
			break
		}
		shift += 7
	}
	body := make([]byte, n)
	_, err = io.ReadFull(br, body)
	return hdr, body, err
}

var volatile = regexp.MustCompile(`^\d\d:\d\d:\d\d\.\d{3} |\d[\d.]*(µs|ms|s)\b|\d+ msg/s`)

// Log returns the lines of a tool's log without their timestamps, with
// durations and rates replaced by "<T>", sorted because lines logged by
// different goroutines interleave.
func Log(s string) []string {
	lines := strings.Split(strings.TrimSuffix(s, "\n"), "\n")
	for i, l := range lines {
		lines[i] = volatile.ReplaceAllStringFunc(l, func(m string) string {
			if strings.HasSuffix(m, " ") {
				return "" // timestamp
			}
			return "<T>"
		})
	}
	slices.Sort(lines)
	return lines
}

// Buffer is a bytes.Buffer safe for the tools' concurrent logging.
type Buffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (b *Buffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.Write(p)
}

func (b *Buffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.String()
}
