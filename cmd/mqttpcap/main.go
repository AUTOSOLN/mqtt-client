// Command mqttpcap shows what a packet capture holds: the TCP connections
// in it, and the MQTT packets of one of them, with their headers. Use it
// to look at a capture a user sent, and to pick the connections to replay
// with the other tools' -replay and -replay-conn flags.
//
// It reads classic pcap and pcapng, either of them gzip-compressed, as
// written by tcpdump, dumpcap and Wireshark. It never prints passwords,
// and prints payloads only with -payload.
//
// Examples:
//
//	mqttpcap -list capture.pcap
//	mqttpcap -conn 0 capture.pcap
//	mqttpcap -conn 2 -payload capture.pcapng.gz
//	mqttpcap -conn 1 -v 5 mid-session.pcap
//
// Exit status: 0 when everything shown decoded without problems; 1 when a
// connection's streams end early (a gap or truncated frame), it is
// encrypted, or a packet is malformed; 2 for a setup error (bad arguments,
// or a file that isn't a capture it can read).
package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	mqttclient "github.com/AUTOSOLN/mqtt-client"
	"github.com/AUTOSOLN/mqtt-client/capture"
	"github.com/AUTOSOLN/mqtt-client/replay"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

type config struct {
	list    bool
	conn    int
	version string
	payload bool
	file    string
}

func run(args []string, stdout, stderr io.Writer) int {
	var cfg config
	fs := flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() {
		fmt.Fprintln(stderr, "usage: mqttpcap -list FILE | -conn N [-v VERSION] [-payload] FILE")
		fs.PrintDefaults()
	}
	fs.BoolVar(&cfg.list, "list", false, "list the TCP connections in the capture")
	fs.IntVar(&cfg.conn, "conn", -1, "print the MQTT packets of this connection (an index from -list)")
	fs.StringVar(&cfg.version, "v", "", "MQTT protocol version (31, 311 or 5) for a connection whose CONNECT wasn't captured")
	fs.BoolVar(&cfg.payload, "payload", false, "also print PUBLISH payloads and Will payloads")
	_ = fs.Parse(args)
	if fs.NArg() == 1 {
		cfg.file = fs.Arg(0)
	}
	version, err := cfg.check()
	if err != nil {
		fmt.Fprintln(stderr, "mqttpcap:", err)
		return 2
	}

	f, err := os.Open(cfg.file)
	if err != nil {
		fmt.Fprintln(stderr, "mqttpcap:", err)
		return 2
	}
	defer f.Close()
	c, err := capture.Read(f)
	if err != nil {
		fmt.Fprintln(stderr, "mqttpcap:", err)
		return 2
	}
	if cfg.list {
		return list(stdout, c, version)
	}
	if cfg.conn >= len(c.Conns) {
		fmt.Fprintf(stderr, "mqttpcap: -conn %d: the capture has %d connections\n", cfg.conn, len(c.Conns))
		return 2
	}
	return timeline(stdout, c.Conns[cfg.conn], version, cfg.payload)
}

// check validates the flags and returns the -v protocol version (0 if not
// given).
func (cfg *config) check() (byte, error) {
	switch {
	case cfg.file == "":
		return 0, errors.New("give one capture file")
	case cfg.list == (cfg.conn >= 0):
		return 0, errors.New("give either -list or -conn N")
	case cfg.list && cfg.payload:
		return 0, errors.New("-payload needs -conn")
	}
	switch cfg.version {
	case "":
		return 0, nil
	case "31", "3":
		return mqttclient.MQTT31, nil
	case "311", "4":
		return mqttclient.MQTT311, nil
	case "5":
		return mqttclient.MQTT5, nil
	}
	return 0, fmt.Errorf("unknown protocol version %q", cfg.version)
}

// list prints one line per connection.
func list(w io.Writer, c *capture.Capture, version byte) int {
	fmt.Fprintf(w, "%d connections, %d frames ignored (not IPv4 or IPv6 TCP)\n", len(c.Conns), c.Ignored)
	code := 0
	for i, conn := range c.Conns {
		var up, down int
		for _, s := range conn.Segments {
			if s.Dir == capture.FromClient {
				up += len(s.Data)
			} else {
				down += len(s.Data)
			}
		}
		line := fmt.Sprintf("%d  %v -> %v  client %d B, server %d B", i, conn.Client, conn.Server, up, down)
		if conn.Transport == capture.TLS {
			fmt.Fprintf(w, "%s  TLS (encrypted, not decoded)\n", line)
			code = 1
			continue
		}
		tl, err := replay.Decode(conn, version)
		if len(tl.Packets) > 0 && tl.Packets[0].Connect != nil {
			ci := tl.Packets[0].Connect
			line += fmt.Sprintf("  client_id=%q v=%d", ci.ClientID, ci.Version)
		}
		line += fmt.Sprintf("  %d packets", countPackets(tl))
		if bad := malformed(tl); bad > 0 {
			line += fmt.Sprintf("  %d MALFORMED", bad)
			code = 1
		}
		if err != nil {
			line += "  ERROR " + err.Error()
			code = 1
		}
		fmt.Fprintln(w, line)
	}
	return code
}

func countPackets(tl replay.Timeline) int {
	n := 0
	for _, p := range tl.Packets {
		if !p.Close {
			n++
		}
	}
	return n
}

func malformed(tl replay.Timeline) int {
	n := 0
	for _, p := range tl.Packets {
		if p.Err != nil {
			n++
		}
	}
	return n
}

// timeline prints the packets of one connection.
func timeline(w io.Writer, conn capture.Conn, version byte, payload bool) int {
	fmt.Fprintf(w, "%v -> %v\n", conn.Client, conn.Server)
	if conn.Transport == capture.TLS {
		fmt.Fprintln(w, "TLS (encrypted, not decoded)")
		return 1
	}
	tl, err := replay.Decode(conn, version)
	var start time.Time
	for _, p := range tl.Packets {
		if !p.Time.IsZero() {
			start = p.Time
			break
		}
	}
	code := 0
	for _, p := range tl.Packets {
		at := "-"
		if !p.Time.IsZero() {
			at = fmt.Sprintf("+%.6fs", p.Time.Sub(start).Seconds())
		}
		fmt.Fprintf(w, "%s %s %s%s\n", at, p.Dir, p.TypeName(), describe(p, payload))
		if p.Err != nil {
			code = 1
		}
	}
	if err != nil {
		fmt.Fprintln(w, "ERROR", err)
		code = 1
	}
	return code
}

// describe prints a packet's header fields, never a password.
func describe(p replay.Packet, payload bool) string {
	if p.Close {
		return ""
	}
	var b strings.Builder
	if p.Err != nil {
		fmt.Fprintf(&b, " MALFORMED: %v", p.Err)
		return b.String()
	}
	switch p.Type {
	case replay.Connect:
		c := p.Connect
		fmt.Fprintf(&b, " v=%d client_id=%q clean=%v keepalive=%d", c.Version, c.ClientID, c.Clean, c.KeepAlive)
		if w := c.Will; w != nil {
			fmt.Fprintf(&b, " will_topic=%q will_qos=%d will_retain=%v will_bytes=%d", w.Topic, w.QoS, w.Retain, len(w.Payload))
			if payload {
				fmt.Fprintf(&b, " will_payload=%s", strconv.Quote(string(w.Payload)))
			}
		}
	case replay.Publish:
		if p.QoS > 0 {
			fmt.Fprintf(&b, " id=%d", p.PacketID)
		}
		fmt.Fprintf(&b, " qos=%d retain=%v dup=%v topic=%q bytes=%d", p.QoS, p.Retain, p.Dup, p.Topic, len(p.Payload))
		if p.Properties != nil && p.Properties.TopicAliasFlag {
			fmt.Fprintf(&b, " topic_alias=%d", p.Properties.TopicAlias)
		}
		if payload {
			fmt.Fprintf(&b, " payload=%s", strconv.Quote(string(p.Payload)))
		}
	case replay.Subscribe, replay.Unsubscribe:
		fmt.Fprintf(&b, " id=%d", p.PacketID)
		for _, f := range p.Filters {
			if p.Type == replay.Subscribe {
				fmt.Fprintf(&b, " %q:qos%d", f.Topic, f.QoS)
			} else {
				fmt.Fprintf(&b, " %q", f.Topic)
			}
		}
	case replay.Puback, replay.Pubrec, replay.Pubrel, replay.Pubcomp, replay.Suback, replay.Unsuback:
		fmt.Fprintf(&b, " id=%d", p.PacketID)
	}
	if len(p.ReasonCodes) > 0 {
		fmt.Fprintf(&b, " reason=% x", p.ReasonCodes)
	}
	return b.String()
}
