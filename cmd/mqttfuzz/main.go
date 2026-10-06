// Command mqttfuzz drives a broker with MQTT control packets in an order you
// choose, for hardening and edge-case probing. A YAML scenario lists the
// packets to send, their parameters (with ${token} substitution) and what, if
// anything, to expect back. The tool writes every packet verbatim over a raw
// connection and applies no client-side state machine, so it can send orders
// the protocol forbids: UNSUBSCRIBE with no prior SUBSCRIBE, DISCONNECT before
// CONNECT, two CONNECTs, PUBLISH before CONNACK, reserved-bit and malformed
// frames, and so on.
//
// Usage:
//
//	mqttfuzz -f scenario.yaml [-server URL] [-v 5] [-set name=value ...]
//
// Tokens: write ${name} anywhere in the scenario. Values come from the
// scenario's vars:, overridden by -set name=value. Built-ins: ${rand} (a
// random hex suffix), ${pid}, ${time} (unix seconds), ${date}.
//
// Exit status: 0 when every expectation held, 1 when one failed, 2 for a
// setup or I/O error.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/AUTOSOLN/mqtt-client/cmd/internal/cli"
)

type setFlag map[string]string

func (s setFlag) String() string { return "" }
func (s setFlag) Set(v string) error {
	k, val, ok := strings.Cut(v, "=")
	if !ok {
		return fmt.Errorf("want name=value, got %q", v)
	}
	s[k] = val
	return nil
}

func main() { os.Exit(run()) }

func run() int {
	var (
		file        = flag.String("f", "", "scenario YAML file (required)")
		server      = flag.String("server", "", "broker URL (overrides the scenario's server:, default mqtt://localhost:1883)")
		version     = flag.String("v", "", "protocol version 31|311|5 (overrides the scenario's version:)")
		recvTimeout = flag.Duration("recv-timeout", 2*time.Second, "default wait for a recv/expect-close step")
		dialTimeout = flag.Duration("dial-timeout", 10*time.Second, "connection (and TLS handshake) timeout")
		writeTO     = flag.Duration("write-timeout", 5*time.Second, "per-packet write timeout")
		sets        = setFlag{}
	)
	flag.Var(sets, "set", "token override name=value (repeatable)")
	var tlsFlags cli.ConnFlags
	registerTLS(&tlsFlags)
	flag.Parse()

	if *file == "" {
		fmt.Fprintln(os.Stderr, "mqttfuzz: -f scenario.yaml is required")
		return 2
	}

	sc, err := loadScenario(*file, sets)
	if err != nil {
		fmt.Fprintln(os.Stderr, "mqttfuzz:", err)
		return 2
	}

	ver := sc.Version
	if *version != "" {
		ver = *version
	}
	verByte, err := parseVersion(ver)
	if err != nil {
		fmt.Fprintln(os.Stderr, "mqttfuzz:", err)
		return 2
	}

	addr := sc.Server
	if *server != "" {
		addr = *server
	}
	if addr == "" {
		addr = "mqtt://localhost:1883"
	}

	tc, err := tlsFlags.TLSConfig()
	if err != nil {
		fmt.Fprintln(os.Stderr, "mqttfuzz:", err)
		return 2
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	logf("mqttfuzz: %s  v%s  %d step(s)", addr, versionName(verByte), len(sc.Steps))
	rc, err := dial(ctx, addr, tc, *dialTimeout, verByte)
	if err != nil {
		fmt.Fprintln(os.Stderr, "mqttfuzz:", err)
		return 2
	}
	defer rc.close()

	r := &runner{rc: rc, recvTimeout: *recvTimeout, writeTO: *writeTO}
	if err := r.run(ctx, sc, verByte); err != nil {
		logf("ABORT: %v", err)
		return 2
	}
	if r.failed {
		logf("RESULT: expectations FAILED")
		return 1
	}
	logf("RESULT: OK")
	return 0
}

// registerTLS adds only the TLS-related flags from cli.ConnFlags; the rest of
// a connection is described by the scenario.
func registerTLS(f *cli.ConnFlags) {
	flag.StringVar(&f.CAFile, "cafile", "", "PEM file of CA certificates to trust (TLS)")
	flag.StringVar(&f.CertFile, "cert", "", "PEM client certificate (TLS)")
	flag.StringVar(&f.KeyFile, "key", "", "PEM client private key (TLS)")
	flag.BoolVar(&f.Insecure, "insecure", false, "skip server certificate verification (TLS)")
	flag.StringVar(&f.ServerName, "servername", "", "TLS server name, if different from the URL host")
}

func versionName(v byte) string {
	switch v {
	case 3:
		return "3.1"
	case 4:
		return "3.1.1"
	case 5:
		return "5"
	}
	return fmt.Sprint(v)
}
