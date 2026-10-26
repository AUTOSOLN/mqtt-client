// Command mqttfuzz drives a broker with MQTT control packets in an order you
// choose, for hardening and edge-case probing. A YAML scenario lists the
// packets to send, their parameters (with ${token} substitution) and what, if
// anything, to expect back. The tool writes every packet verbatim over a raw
// connection and applies no client-side state machine, so it can send orders
// the protocol forbids: UNSUBSCRIBE with no prior SUBSCRIBE, DISCONNECT before
// CONNECT, two CONNECTs, PUBLISH before CONNACK, reserved-bit and malformed
// frames, and so on.
//
// It is a wrapper around the fuzz package, which tests can import to run
// the same scenarios in-process.
//
// Usage:
//
//	mqttfuzz -f scenario.yaml [-address host:port] [-tls] [-v 5] [-set name=value ...]
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
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/AUTOSOLN/mqtt-client/cmd/internal/cli"
	"github.com/AUTOSOLN/mqtt-client/fuzz"
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
		address     = flag.String("address", "", "broker address as host:port (overrides the scenario's address:, default localhost:1883)")
		useTLS      = flag.Bool("tls", false, "connect with TLS (overrides the scenario's tls:)")
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

	sc, err := fuzz.Load(*file, sets)
	if err != nil {
		return setupErr(err)
	}

	opts := fuzz.RunOptions{
		Address:      *address,
		RecvTimeout:  *recvTimeout,
		DialTimeout:  *dialTimeout,
		WriteTimeout: *writeTO,
		Logf:         func(format string, args ...any) { fmt.Fprintf(os.Stderr, format+"\n", args...) },
	}
	if *version != "" {
		if opts.Version, err = fuzz.ParseVersion(*version); err != nil {
			return setupErr(err)
		}
	}
	flag.Visit(func(f *flag.Flag) {
		if f.Name == "tls" {
			sc.TLS = *useTLS // -tls=false turns off the scenario's tls: too
		}
	})
	if opts.TLSConfig, err = tlsFlags.TLSConfig(); err != nil {
		return setupErr(err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	_, err = sc.Run(ctx, opts)
	switch {
	case errors.Is(err, fuzz.ErrExpectation):
		return 1
	case err != nil:
		return 2 // Run logged it
	}
	return 0
}

// setupErr reports an error before the run, with exit status 2.
func setupErr(err error) int {
	fmt.Fprintln(os.Stderr, "mqttfuzz:", err)
	return 2
}

// registerTLS adds only the TLS-related flags from cli.ConnFlags; the rest of
// a connection is described by the scenario.
func registerTLS(f *cli.ConnFlags) {
	flag.StringVar(&f.CAFile, "cafile", "", "PEM file of CA certificates to trust (TLS)")
	flag.StringVar(&f.CertFile, "cert", "", "PEM client certificate (TLS)")
	flag.StringVar(&f.KeyFile, "key", "", "PEM client private key (TLS)")
	flag.BoolVar(&f.Insecure, "insecure", false, "skip server certificate verification (TLS)")
	flag.StringVar(&f.ServerName, "servername", "", "TLS server name, if different from the address host")
}
