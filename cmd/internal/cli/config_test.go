package cli

import (
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	mqttclient "github.com/AUTOSOLN/mqtt-client"
)

// parse registers the connection flags and parses args, with -config
// pointing at a file holding yaml.
func parse(t *testing.T, yaml string, args ...string) *ConnFlags {
	t.Helper()
	path := filepath.Join(t.TempDir(), "servers.yaml")
	if err := os.WriteFile(path, []byte(yaml), 0o600); err != nil {
		t.Fatal(err)
	}
	var f ConnFlags
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	f.Register(fs)
	if err := fs.Parse(append([]string{"-config", path}, args...)); err != nil {
		t.Fatal(err)
	}
	return &f
}

func TestConfigServers(t *testing.T) {
	f := parse(t, `
servers:
  - address: one:1883
  - address: two:1884
    version: 5
    username: bob
    session-expiry: 60
    reconnect-delay: 2s
`, "-u", "alice", "-P", "pw")
	opts, err := f.Options()
	if err != nil {
		t.Fatal(err)
	}
	if len(opts.Servers) != 2 {
		t.Fatalf("got %d servers", len(opts.Servers))
	}
	one, two := opts.Servers[0], opts.Servers[1]
	if one.Address != "one:1883" || one.ProtocolVersion != mqttclient.MQTT311 || one.Username != "alice" || string(one.Password) != "pw" {
		t.Errorf("server 1 does not take the flags as defaults: %+v", one)
	}
	if one.ReconnectDelay != time.Second || one.ReconnectDelayMax != 30*time.Second {
		t.Errorf("server 1 reconnect delay %v..%v, want the flag defaults", one.ReconnectDelay, one.ReconnectDelayMax)
	}
	if two.Address != "two:1884" || two.ProtocolVersion != mqttclient.MQTT5 || two.Username != "bob" || string(two.Password) != "pw" {
		t.Errorf("server 2 does not override the flags: %+v", two)
	}
	if two.ConnectProperties == nil || two.ConnectProperties.SessionExpiryInterval != 60 || two.ReconnectDelay != 2*time.Second {
		t.Errorf("server 2 session expiry or reconnect delay not set: %+v", two)
	}
}

func TestConfigTLS(t *testing.T) {
	f := parse(t, `
servers:
  - address: one:8883
    tls: true
    insecure: true
    servername: broker.example
  - address: two:1883
`)
	opts, err := f.Options()
	if err != nil {
		t.Fatal(err)
	}
	one, two := opts.Servers[0], opts.Servers[1]
	if !one.TLS || one.TLSConfig == nil || !one.TLSConfig.InsecureSkipVerify || one.TLSConfig.ServerName != "broker.example" {
		t.Errorf("server 1 TLS settings not applied: tls=%v config=%+v", one.TLS, one.TLSConfig)
	}
	if two.TLS || two.TLSConfig != nil {
		t.Errorf("server 2 has TLS settings from server 1: tls=%v config=%+v", two.TLS, two.TLSConfig)
	}
}

func TestConfigErrors(t *testing.T) {
	for _, tc := range []struct {
		name, yaml string
		args       []string
		want       string
	}{
		{"no servers", "servers: []\n", nil, "no servers"},
		{"unknown key", "servers:\n  - address: a:1\n    pasword: x\n", nil, "pasword"},
		{"unknown top-level key", "server:\n  - address: a:1\n", nil, "server"},
		{"no address", "servers:\n  - version: 5\n", nil, "server 1 (line 2): no address"},
		{"bad server", "servers:\n  - address: a:1\n  - address: b:1\n    version: 6\n", nil, "server 2 (line 3): unknown protocol version"},
		{"with -address", "servers:\n  - address: a:1\n", []string{"-address", "b:1"}, "-address and -config"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := parse(t, tc.yaml, tc.args...).Options()
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want it to contain %q", err, tc.want)
			}
		})
	}
}
