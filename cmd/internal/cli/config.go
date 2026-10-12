package cli

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"os"

	"gopkg.in/yaml.v3"

	mqttclient "github.com/AUTOSOLN/mqtt-client"
)

// loadServers reads the -config file, a list of servers tried in order:
//
//	servers:
//	  - address: broker1:1883
//	    version: 5
//	    username: alice
//	    password: secret
//	  - address: broker2:8883
//	    tls: true
//	    cafile: ca.pem
//	    reconnect-delay: 2s
//
// The keys are the yaml tags of ConnFlags and ReconnectFlags. Each server
// starts from the flags, so a flag given on the command line (-v, -u,
// -tls, ...) is the default for every server that does not set that key.
// -address is the exception: every server must give its own address.
func (f *ConnFlags) loadServers() ([]mqttclient.Server, error) {
	if f.fs != nil {
		var err error
		f.fs.Visit(func(fl *flag.Flag) {
			if fl.Name == "address" {
				err = errors.New("-address and -config cannot be used together; list the addresses in the file")
			}
		})
		if err != nil {
			return nil, err
		}
	}
	raw, err := os.ReadFile(f.Config)
	if err != nil {
		return nil, err
	}
	var file struct {
		Servers []yaml.Node `yaml:"servers"`
	}
	if err := decodeStrict(raw, &file); err != nil {
		return nil, fmt.Errorf("%s: %w", f.Config, err)
	}
	if len(file.Servers) == 0 {
		return nil, fmt.Errorf("%s: no servers", f.Config)
	}
	var servers []mqttclient.Server
	for i := range file.Servers {
		node := &file.Servers[i]
		where := fmt.Sprintf("%s: server %d (line %d)", f.Config, i+1, node.Line)
		entry := *f
		entry.Address = ""
		b, err := yaml.Marshal(node)
		if err == nil {
			err = decodeStrict(b, &entry)
		}
		if err != nil {
			return nil, fmt.Errorf("%s: %w", where, err)
		}
		if entry.Address == "" {
			return nil, fmt.Errorf("%s: no address", where)
		}
		srv, err := entry.Server()
		if err != nil {
			return nil, fmt.Errorf("%s: %w", where, err)
		}
		servers = append(servers, srv)
	}
	return servers, nil
}

// decodeStrict decodes YAML into v, rejecting keys v has no field for, so a
// misspelt key is an error rather than silently ignored.
func decodeStrict(b []byte, v any) error {
	dec := yaml.NewDecoder(bytes.NewReader(b))
	dec.KnownFields(true)
	return dec.Decode(v)
}
