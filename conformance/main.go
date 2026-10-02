// Command conformance runs this client against libmosquitto's client test
// suite (mosquitto/test/lib). Each case is a Go port of the C program
// test/lib/c/<name>.c; the Python script <name>.py plays the broker and
// checks the bytes the client sends.
//
// It is a multi-call binary: run.sh links it as c/<name>.test and
// cpp/<name>.test, and the case is chosen from the program name. The first
// argument is the port to connect to.
package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	mqttclient "github.com/AUTOSOLN/mqtt-client"
)

// cases maps a mosquitto test name to its client program.
var cases = map[string]func(port int) int{
	"01-con-discon-success": func(port int) int {
		return connectDisconnect(port, mqttclient.Options{ClientID: "01-con-discon-success", CleanStart: true, KeepAlive: 60}, nil)
	},
	"01-con-discon-success-v5": func(port int) int {
		return connectDisconnect(port, mqttclient.Options{
			ClientID: "01-con-discon-success-v5", CleanStart: true, KeepAlive: 60, ProtocolVersion: mqttclient.MQTT5,
			ConnectProperties: &mqttclient.Properties{MaximumPacketSize: 1000},
		}, nil)
	},
	"01-con-discon-will": func(port int) int {
		return connectDisconnect(port, mqttclient.Options{
			ClientID: "01-con-discon-will", CleanStart: true, KeepAlive: 60,
			Will: &mqttclient.Message{Topic: "will/topic", Payload: []byte("will-payload"), QoS: 1, Retain: true},
		}, nil)
	},
	"01-con-discon-will-v5": func(port int) int {
		return connectDisconnect(port, mqttclient.Options{
			ClientID: "01-con-discon-will", CleanStart: true, KeepAlive: 60, ProtocolVersion: mqttclient.MQTT5,
			Will: &mqttclient.Message{Topic: "will/topic", Payload: []byte("will-payload"), QoS: 1, Retain: true,
				Properties: &mqttclient.Properties{PayloadFormat: 1, PayloadFormatFlag: true}},
		}, nil)
	},
	"01-con-discon-will-clear": func(port int) int {
		return connectDisconnect(port, mqttclient.Options{
			ClientID: "01-con-discon-will", CleanStart: true, KeepAlive: 60,
			Will: &mqttclient.Message{Topic: "will/topic", Payload: []byte("will-payload"), QoS: 1, Retain: true},
		}, func(c *mqttclient.Client) error { return c.SetWill(nil) })
	},
	"01-keepalive-pingreq": func(port int) int {
		return connectStay(port, mqttclient.Options{ClientID: "01-keepalive-pingreq", CleanStart: true, KeepAlive: 5})
	},
	"01-server-keepalive-pingreq": func(port int) int {
		return connectStay(port, mqttclient.Options{
			ClientID: "01-server-keepalive-pingreq", CleanStart: true, KeepAlive: 60, ProtocolVersion: mqttclient.MQTT5,
		})
	},
	"01-no-clean-session": func(port int) int {
		return connectDisconnect(port, mqttclient.Options{ClientID: "01-no-clean-session", KeepAlive: 60}, nil)
	},
	"01-unpwd-set": func(port int) int {
		return connectDisconnect(port, mqttclient.Options{ClientID: "01-unpwd-set", CleanStart: true, KeepAlive: 60}, func(c *mqttclient.Client) error {
			return c.SetCredentials("uname", []byte(";'[08gn=#"))
		})
	},
	"01-will-set": func(port int) int {
		return connectDisconnect(port, mqttclient.Options{
			ClientID: "01-will-set", CleanStart: true, KeepAlive: 60,
			Will: &mqttclient.Message{Topic: "topic/on/unexpected/disconnect", Payload: []byte("will message"), QoS: 1, Retain: true},
		}, nil)
	},
	"01-will-unpwd-set": func(port int) int {
		return connectDisconnect(port, mqttclient.Options{
			ClientID: "01-will-unpwd-set", CleanStart: true, KeepAlive: 60,
			Username: "oibvvwqw", Password: []byte("#'^2hg9a&nm38*us"),
			Will: &mqttclient.Message{Topic: "will-topic", Payload: []byte("will message"), QoS: 2},
		}, nil)
	},
	"01-pre-connect-callback": func(port int) int {
		return run(port, mqttclient.Options{ClientID: "01-pre-connect", CleanStart: true, KeepAlive: 60}, nil, mqttclient.Handlers{
			OnPreConnect: func(c *mqttclient.Client) { _ = c.SetCredentials("uname", []byte(";'[08gn=#")) },
		}, false)
	},
}

// connectDisconnect is the shape of most 01-* programs: connect, and on a
// successful CONNACK disconnect again. setup runs after New, before Connect.
func connectDisconnect(port int, opts mqttclient.Options, setup func(*mqttclient.Client) error) int {
	return run(port, opts, setup, mqttclient.Handlers{}, false)
}

// connectStay connects and stays connected until the test broker closes the
// connection.
func connectStay(port int, opts mqttclient.Options) int {
	return run(port, opts, nil, mqttclient.Handlers{}, true)
}

func run(port int, opts mqttclient.Options, setup func(*mqttclient.Client) error, h mqttclient.Handlers, stay bool) int {
	opts.Server = fmt.Sprintf("mqtt://localhost:%d", port)
	done := make(chan int, 1)
	h.OnConnect = func(c *mqttclient.Client, ack mqttclient.ConnAck) {
		if ack.ReasonCode != 0 {
			os.Exit(1)
		}
		if !stay {
			_ = c.Disconnect(context.Background(), 0, nil)
		}
	}
	h.OnDisconnect = func(_ *mqttclient.Client, ev mqttclient.DisconnectEvent) {
		if ev.Err != nil {
			fmt.Fprintln(os.Stderr, "disconnected:", ev.Err)
			done <- 1
			return
		}
		done <- 0
	}
	c, err := mqttclient.New(opts, h)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	if setup != nil {
		if err := setup(c); err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := c.Connect(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "connect:", err)
		// OnDisconnect follows if the network connection was established.
		select {
		case rc := <-done:
			return rc
		case <-time.After(time.Second):
			return 1
		}
	}
	return <-done
}

func main() {
	name := strings.TrimSuffix(filepath.Base(os.Args[0]), ".test")
	if len(os.Args) == 2 && os.Args[1] == "-list" {
		for n := range cases {
			fmt.Println(n)
		}
		return
	}
	f, ok := cases[name]
	if !ok || len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "usage: <case>.test PORT (unknown case %q)\n", name)
		os.Exit(1)
	}
	port, err := strconv.Atoi(os.Args[1])
	if err != nil {
		fmt.Fprintln(os.Stderr, "bad port:", err)
		os.Exit(1)
	}
	os.Exit(f(port))
}
