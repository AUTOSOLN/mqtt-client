package main

// Ports of the 03-* (publish) and 11-* (properties) programs. Each follows
// its C original in test/lib/c: the handlers make the same calls, and a
// program ends where the C one sets run = 0 or calls exit.

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"

	mqttclient "github.com/AUTOSOLN/mqtt-client"
)

var done = make(chan int, 1)

// finish ends the program with rc; later calls are ignored.
func finish(rc int) {
	select {
	case done <- rc:
	default:
	}
}

// client connects with opts and h and waits for finish. Without an
// OnDisconnect handler, a clean disconnect finishes with 0 and a lost
// connection with 1.
func client(port int, opts mqttclient.Options, h mqttclient.Handlers) int {
	opts.Server = fmt.Sprintf("mqtt://localhost:%d", port)
	opts.CleanStart = true
	if opts.KeepAlive == 0 {
		opts.KeepAlive = 60
	}
	if h.OnDisconnect == nil {
		h.OnDisconnect = func(_ *mqttclient.Client, ev mqttclient.DisconnectEvent) {
			if ev.Err != nil {
				fmt.Fprintln(os.Stderr, "disconnected:", ev.Err)
				finish(1)
				return
			}
			finish(0)
		}
	}
	c, err := mqttclient.New(opts, h)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	if err := connect(c); err != nil {
		fmt.Fprintln(os.Stderr, "connect:", err)
		return 1
	}
	return <-done
}

// runForever is client for the programs that use mosquitto_loop_forever:
// Run keeps the connection up, reconnecting after a loss, and the program
// ends when Run returns (after Disconnect).
func runForever(port int, opts mqttclient.Options, h mqttclient.Handlers) int {
	opts.Server = fmt.Sprintf("mqtt://localhost:%d", port)
	opts.CleanStart = true
	if opts.KeepAlive == 0 {
		opts.KeepAlive = 60
	}
	c, err := mqttclient.New(opts, h)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	if err := c.Run(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, "run:", err)
		return 1
	}
	return 0
}

// starter is client or runForever.
type starter func(port int, opts mqttclient.Options, h mqttclient.Handlers) int

func connect(c *mqttclient.Client) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := c.Connect(ctx)
	return err
}

func publish(c *mqttclient.Client, topic, payload string, qos byte, props *mqttclient.Properties) (*mqttclient.Pending, error) {
	return c.Publish(context.Background(), &mqttclient.Message{Topic: topic, Payload: []byte(payload), QoS: qos, Properties: props})
}

func disconnect(c *mqttclient.Client) {
	_ = c.Disconnect(context.Background(), 0, nil)
}

// expectMessage checks a received message the way the b2c programs do.
func expectMessage(m *mqttclient.Message, mid uint16, qos byte, topic, payload string) error {
	switch {
	case m.Mid != mid:
		return fmt.Errorf("invalid mid (%d)", m.Mid)
	case m.QoS != qos:
		return fmt.Errorf("invalid qos (%d)", m.QoS)
	case m.Topic != topic:
		return fmt.Errorf("invalid topic (%s)", m.Topic)
	case string(m.Payload) != payload:
		return fmt.Errorf("invalid payload (%s)", m.Payload)
	case m.Retain:
		return errors.New("invalid retain (1)")
	}
	return nil
}

// receive is a b2c program: check the first message, then call next.
func receive(port int, opts mqttclient.Options, mid uint16, qos byte, topic string, next func(c *mqttclient.Client)) int {
	return client(port, opts, mqttclient.Handlers{
		OnMessage: func(c *mqttclient.Client, m *mqttclient.Message) {
			if err := expectMessage(m, mid, qos, topic, "message"); err != nil {
				fmt.Println(err)
				os.Exit(1)
			}
			next(c)
		},
	})
}

// unexpectedAck is the shape of the unexpected-PUBACK/PUBCOMP programs: stay
// connected (the script checks the client ignores the acknowledgement and
// still sends PINGREQ) until the connection ends.
func unexpectedAck(port int, id string) int {
	return client(port, mqttclient.Options{ClientID: id, KeepAlive: 5}, mqttclient.Handlers{
		OnDisconnect: func(*mqttclient.Client, mqttclient.DisconnectEvent) { finish(0) },
	})
}

// publishThenDisconnect publishes one message on connect and disconnects
// once it is complete.
func publishThenDisconnect(port int, opts mqttclient.Options, topic, payload string, qos byte, props *mqttclient.Properties) int {
	return client(port, opts, mqttclient.Handlers{
		OnConnect: func(c *mqttclient.Client, _ mqttclient.ConnAck) {
			if _, err := publish(c, topic, payload, qos, props); err != nil {
				fmt.Fprintln(os.Stderr, "publish:", err)
				os.Exit(1)
			}
		},
		OnPublish: func(c *mqttclient.Client, _ uint16, _ byte, _ *mqttclient.Properties) { disconnect(c) },
	})
}

// publishMany publishes n messages on connect and finishes when the last is
// complete, as the receive-maximum programs do.
func publishMany(port int, opts mqttclient.Options, n int, qos byte) int {
	return client(port, opts, mqttclient.Handlers{
		OnConnect: func(c *mqttclient.Client, _ mqttclient.ConnAck) {
			for range n {
				if _, err := publish(c, "topic", "12345", qos, nil); err != nil {
					fmt.Fprintln(os.Stderr, "publish:", err)
					os.Exit(1)
				}
			}
		},
		OnPublish: func(c *mqttclient.Client, mid uint16, _ byte, _ *mqttclient.Properties) {
			if int(mid) == n {
				disconnect(c)
			}
		},
	})
}

// publishReconnect publishes once on the first connection and disconnects
// when it is complete. The script drops the connection before that, so Run
// reconnects and resends the message (mosquitto_loop_forever).
func publishReconnect(port int, id string, qos byte, topic string) int {
	first := true
	return runForever(port, mqttclient.Options{ClientID: id}, mqttclient.Handlers{
		OnConnect: func(c *mqttclient.Client, _ mqttclient.ConnAck) {
			if first {
				first = false
				if _, err := publish(c, topic, "message", qos, nil); err != nil {
					fmt.Fprintln(os.Stderr, "publish:", err)
					os.Exit(1)
				}
			}
		},
		OnPublish: func(c *mqttclient.Client, _ uint16, _ byte, _ *mqttclient.Properties) { disconnect(c) },
	})
}

// maximumQoS publishes at QoS 2, 1 and 0 against a server with Maximum QoS
// max, checking which are refused, and disconnects when the last is done.
func maximumQoS(port int, max byte) int {
	rc := 0
	return client(port, mqttclient.Options{ClientID: "publish-qos2-test", ProtocolVersion: mqttclient.MQTT5}, mqttclient.Handlers{
		OnConnect: func(c *mqttclient.Client, _ mqttclient.ConnAck) {
			for _, qos := range []byte{2, 1, 0} {
				_, err := publish(c, "maximum/qos/qos"+strconv.Itoa(int(qos)), "message", qos, nil)
				if want := qos > max; want != errors.Is(err, mqttclient.ErrQoSNotSupported) || (!want && err != nil) {
					fmt.Fprintf(os.Stderr, "qos %d: %v\n", qos, err)
					rc = 1
				}
			}
		},
		OnPublish: func(c *mqttclient.Client, mid uint16, _ byte, _ *mqttclient.Properties) {
			// Refused messages take no mid, so the QoS 0 one is the last.
			if mid == uint16(max)+1 {
				disconnect(c)
			}
		},
		OnDisconnect: func(*mqttclient.Client, mqttclient.DisconnectEvent) { finish(rc) },
	})
}

// sendOnce publishes one QoS 0 message and disconnects when OnPublish
// reports its mid.
func sendOnce(start starter, port int, opts mqttclient.Options, topic, payload string, props *mqttclient.Properties) int {
	var sent uint16
	return start(port, opts, mqttclient.Handlers{
		OnConnect: func(c *mqttclient.Client, _ mqttclient.ConnAck) {
			p, err := publish(c, topic, payload, 0, props)
			if err != nil {
				fmt.Fprintln(os.Stderr, "publish:", err)
				os.Exit(1)
			}
			sent = p.Mid
		},
		OnPublish: func(c *mqttclient.Client, mid uint16, _ byte, _ *mqttclient.Properties) {
			if mid != sent {
				os.Exit(1)
			}
			disconnect(c)
		},
	})
}

var v5 = mqttclient.MQTT5

func init() {
	add := map[string]func(port int) int{
		"03-publish-b2c-qos1-unexpected-puback":  func(port int) int { return unexpectedAck(port, "publish-qos1-test") },
		"03-publish-b2c-qos2-unexpected-pubcomp": func(port int) int { return unexpectedAck(port, "publish-qos2-test") },
		"03-publish-b2c-qos1": func(port int) int {
			return receive(port, mqttclient.Options{ClientID: "publish-qos1-test"}, 123, 1, "pub/qos1/receive", func(*mqttclient.Client) { finish(0) })
		},
		"03-publish-b2c-qos2": func(port int) int {
			return receive(port, mqttclient.Options{ClientID: "publish-qos2-test"}, 13423, 2, "pub/qos2/receive", func(*mqttclient.Client) { finish(0) })
		},
		"03-publish-b2c-qos2-len": func(port int) int {
			return receive(port, mqttclient.Options{ClientID: "publish-qos2-test", ProtocolVersion: v5}, 56, 2, "len/qos2/test", disconnect)
		},
		"03-publish-b2c-qos2-unexpected-pubrel": func(port int) int {
			return client(port, mqttclient.Options{ClientID: "publish-qos2-test"}, mqttclient.Handlers{
				OnMessage: func(_ *mqttclient.Client, m *mqttclient.Message) {
					if m.Topic == "quit" {
						finish(0)
						return
					}
					if err := expectMessage(m, 13423, 2, "pub/qos2/receive", "message"); err != nil {
						fmt.Println(err)
						os.Exit(1)
					}
				},
			})
		},
		"03-publish-c2b-qos1-disconnect": func(port int) int {
			return publishReconnect(port, "publish-qos1-test", 1, "pub/qos1/test")
		},
		"03-publish-c2b-qos2-disconnect": func(port int) int {
			return publishReconnect(port, "publish-qos2-test", 2, "pub/qos2/test")
		},
		"03-publish-c2b-qos1-len": func(port int) int {
			return publishThenDisconnect(port, mqttclient.Options{ClientID: "publish-qos1-test", ProtocolVersion: v5}, "pub/qos1/test", "message", 1, nil)
		},
		"03-publish-c2b-qos2-len": func(port int) int {
			return publishThenDisconnect(port, mqttclient.Options{ClientID: "publish-qos2-test", ProtocolVersion: v5}, "pub/qos2/test", "message", 2, nil)
		},
		"03-publish-c2b-qos2": func(port int) int {
			return publishThenDisconnect(port, mqttclient.Options{ClientID: "publish-qos2-test"}, "pub/qos2/test", "message", 2, nil)
		},
		"03-publish-c2b-qos1-receive-maximum": func(port int) int {
			return publishMany(port, mqttclient.Options{ClientID: "publish-qos1-test", ProtocolVersion: v5}, 6, 1)
		},
		"03-publish-c2b-qos2-receive-maximum": func(port int) int {
			return publishMany(port, mqttclient.Options{ClientID: "publish-qos2-test", ProtocolVersion: v5}, 5, 2)
		},
		"03-publish-c2b-qos2-maximum-qos-0": func(port int) int { return maximumQoS(port, 0) },
		"03-publish-c2b-qos2-maximum-qos-1": func(port int) int { return maximumQoS(port, 1) },
		"03-publish-c2b-qos2-pubrec-error": func(port int) int {
			return client(port, mqttclient.Options{ClientID: "publish-qos2-test", ProtocolVersion: v5}, mqttclient.Handlers{
				OnConnect: func(c *mqttclient.Client, _ mqttclient.ConnAck) {
					_, _ = publish(c, "topic", "rejected", 2, nil)
					_, _ = publish(c, "topic", "accepted", 2, nil)
				},
				OnPublish: func(_ *mqttclient.Client, mid uint16, _ byte, _ *mqttclient.Properties) {
					if mid == 2 {
						finish(0)
					}
				},
			})
		},
		"03-publish-qos0": func(port int) int {
			// The C program uses mosquitto_loop_forever.
			return sendOnce(runForever, port, mqttclient.Options{ClientID: "publish-qos0-test"}, "pub/qos0/test", "message", nil)
		},
		"03-publish-qos0-no-payload": func(port int) int {
			return sendOnce(client, port, mqttclient.Options{ClientID: "publish-qos0-test-np"}, "pub/qos0/no-payload/test", "", nil)
		},
		"11-prop-send-content-type": func(port int) int {
			return sendOnce(client, port, mqttclient.Options{ClientID: "prop-test", ProtocolVersion: v5}, "prop/qos0", "message",
				&mqttclient.Properties{ContentType: "application/json"})
		},
		"11-prop-send-payload-format": func(port int) int {
			return sendOnce(client, port, mqttclient.Options{ClientID: "prop-test", ProtocolVersion: v5}, "prop/qos0", "message",
				&mqttclient.Properties{PayloadFormat: 1})
		},
		"11-prop-oversize-packet": func(port int) int {
			// The payloads are one byte shorter than in mosquitto's script:
			// see overrides/11-prop-oversize-packet.py.
			var sent uint16
			return client(port, mqttclient.Options{ClientID: "publish-qos0-test", ProtocolVersion: v5}, mqttclient.Handlers{
				OnConnect: func(c *mqttclient.Client, _ mqttclient.ConnAck) {
					const long = "0123456789012345678901234567890"
					if _, err := c.Subscribe(context.Background(), []mqttclient.Subscription{{Topic: long}}, nil); !errors.Is(err, mqttclient.ErrOversizePacket) {
						fmt.Println("Fail on subscribe:", err)
						os.Exit(1)
					}
					if _, err := c.Unsubscribe(context.Background(), []string{long}, nil); !errors.Is(err, mqttclient.ErrOversizePacket) {
						fmt.Println("Fail on unsubscribe:", err)
						os.Exit(1)
					}
					if _, err := publish(c, "pub/test", "012345678901234567", 0, nil); !errors.Is(err, mqttclient.ErrOversizePacket) {
						fmt.Println("Fail on publish 1:", err)
						os.Exit(1)
					}
					p, err := publish(c, "pub/test", "01234567890123456", 0, nil)
					if err != nil {
						fmt.Println("Fail on publish 2:", err)
						os.Exit(1)
					}
					sent = p.Mid
				},
				OnPublish: func(c *mqttclient.Client, mid uint16, _ byte, _ *mqttclient.Properties) {
					if mid != sent {
						os.Exit(1)
					}
					disconnect(c)
				},
			})
		},
		"11-prop-recv": func(port int) int {
			// The script passes the expected QoS after the port.
			qos := -1
			if len(os.Args) > 2 {
				qos, _ = strconv.Atoi(os.Args[2])
			}
			return client(port, mqttclient.Options{ClientID: "prop-test", ProtocolVersion: v5}, mqttclient.Handlers{
				OnMessage: func(c *mqttclient.Client, m *mqttclient.Message) {
					if p := m.Properties; p != nil && p.ContentType == "plain/text" && p.ResponseTopic == "msg/123" && int(m.QoS) == qos {
						_, _ = publish(c, "ok", "ok", 0, nil)
						return
					}
					os.Exit(1)
				},
			})
		},
	}
	for name, f := range add {
		cases[name] = f
	}
}

// scripts maps a mosquitto test script to the program it runs, where the
// names differ.
var scripts = map[string]string{
	"03-publish-c2b-qos2-receive-maximum-1": "03-publish-c2b-qos2-receive-maximum",
	"03-publish-c2b-qos2-receive-maximum-2": "03-publish-c2b-qos2-receive-maximum",
	"11-prop-recv-qos0":                     "11-prop-recv",
	"11-prop-recv-qos1":                     "11-prop-recv",
	"11-prop-recv-qos2":                     "11-prop-recv",
}

// helpers are programs without a script of their own: another script runs
// them.
var helpers = map[string]bool{}

// scriptNames lists every script the programs cover.
func scriptNames() []string {
	renamed := make(map[string]bool)
	var names []string
	for s, prog := range scripts {
		renamed[prog] = true
		names = append(names, s)
	}
	for prog := range cases {
		if !renamed[prog] && !helpers[prog] {
			names = append(names, prog)
		}
	}
	return names
}
