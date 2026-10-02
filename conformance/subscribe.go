package main

// Ports of the 02-* (subscribe and unsubscribe) programs, and of the 03-*
// programs that subscribe. Each follows its C original in test/lib/c.

import (
	"context"
	"fmt"
	"os"

	mqttclient "github.com/AUTOSOLN/mqtt-client"
)

func subscribe(c *mqttclient.Client, topic string, qos byte) {
	if _, err := c.Subscribe(context.Background(), []mqttclient.Subscription{{Topic: topic, QoS: qos}}, nil); err != nil {
		fmt.Fprintln(os.Stderr, "subscribe:", err)
		os.Exit(1)
	}
}

func unsubscribe(c *mqttclient.Client, topics []string, props *mqttclient.Properties) {
	if _, err := c.Unsubscribe(context.Background(), topics, props); err != nil {
		fmt.Fprintln(os.Stderr, "unsubscribe:", err)
		os.Exit(1)
	}
}

// expectGranted aborts unless SUBACK granted exactly qos, as the C
// programs' on_subscribe do.
func expectGranted(granted []byte, qos byte) {
	if len(granted) != 1 || granted[0] != qos {
		fmt.Fprintf(os.Stderr, "granted %v, want [%d]\n", granted, qos)
		os.Exit(1)
	}
}

// subscribeThenDisconnect subscribes on connect and disconnects on SUBACK
// (02-subscribe-qos*).
func subscribeThenDisconnect(port int, id, topic string, qos byte) int {
	return client(port, mqttclient.Options{ClientID: id}, mqttclient.Handlers{
		OnConnect: func(c *mqttclient.Client, _ mqttclient.ConnAck) { subscribe(c, topic, qos) },
		OnSubscribe: func(c *mqttclient.Client, _ uint16, granted []byte, _ *mqttclient.Properties) {
			expectGranted(granted, qos)
			disconnect(c)
		},
	})
}

// subscribeHelper is mosquitto_subscribe_simple / mosquitto_subscribe_callback
// for one message: subscribe, and disconnect once a message arrives.
func subscribeHelper(port int) int {
	return client(port, mqttclient.Options{ClientID: "subscribe-qos2-test"}, mqttclient.Handlers{
		OnConnect: func(c *mqttclient.Client, _ mqttclient.ConnAck) { subscribe(c, "qos2/test", 2) },
		OnMessage: func(c *mqttclient.Client, m *mqttclient.Message) {
			if m.Topic != "qos2/test" {
				os.Exit(1)
			}
			disconnect(c)
		},
	})
}

// requester subscribes to response/topic, publishes a request with props,
// and finishes when the response arrives (03-request-response-1 and
// 03-request-response-correlation-1).
func requester(port int, props *mqttclient.Properties) int {
	return client(port, mqttclient.Options{ClientID: "request-test", ProtocolVersion: v5}, mqttclient.Handlers{
		OnConnect: func(c *mqttclient.Client, _ mqttclient.ConnAck) { subscribe(c, "response/topic", 0) },
		OnSubscribe: func(c *mqttclient.Client, _ uint16, granted []byte, _ *mqttclient.Properties) {
			expectGranted(granted, 0)
			if _, err := publish(c, "request/topic", "action", 0, props); err != nil {
				fmt.Fprintln(os.Stderr, "publish:", err)
				os.Exit(1)
			}
		},
		OnMessage: func(_ *mqttclient.Client, m *mqttclient.Message) {
			if string(m.Payload) == "a response" {
				finish(0)
			} else {
				finish(1)
			}
		},
	})
}

func init() {
	add := map[string]func(port int) int{
		"02-subscribe-qos0": func(port int) int { return subscribeThenDisconnect(port, "subscribe-qos0-test", "qos0/test", 0) },
		"02-subscribe-qos1": func(port int) int { return subscribeThenDisconnect(port, "subscribe-qos1-test", "qos1/test", 1) },
		"02-subscribe-qos2": func(port int) int { return subscribeThenDisconnect(port, "subscribe-qos2-test", "qos2/test", 2) },
		// The async variants test mosquitto_loop_start ordering; on the wire
		// they are the same as 02-subscribe-qos1.
		"02-subscribe-qos1-async1": func(port int) int { return subscribeThenDisconnect(port, "subscribe-qos1-test", "qos1/test", 1) },
		"02-subscribe-qos1-async2": func(port int) int { return subscribeThenDisconnect(port, "subscribe-qos1-test", "qos1/test", 1) },

		"02-subscribe-helper-simple-qos2":   subscribeHelper,
		"02-subscribe-helper-callback-qos2": subscribeHelper,

		"02-unsubscribe": func(port int) int {
			return client(port, mqttclient.Options{ClientID: "unsubscribe-test"}, mqttclient.Handlers{
				OnConnect: func(c *mqttclient.Client, _ mqttclient.ConnAck) {
					unsubscribe(c, []string{"unsubscribe/test"}, nil)
				},
				OnUnsubscribe: func(c *mqttclient.Client, _ uint16, _ []byte, _ *mqttclient.Properties) { disconnect(c) },
			})
		},
		"02-unsubscribe-v5": func(port int) int {
			return client(port, mqttclient.Options{ClientID: "unsubscribe-test", ProtocolVersion: v5}, mqttclient.Handlers{
				OnConnect: func(c *mqttclient.Client, _ mqttclient.ConnAck) {
					unsubscribe(c, []string{"unsubscribe/test"}, &mqttclient.Properties{User: []mqttclient.UserProperty{{Key: "key", Val: "value"}}})
				},
				OnUnsubscribe: func(c *mqttclient.Client, _ uint16, _ []byte, _ *mqttclient.Properties) { disconnect(c) },
			})
		},
		"02-unsubscribe-multiple-v5": func(port int) int {
			return client(port, mqttclient.Options{ClientID: "unsubscribe-test", ProtocolVersion: v5}, mqttclient.Handlers{
				OnConnect: func(c *mqttclient.Client, _ mqttclient.ConnAck) { subscribe(c, "unsubscribe/test", 2) },
				OnSubscribe: func(c *mqttclient.Client, _ uint16, granted []byte, _ *mqttclient.Properties) {
					expectGranted(granted, 2)
					unsubscribe(c, []string{"unsubscribe/test", "no-sub"}, nil)
				},
				OnUnsubscribe: func(c *mqttclient.Client, _ uint16, _ []byte, _ *mqttclient.Properties) { disconnect(c) },
			})
		},

		"03-publish-loop": publishLoop,
		// mosquitto runs the same script with each of its loop functions.
		"03-publish-loop-forever": publishLoop,
		"03-publish-loop-manual":  publishLoop,
		"03-publish-loop-start":   publishLoop,

		"03-request-response-1": func(port int) int {
			return requester(port, &mqttclient.Properties{ResponseTopic: "response/topic"})
		},
		"03-request-response-correlation-1": func(port int) int {
			return requester(port, &mqttclient.Properties{ResponseTopic: "response/topic", CorrelationData: []byte("corridor")})
		},
		"03-request-response-2": func(port int) int {
			return client(port, mqttclient.Options{ClientID: "response-test", ProtocolVersion: v5}, mqttclient.Handlers{
				OnConnect: func(c *mqttclient.Client, _ mqttclient.ConnAck) { subscribe(c, "request/topic", 0) },
				OnMessage: func(c *mqttclient.Client, m *mqttclient.Message) {
					p := m.Properties
					if m.Topic != "request/topic" || p == nil || p.ResponseTopic == "" {
						return
					}
					// Only the correlation data goes back, as the C program
					// passes just that property.
					var props *mqttclient.Properties
					if len(p.CorrelationData) > 0 {
						props = &mqttclient.Properties{CorrelationData: p.CorrelationData}
					}
					if _, err := publish(c, p.ResponseTopic, "a response", 0, props); err != nil {
						fmt.Fprintln(os.Stderr, "publish:", err)
						os.Exit(1)
					}
				},
				OnPublish: func(*mqttclient.Client, uint16, byte, *mqttclient.Properties) { finish(0) },
			})
		},
	}
	for name, f := range add {
		cases[name] = f
	}
	for script, prog := range map[string]string{
		"02-subscribe-helper-qos2":        "02-subscribe-helper-simple-qos2",
		"03-request-response":             "03-request-response-1",
		"03-request-response-correlation": "03-request-response-correlation-1",
	} {
		scripts[script] = prog
	}
	for _, prog := range []string{
		"02-subscribe-qos1-async1", "02-subscribe-qos1-async2", "02-subscribe-helper-callback-qos2",
		"03-publish-loop-forever", "03-publish-loop-manual", "03-publish-loop-start", "03-request-response-2",
	} {
		helpers[prog] = true
	}
}

// publishLoop subscribes, publishes to its own subscription, and disconnects
// when the message comes back.
func publishLoop(port int) int {
	return client(port, mqttclient.Options{ClientID: "loop-test", ProtocolVersion: v5}, mqttclient.Handlers{
		OnConnect: func(c *mqttclient.Client, _ mqttclient.ConnAck) { subscribe(c, "loop/test", 0) },
		OnSubscribe: func(c *mqttclient.Client, _ uint16, _ []byte, _ *mqttclient.Properties) {
			if _, err := publish(c, "loop/test", "message", 0, nil); err != nil {
				fmt.Fprintln(os.Stderr, "publish:", err)
				os.Exit(1)
			}
		},
		OnMessage: func(c *mqttclient.Client, _ *mqttclient.Message) { disconnect(c) },
	})
}
