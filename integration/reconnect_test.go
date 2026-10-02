package integration

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	mqttclient "github.com/AUTOSOLN/mqtt-client"
)

const waitTime = 10 * time.Second

// events collects one client's callbacks.
type events struct {
	connects    chan mqttclient.ConnAck
	disconnects chan mqttclient.DisconnectEvent
	failures    chan error
	messages    chan *mqttclient.Message
}

func newEvents() *events {
	return &events{
		connects:    make(chan mqttclient.ConnAck, 10),
		disconnects: make(chan mqttclient.DisconnectEvent, 10),
		failures:    make(chan error, 100),
		messages:    make(chan *mqttclient.Message, 100),
	}
}

// handlers records events; onConnect, if set, runs first in OnConnect.
func (e *events) handlers(onConnect func(*mqttclient.Client, mqttclient.ConnAck)) mqttclient.Handlers {
	return mqttclient.Handlers{
		OnConnect: func(c *mqttclient.Client, a mqttclient.ConnAck) {
			if onConnect != nil {
				onConnect(c, a)
			}
			e.connects <- a
		},
		OnDisconnect:   func(_ *mqttclient.Client, ev mqttclient.DisconnectEvent) { e.disconnects <- ev },
		OnConnectError: func(_ *mqttclient.Client, err error) { e.failures <- err },
		OnMessage:      func(_ *mqttclient.Client, m *mqttclient.Message) { e.messages <- m },
	}
}

func next[T any](t *testing.T, ch chan T, what string) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(waitTime):
		t.Fatalf("timed out waiting for %s", what)
		var zero T
		return zero
	}
}

// message waits for a message on topic, skipping others.
func (e *events) message(t *testing.T, topic string) *mqttclient.Message {
	t.Helper()
	deadline := time.After(waitTime)
	for {
		select {
		case m := <-e.messages:
			if m.Topic == topic {
				return m
			}
		case <-deadline:
			t.Fatalf("no message on %s", topic)
			return nil
		}
	}
}

// run starts c.Run and stops it at the end of the test.
func run(t *testing.T, c *mqttclient.Client) <-chan error {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	res := make(chan error, 1)
	go func() { res <- c.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-res:
		case <-time.After(waitTime):
			t.Error("Run did not return")
		}
	})
	return res
}

func publish(t *testing.T, c *mqttclient.Client, topic, payload string, qos byte) *mqttclient.Pending {
	t.Helper()
	p, err := c.Publish(context.Background(), &mqttclient.Message{Topic: topic, Payload: []byte(payload), QoS: qos})
	if err != nil {
		t.Fatalf("publish %s: %v", topic, err)
	}
	return p
}

func wait(t *testing.T, p *mqttclient.Pending) mqttclient.Result {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), waitTime)
	defer cancel()
	res, err := p.Wait(ctx)
	if err != nil {
		t.Fatalf("mid %d: %v", p.Mid, err)
	}
	return res
}

func subscribeOnConnect(topic string) func(*mqttclient.Client, mqttclient.ConnAck) {
	return func(c *mqttclient.Client, _ mqttclient.ConnAck) {
		_, _ = c.Subscribe(context.Background(), []mqttclient.Subscription{{Topic: topic, QoS: 1}}, nil)
	}
}

func options(b *broker, version byte, id string) mqttclient.Options {
	return mqttclient.Options{
		Server:          b.url(),
		ProtocolVersion: version,
		ClientID:        id,
		CleanStart:      true,
		KeepAlive:       5,
		ReconnectDelay:  100 * time.Millisecond,
		ConnectTimeout:  2 * time.Second,
	}
}

// The broker is killed and restarted: Run reconnects, the message published
// during the outage is delivered, and the subscription made in OnConnect is
// back.
func TestReconnectAfterBrokerKill(t *testing.T) {
	for _, v := range []byte{mqttclient.MQTT311, mqttclient.MQTT5} {
		t.Run(fmt.Sprintf("v%d", v), func(t *testing.T) {
			b := newBroker(t, false)
			b.start()
			ev := newEvents()
			c, err := mqttclient.New(options(b, v, "kill-test"), ev.handlers(subscribeOnConnect("it/#")))
			if err != nil {
				t.Fatal(err)
			}
			run(t, c)
			next(t, ev.connects, "first connection")
			wait(t, publish(t, c, "it/before", "1", 1))
			ev.message(t, "it/before")

			b.kill()
			if d := next(t, ev.disconnects, "OnDisconnect"); d.Err == nil {
				t.Fatal("OnDisconnect without error after the broker was killed")
			}
			during := publish(t, c, "it/during", "2", 1) // queued until reconnected
			next(t, ev.failures, "a failed reconnect attempt")

			b.start()
			next(t, ev.connects, "reconnection")
			wait(t, during)
			wait(t, publish(t, c, "it/after", "3", 1))
			ev.message(t, "it/after")
		})
	}
}

// Run started while the broker is down keeps trying until it is up.
func TestBrokerDownAtStart(t *testing.T) {
	b := newBroker(t, false)
	ev := newEvents()
	c, err := mqttclient.New(options(b, mqttclient.MQTT5, "late-test"), ev.handlers(nil))
	if err != nil {
		t.Fatal(err)
	}
	run(t, c)
	next(t, ev.failures, "first failure")
	next(t, ev.failures, "second failure")
	b.start()
	next(t, ev.connects, "connection")
}

// With CleanStart false the broker keeps the session (here across a clean
// broker restart with persistence): the client gets Session Present and
// receives messages on a subscription it made only once.
func TestSessionResumption(t *testing.T) {
	b := newBroker(t, true)
	b.start()
	ev := newEvents()
	opts := options(b, mqttclient.MQTT311, "resume-test")
	opts.CleanStart = false
	first := true
	c, err := mqttclient.New(opts, ev.handlers(func(c *mqttclient.Client, a mqttclient.ConnAck) {
		if first {
			first = false
			subscribeOnConnect("resume/#")(c, a)
		}
	}))
	if err != nil {
		t.Fatal(err)
	}
	run(t, c)
	next(t, ev.connects, "first connection")
	wait(t, publish(t, c, "resume/a", "1", 1))
	ev.message(t, "resume/a")

	b.stop()
	next(t, ev.disconnects, "OnDisconnect")
	b.start()
	if a := next(t, ev.connects, "reconnection"); !a.SessionPresent {
		t.Fatal("no session present after the restart")
	}
	wait(t, publish(t, c, "resume/b", "2", 1))
	ev.message(t, "resume/b")
	// Clean up the stored session.
	if err := c.Disconnect(context.Background(), 0, nil); err != nil {
		t.Fatal(err)
	}
}

// Ending Run's ctx disconnects cleanly: the broker does not publish the Will.
func TestRunCancelSendsNoWill(t *testing.T) {
	b := newBroker(t, false)
	b.start()

	watch := newEvents()
	w, err := mqttclient.New(options(b, mqttclient.MQTT5, "will-watch"), watch.handlers(subscribeOnConnect("will/#")))
	if err != nil {
		t.Fatal(err)
	}
	run(t, w)
	next(t, watch.connects, "watcher connection")
	time.Sleep(200 * time.Millisecond) // let the subscription settle

	opts := options(b, mqttclient.MQTT5, "will-test")
	opts.Will = &mqttclient.Message{Topic: "will/test", Payload: []byte("gone")}
	ev := newEvents()
	c, err := mqttclient.New(opts, ev.handlers(nil))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	res := make(chan error, 1)
	go func() { res <- c.Run(ctx) }()
	next(t, ev.connects, "connection")
	cancel()
	if err := next(t, res, "Run to return"); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run returned %v", err)
	}
	select {
	case m := <-watch.messages:
		t.Fatalf("Will published after a clean stop: %+v", m)
	case <-time.After(500 * time.Millisecond):
	}
}

// Disconnect stops Run while it is waiting for the broker to come back.
func TestDisconnectWhileBrokerDown(t *testing.T) {
	b := newBroker(t, false)
	b.start()
	ev := newEvents()
	opts := options(b, mqttclient.MQTT311, "stop-test")
	opts.ReconnectDelay = time.Second
	opts.ReconnectDelayMax = time.Minute
	opts.ReconnectExponential = true
	c, err := mqttclient.New(opts, ev.handlers(nil))
	if err != nil {
		t.Fatal(err)
	}
	res := make(chan error, 1)
	go func() { res <- c.Run(context.Background()) }()
	next(t, ev.connects, "connection")
	b.kill()
	next(t, ev.disconnects, "OnDisconnect")
	next(t, ev.failures, "a failed attempt")

	start := time.Now()
	if err := c.Disconnect(context.Background(), 0, nil); err != nil {
		t.Fatal(err)
	}
	if err := next(t, res, "Run to return"); err != nil {
		t.Fatalf("Run returned %v", err)
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("Run took %v to stop", d)
	}
}
