// Run is the connection supervisor: libmosquitto's mosquitto_loop_forever
// with its reconnect delay (loop.c, mosquitto_reconnect_delay_set).

package mqttclient

import (
	"context"
	"crypto/tls"
	"errors"
	"sync"
	"time"
)

// runner is the state of an active Run.
type runner struct {
	stopOnce sync.Once
	stop     chan struct{} // closed by Disconnect
}

func (r *runner) requestStop() { r.stopOnce.Do(func() { close(r.stop) }) }

// Run connects and keeps the client connected: when the connection is lost
// or an attempt fails, it waits (see Options.ReconnectDelay) and connects
// again, until ctx ends or Disconnect is called. It blocks, so start it on
// its own goroutine.
//
// Each new connection resends unacknowledged QoS 1 and 2 messages, as
// Connect does. Subscriptions are not resent: subscribe from OnConnect,
// which runs for every accepted connection. OnConnectError reports each
// failed attempt.
//
// Run returns:
//   - nil after Disconnect;
//   - ctx.Err() when ctx ends, after a clean DISCONNECT if connected (so
//     the server does not publish the Will);
//   - the error when an attempt fails in a way retrying cannot fix: a
//     *ConnRefusedError for credentials, authorization, client id,
//     protocol version and other non-transient reasons, a protocol error
//     from the server (ErrProtocol), or a TLS certificate verification
//     failure.
//
// Connect returns ErrRunning while Run is active.
func (c *Client) Run(ctx context.Context) error {
	c.mu.Lock()
	switch {
	case c.runner != nil:
		c.mu.Unlock()
		return ErrRunning
	case c.cn != nil:
		c.mu.Unlock()
		return ErrAlreadyConnected
	}
	r := &runner{stop: make(chan struct{})}
	c.runner = r
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		c.runner = nil
		c.mu.Unlock()
	}()

	reconnects := 0
	for first := true; ; first = false {
		connected, err := c.runOnce(ctx, r, first)
		select {
		case <-r.stop:
			return nil
		default:
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if errors.Is(err, ErrDisconnected) {
			return nil
		}
		if !connected {
			if h := c.h.OnConnectError; h != nil {
				c.disp.post(func() { h(c, err) })
			}
		}
		if permanent(err) {
			c.log.Debug("not reconnecting", "err", err)
			return err
		}
		if connected {
			reconnects = 0 // a successful CONNACK resets the backoff
		}
		var delay time.Duration
		delay, reconnects = c.reconnectDelay(reconnects)
		c.log.Debug("reconnecting", "in", delay, "err", err)
		t := time.NewTimer(delay)
		select {
		case <-t.C:
		case <-ctx.Done():
			t.Stop()
			return ctx.Err()
		case <-r.stop:
			t.Stop()
			return nil
		}
	}
}

// runOnce makes one connection attempt and, if it is accepted, waits for the
// connection to end. It reports whether CONNACK accepted the connection and
// why it ended; nil means Disconnect ended it.
func (c *Client) runOnce(ctx context.Context, r *runner, first bool) (connected bool, err error) {
	// Disconnect aborts or closes a connection attempt itself.
	actx, cancel := context.WithTimeout(ctx, c.opts.ConnectTimeout)
	_, cn, err := c.connect(actx, first)
	cancel()
	if err != nil {
		return false, err
	}

	select {
	case <-cn.finished:
	case <-ctx.Done():
		dctx, dcancel := context.WithTimeout(context.Background(), c.opts.ConnectTimeout)
		_ = c.disconnect(dctx, cn, 0, nil)
		dcancel()
		return true, ctx.Err()
	case <-r.stop:
		<-cn.finished // Disconnect is closing it
		return true, nil
	}
	if cn.userDisconnect.Load() {
		return true, nil
	}
	return true, cn.cause
}

// reconnectDelay is the wait before reconnect attempt n (from 0) and the
// next n, as in mosquitto_loop_forever: n stops growing once the delay
// reaches the maximum.
func (c *Client) reconnectDelay(n int) (time.Duration, int) {
	d, maxDelay := c.opts.ReconnectDelay, c.opts.ReconnectDelayMax
	delay := d
	if maxDelay > d {
		f := time.Duration(n + 1)
		if c.opts.ReconnectExponential {
			f *= time.Duration(n + 1)
		}
		delay = d * f
	}
	if delay > maxDelay {
		return maxDelay, n
	}
	return delay, n + 1
}

// permanent reports whether retrying cannot fix err.
func permanent(err error) bool {
	var refused *ConnRefusedError
	if errors.As(err, &refused) {
		return !transientRefusal(refused)
	}
	if errors.Is(err, ErrProtocol) {
		return true
	}
	var cv *tls.CertificateVerificationError
	return errors.As(err, &cv)
}

// transientRefusal reports whether a CONNACK refusal may go away by itself:
// the server is unavailable, busy or over a quota or rate, or gave no
// specific reason.
func transientRefusal(e *ConnRefusedError) bool {
	if e.ProtocolVersion < MQTT5 {
		return e.ReasonCode == 3 // server unavailable
	}
	switch e.ReasonCode {
	case 0x80, 0x83, 0x88, 0x89, 0x97, 0x9F:
		return true
	}
	return false
}
