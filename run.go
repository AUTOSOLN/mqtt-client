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
// or an attempt fails, it waits (see Server.ReconnectDelay) and connects
// again, until ctx ends or Disconnect is called. It blocks, so start it on
// its own goroutine.
//
// Run starts with the current server (see ServerIndex). After a failed
// attempt it moves on to the next server in Options.Servers, cycling back
// to the first after the last. After an accepted connection is lost it
// tries the same server again first. The wait before each attempt uses the
// settings of the server about to be tried. A server that fails in a way
// retrying cannot fix (see below) is not tried again during this Run.
//
// Each new connection resends unacknowledged QoS 1 and 2 messages, as
// Connect does, whichever server it is to. Subscriptions are not resent:
// subscribe from OnConnect, which runs for every accepted connection.
// OnConnectError reports each failed attempt.
//
// Run returns:
//   - nil after Disconnect;
//   - ctx.Err() when ctx ends, after a clean DISCONNECT if connected (so
//     the server does not publish the Will);
//   - once every server has failed in a way retrying cannot fix, the
//     error from a single server as it is, or the errors from several
//     servers joined: a *ConnRefusedError for credentials, authorization,
//     client id, protocol version and other non-transient reasons, a
//     protocol error from the server (ErrProtocol), or a TLS certificate
//     verification failure.
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

	reconnects := make([]int, len(c.srvs)) // per server, as n in Server.ReconnectDelay
	failed := make([]error, len(c.srvs))   // permanent failures
	for first := true; ; first = false {
		i := c.ServerIndex()
		connected, err := c.runOnce(ctx, r, i, first)
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
		if connected {
			reconnects[i] = 0 // a successful CONNACK resets the backoff
		}
		if permanent(err) {
			c.log.Debug("not reconnecting to server", "address", c.srvs[i].Address, "err", err)
			failed[i] = c.serverErr(i, err)
		}
		next := i
		if !connected || failed[i] != nil {
			var ok bool
			if next, ok = nextServer(i, failed); !ok {
				return joinServerErrs(failed)
			}
			c.mu.Lock()
			c.cur = next
			c.mu.Unlock()
		}
		var delay time.Duration
		delay, reconnects[next] = c.srvs[next].reconnectDelay(reconnects[next])
		c.log.Debug("reconnecting", "address", c.srvs[next].Address, "in", delay, "err", err)
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

// nextServer returns the first server after i, cycling, that has not
// failed permanently; i itself comes last.
func nextServer(i int, failed []error) (int, bool) {
	for k := 1; k <= len(failed); k++ {
		if j := (i + k) % len(failed); failed[j] == nil {
			return j, true
		}
	}
	return 0, false
}

// runOnce makes one connection attempt to server i and, if it is accepted,
// waits for the connection to end. It reports whether CONNACK accepted the
// connection and why it ended; nil means Disconnect ended it.
func (c *Client) runOnce(ctx context.Context, r *runner, i int, first bool) (connected bool, err error) {
	// Disconnect aborts or closes a connection attempt itself.
	_, cn, err := c.connectTimeout(ctx, i, first)
	if err != nil {
		return false, err
	}

	select {
	case <-cn.finished:
	case <-ctx.Done():
		dctx, dcancel := context.WithTimeout(context.Background(), cn.srv.ConnectTimeout)
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
func (s *Server) reconnectDelay(n int) (time.Duration, int) {
	d, maxDelay := s.ReconnectDelay, s.ReconnectDelayMax
	delay := d
	if maxDelay > d {
		f := time.Duration(n + 1)
		if s.ReconnectExponential {
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
