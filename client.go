package mqttclient

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"

	"github.com/wind-c/comqtt/v2/mqtt/packets"
)

// Client is an MQTT client connection to one of the servers in
// Options.Servers at a time. It is safe for concurrent use.
type Client struct {
	opts Options
	h    Handlers
	log  *slog.Logger
	disp dispatcher
	sess session

	mu     sync.Mutex
	cn     *conn   // current network connection, nil when disconnected
	runner *runner // set while Run is active
	srvs   []*serverState
	cur    int // index in srvs of the current server
}

// serverState is one of Options.Servers with the settings that may change
// after New.
type serverState struct {
	Server
	host            string
	clientID        string
	username        string
	password        []byte
	will            *Message
	retainAvailable bool
}

// New validates opts and returns a disconnected client (mosquitto_new).
func New(opts Options, h Handlers) (*Client, error) {
	if err := opts.validate(); err != nil {
		return nil, err
	}
	log := opts.Logger
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	c := &Client{opts: opts, h: h, log: log}
	for _, s := range opts.Servers {
		host, _ := splitAddress(s.Address)
		c.srvs = append(c.srvs, &serverState{
			Server:          s,
			host:            host,
			clientID:        s.ClientID,
			username:        s.Username,
			password:        s.Password,
			will:            copyMessage(s.Will),
			retainAvailable: true,
		})
	}
	c.sess.init()
	return c, nil
}

// ServerIndex returns the index in Options.Servers of the current server:
// the one the client is connected to, or is connecting to, or will try
// next. It starts at 0. OnPreConnect may call it to learn which server is
// about to be tried.
func (c *Client) ServerIndex() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.cur
}

// server returns the current server. c.mu must be held.
func (c *Client) server() *serverState { return c.srvs[c.cur] }

// version returns the protocol version of the current server.
func (c *Client) version() byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.server().ProtocolVersion
}

// advance makes the server after i current, if i still is.
func (c *Client) advance(i int) {
	c.mu.Lock()
	if c.cur == i {
		c.cur = (i + 1) % len(c.srvs)
	}
	c.mu.Unlock()
}

// ClientID returns the client identifier for the current server, which may
// have been assigned by an MQTT 5 server in CONNACK.
func (c *Client) ClientID() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.server().clientID
}

// SetCredentials replaces the username and password sent in the next
// CONNECT to the current server (mosquitto_username_pw_set). Call it from
// OnPreConnect to change them for the server about to be tried.
func (c *Client) SetCredentials(username string, password []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	s := c.server()
	if err := checkCredentials(s.ProtocolVersion, username, password); err != nil {
		return err
	}
	s.username, s.password = username, password
	return nil
}

// SetWill replaces the Will sent in the next CONNECT to the current server;
// nil clears it (mosquitto_will_set_v5 / mosquitto_will_clear). Call it from
// OnPreConnect to change it for the server about to be tried.
func (c *Client) SetWill(w *Message) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	s := c.server()
	if err := checkWill(s.ProtocolVersion, c.opts.NoTopicCheck, w); err != nil {
		return err
	}
	s.will = copyMessage(w)
	return nil
}

// IsConnected reports whether the client has an accepted connection.
func (c *Client) IsConnected() bool {
	c.mu.Lock()
	cn := c.cn
	c.mu.Unlock()
	return cn != nil && cn.active.Load() && !cn.isClosed()
}

// Connect opens a network connection, sends CONNECT and waits for CONNACK
// or for ctx to end. OnConnect is called when CONNACK arrives; if the
// connection is refused, OnDisconnect follows.
//
// Connect tries each server once, in order, starting from the current one
// (see ServerIndex), and stops at the first that accepts the connection.
// Each attempt is also bounded by that server's ConnectTimeout. If every
// attempt fails, the server after the last one tried becomes current, and
// Connect returns the error from a single server as it is (a
// *ConnRefusedError for a refusal), or the errors from several servers
// joined.
//
// Connect does not reconnect after the connection is lost; OnDisconnect
// reports the loss and Connect may be called again. Use Run for a
// connection that is kept up automatically.
func (c *Client) Connect(ctx context.Context) (ConnAck, error) {
	c.mu.Lock()
	running := c.runner != nil
	c.mu.Unlock()
	if running {
		return ConnAck{}, ErrRunning
	}
	var ack ConnAck
	var errs []error
	for range c.srvs {
		i := c.ServerIndex()
		var err error
		ack, _, err = c.connectTimeout(ctx, i, true)
		if err == nil {
			return ack, nil
		}
		if ctx.Err() != nil || errors.Is(err, ErrDisconnected) || errors.Is(err, ErrAlreadyConnected) {
			return ack, err
		}
		errs = append(errs, c.serverErr(i, err))
		c.advance(i)
	}
	return ack, joinServerErrs(errs)
}

// connectTimeout is connect bounded by the server's ConnectTimeout.
func (c *Client) connectTimeout(ctx context.Context, i int, resetRetain bool) (ConnAck, *conn, error) {
	actx, cancel := context.WithTimeout(ctx, c.srvs[i].ConnectTimeout)
	defer cancel()
	return c.connect(actx, i, resetRetain)
}

// serverErr labels err with server i's address when there are several
// servers.
func (c *Client) serverErr(i int, err error) error {
	if len(c.srvs) == 1 {
		return err
	}
	return fmt.Errorf("server %d (%s): %w", i, c.srvs[i].Address, err)
}

// joinServerErrs returns the one error of a single server unchanged.
func joinServerErrs(errs []error) error {
	if len(errs) == 1 {
		return errs[0]
	}
	return errors.Join(errs...)
}

// connect makes one connection attempt to server i, which becomes current,
// and returns the connection on success. resetRetain assumes the server
// supports retain again, as mosquitto_connect does; mosquitto_reconnect
// keeps the last CONNACK's value, which decides whether the Will is sent
// with retain.
func (c *Client) connect(ctx context.Context, i int, resetRetain bool) (ConnAck, *conn, error) {
	ctx, abort := context.WithCancel(ctx)
	defer abort()
	c.mu.Lock()
	if c.cn != nil {
		c.mu.Unlock()
		return ConnAck{}, nil, ErrAlreadyConnected
	}
	c.cur = i
	srv := c.srvs[i]
	cn := newConn(c, srv)
	cn.abort = abort
	c.cn = cn
	if resetRetain {
		srv.retainAvailable = true
	}
	c.mu.Unlock()

	if c.h.OnPreConnect != nil {
		c.h.OnPreConnect(c)
	}

	nc, err := c.dial(ctx, srv)
	if err != nil {
		c.mu.Lock()
		c.cn = nil
		c.mu.Unlock()
		if cn.userDisconnect.Load() {
			return ConnAck{}, nil, ErrDisconnected
		}
		return ConnAck{}, nil, fmt.Errorf("mqttclient: connect to %s: %w", srv.Address, err)
	}

	c.mu.Lock()
	pk := newConnect(connectParams{
		version:         srv.ProtocolVersion,
		clientID:        srv.clientID,
		cleanStart:      srv.CleanStart,
		keepAlive:       srv.KeepAlive,
		username:        srv.username,
		password:        srv.password,
		will:            srv.will,
		retainAvailable: srv.retainAvailable,
		properties:      srv.ConnectProperties,
		receiveMaximum:  srv.ReceiveMaximum,
	})
	c.mu.Unlock()
	if p := connectProperties(&pk); p != nil {
		cn.maxIn = p.MaximumPacketSize
		c.sess.mu.Lock()
		c.sess.recvMax = p.ReceiveMaximum
		c.sess.mu.Unlock()
	}

	cn.start(nc)
	if cn.userDisconnect.Load() {
		cn.close(nil)
		<-cn.finished
		return ConnAck{}, nil, ErrDisconnected
	}

	c.log.Debug("sending CONNECT", "client_id", pk.Connect.ClientIdentifier)
	deadline, _ := ctx.Deadline()
	if err := cn.write(&pk, deadline); err != nil {
		<-cn.finished
		return ConnAck{}, nil, cn.connectErr()
	}

	select {
	case r := <-cn.connack:
		if r.err != nil {
			<-cn.finished
			return r.ack, nil, r.err
		}
		return r.ack, cn, nil
	case <-cn.finished:
		select {
		case r := <-cn.connack:
			if r.err != nil {
				return r.ack, nil, r.err
			}
			return r.ack, cn, nil
		default:
			return ConnAck{}, nil, cn.connectErr()
		}
	case <-ctx.Done():
		// An accepted connection is returned rather than dropped, so that
		// the caller (Run) can close it with DISCONNECT and no Will.
		select {
		case r := <-cn.connack:
			if r.err == nil {
				return r.ack, cn, nil
			}
		default:
		}
		cn.close(ctx.Err())
		<-cn.finished
		if cn.userDisconnect.Load() {
			return ConnAck{}, nil, ErrDisconnected
		}
		return ConnAck{}, nil, ctx.Err()
	}
}

// Disconnect sends DISCONNECT with the given MQTT 5 reason code and
// properties, closes the connection and waits for it to shut down
// (mosquitto_disconnect_v5). For MQTT 3.x, reason must be 0 and props nil.
// OnDisconnect is called with a nil Err.
//
// With Run, Disconnect also stops Run, which returns nil; it does not wait
// for Run to return. Between connections, Disconnect only stops Run.
func (c *Client) Disconnect(ctx context.Context, reason byte, props *Properties) error {
	c.mu.Lock()
	cn, r := c.cn, c.runner
	version := c.server().ProtocolVersion
	c.mu.Unlock()
	if cn != nil {
		version = cn.version
	}
	if version != MQTT5 && (reason != 0 || props != nil) {
		return fmt.Errorf("%w: DISCONNECT reason code and properties require MQTT 5", ErrNotSupported)
	}
	if r != nil {
		r.requestStop()
	}
	noConn := ErrNoConn
	if r != nil {
		noConn = nil // stopping Run is all there is to do
	}
	if cn == nil {
		return noConn
	}
	cn.userDisconnect.Store(true)
	if !cn.started.Load() {
		cn.abort() // connect is still dialling; it returns ErrDisconnected
		return noConn
	}
	return c.disconnect(ctx, cn, reason, props)
}

func (c *Client) disconnect(ctx context.Context, cn *conn, reason byte, props *Properties) error {
	cn.userDisconnect.Store(true)

	pk := newDisconnect(cn.version, reason, props)
	deadline, _ := ctx.Deadline()
	err := cn.write(&pk, deadline)
	cn.close(nil)
	select {
	case <-cn.finished:
	case <-ctx.Done():
		return ctx.Err()
	}
	return err
}

func (c *Client) dial(ctx context.Context, srv *serverState) (net.Conn, error) {
	dial := srv.Dial
	if dial == nil {
		var d net.Dialer
		dial = d.DialContext
	}
	nc, err := dial(ctx, "tcp", srv.Address)
	if err != nil || !srv.TLS {
		return nc, err
	}
	cfg := &tls.Config{}
	if srv.TLSConfig != nil {
		cfg = srv.TLSConfig.Clone()
	}
	if cfg.ServerName == "" {
		cfg.ServerName = srv.host
	}
	tc := tls.Client(nc, cfg)
	if err := tc.HandshakeContext(ctx); err != nil {
		_ = nc.Close()
		return nil, err
	}
	return tc, nil
}

func copyMessage(m *Message) *Message {
	if m == nil {
		return nil
	}
	cp := *m
	return &cp
}

// connectProperties returns the properties of an MQTT 5 CONNECT, or nil for MQTT 3.x.
func connectProperties(pk *packets.Packet) *Properties {
	if pk.ProtocolVersion != MQTT5 {
		return nil
	}
	return &pk.Properties
}
