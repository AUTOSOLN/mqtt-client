package mqttclient

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"net"
	"sync"

	"github.com/wind-c/comqtt/v2/mqtt/packets"
)

// Client is an MQTT client connection to a single server. It is safe for
// concurrent use.
type Client struct {
	opts Options
	h    Handlers
	srv  server
	log  *slog.Logger
	disp dispatcher
	sess session

	mu              sync.Mutex
	cn              *conn   // current network connection, nil when disconnected
	runner          *runner // set while Run is active
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
	srv, err := parseServer(opts.Server)
	if err != nil {
		return nil, err
	}
	if opts.TLSConfig != nil && !srv.useTLS {
		return nil, fmt.Errorf("%w: TLSConfig set for non-TLS server %q", ErrInvalid, opts.Server)
	}
	log := opts.Logger
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	c := &Client{
		opts:     opts,
		h:        h,
		srv:      srv,
		log:      log,
		clientID: opts.ClientID,
		username: opts.Username,
		password: opts.Password,
		will:     copyMessage(opts.Will),
	}
	c.sess.init()
	return c, nil
}

// ClientID returns the client identifier, which may have been assigned by
// an MQTT 5 server in CONNACK.
func (c *Client) ClientID() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.clientID
}

// SetCredentials replaces the username and password sent in the next
// CONNECT (mosquitto_username_pw_set).
func (c *Client) SetCredentials(username string, password []byte) error {
	if err := checkCredentials(c.opts.ProtocolVersion, username, password); err != nil {
		return err
	}
	c.mu.Lock()
	c.username, c.password = username, password
	c.mu.Unlock()
	return nil
}

// SetWill replaces the Will sent in the next CONNECT; nil clears it
// (mosquitto_will_set_v5 / mosquitto_will_clear).
func (c *Client) SetWill(w *Message) error {
	if err := checkWill(c.opts.ProtocolVersion, w); err != nil {
		return err
	}
	c.mu.Lock()
	c.will = copyMessage(w)
	c.mu.Unlock()
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
// connection is refused, Connect returns a *ConnRefusedError and
// OnDisconnect follows.
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
	ack, _, err := c.connect(ctx, true)
	return ack, err
}

// connect makes one connection attempt and returns the connection on
// success. resetRetain assumes the server supports retain again, as
// mosquitto_connect does; mosquitto_reconnect keeps the last CONNACK's
// value, which decides whether the Will is sent with retain.
func (c *Client) connect(ctx context.Context, resetRetain bool) (ConnAck, *conn, error) {
	ctx, abort := context.WithCancel(ctx)
	defer abort()
	c.mu.Lock()
	if c.cn != nil {
		c.mu.Unlock()
		return ConnAck{}, nil, ErrAlreadyConnected
	}
	cn := newConn(c)
	cn.abort = abort
	c.cn = cn
	if resetRetain {
		c.retainAvailable = true
	}
	c.mu.Unlock()

	if c.h.OnPreConnect != nil {
		c.h.OnPreConnect(c)
	}

	nc, err := c.dial(ctx)
	if err != nil {
		c.mu.Lock()
		c.cn = nil
		c.mu.Unlock()
		if cn.userDisconnect.Load() {
			return ConnAck{}, nil, ErrDisconnected
		}
		return ConnAck{}, nil, fmt.Errorf("mqttclient: connect to %s: %w", c.srv.addr, err)
	}

	c.mu.Lock()
	pk := newConnect(connectParams{
		version:         c.opts.ProtocolVersion,
		clientID:        c.clientID,
		cleanStart:      c.opts.CleanStart,
		keepAlive:       c.opts.KeepAlive,
		username:        c.username,
		password:        c.password,
		will:            c.will,
		retainAvailable: c.retainAvailable,
		properties:      c.opts.ConnectProperties,
		receiveMaximum:  c.opts.ReceiveMaximum,
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
	if c.opts.ProtocolVersion != MQTT5 && (reason != 0 || props != nil) {
		return fmt.Errorf("%w: DISCONNECT reason code and properties require MQTT 5", ErrNotSupported)
	}
	c.mu.Lock()
	cn, r := c.cn, c.runner
	c.mu.Unlock()
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

	pk := newDisconnect(c.opts.ProtocolVersion, reason, props)
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

func (c *Client) dial(ctx context.Context) (net.Conn, error) {
	var d net.Dialer
	if !c.srv.useTLS {
		return d.DialContext(ctx, "tcp", c.srv.addr)
	}
	cfg := &tls.Config{}
	if c.opts.TLSConfig != nil {
		cfg = c.opts.TLSConfig.Clone()
	}
	if cfg.ServerName == "" {
		cfg.ServerName = c.srv.host
	}
	td := tls.Dialer{NetDialer: &d, Config: cfg}
	return td.DialContext(ctx, "tcp", c.srv.addr)
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
