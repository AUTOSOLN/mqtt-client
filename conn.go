package mqttclient

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/wind-c/comqtt/v2/mqtt/packets"
)

// conn is one network connection and its protocol state. A reader
// goroutine handles inbound packets and owns teardown; a keepalive goroutine
// sends PINGREQ and detects a silent server. Writes are serialised by wmu.
type conn struct {
	c       *Client
	version byte
	nc      net.Conn
	br      *bufio.Reader
	wmu     sync.Mutex
	maxIn   uint32 // Maximum Packet Size sent in CONNECT; 0 means no limit

	started        atomic.Bool
	active         atomic.Bool // CONNACK accepted (mosq_cs_active)
	userDisconnect atomic.Bool // Disconnect was called

	// Keepalive state, mirroring libmosquitto's keepalive, last_msg_in,
	// next_msg_out and ping_t.
	keepalive       atomic.Int64 // nanoseconds; 0 disables
	lastIn          atomic.Int64 // unix nanoseconds
	nextOut         atomic.Int64 // unix nanoseconds
	pingOutstanding atomic.Bool
	kick            chan struct{} // keepalive value changed
	kaDone          chan struct{}

	// Owned by the reader goroutine.
	connackSeen  bool
	serverReason byte
	serverProps  *Properties

	connack chan connackResult

	closeOnce sync.Once
	cause     error         // why the connection closed; nil for Disconnect
	done      chan struct{} // closed when the connection is closing
	finished  chan struct{} // closed after teardown, once OnDisconnect is queued
}

type connackResult struct {
	ack ConnAck
	err error
}

func newConn(c *Client) *conn {
	return &conn{
		c:        c,
		version:  c.opts.ProtocolVersion,
		kick:     make(chan struct{}, 1),
		kaDone:   make(chan struct{}),
		connack:  make(chan connackResult, 1),
		done:     make(chan struct{}),
		finished: make(chan struct{}),
	}
}

// start begins reading from nc (mosquitto__reconnect after the socket is
// connected).
func (cn *conn) start(nc net.Conn) {
	cn.nc = nc
	cn.br = bufio.NewReader(nc)
	ka := time.Duration(cn.c.opts.KeepAlive) * time.Second
	now := time.Now().UnixNano()
	cn.keepalive.Store(int64(ka))
	cn.lastIn.Store(now)
	cn.nextOut.Store(now + int64(ka))
	cn.started.Store(true)
	go cn.keepaliveLoop()
	go cn.readLoop()
}

func (cn *conn) isClosed() bool {
	select {
	case <-cn.done:
		return true
	default:
		return false
	}
}

// close shuts the connection down; the first cause wins.
func (cn *conn) close(cause error) {
	cn.closeOnce.Do(func() {
		cn.cause = cause
		close(cn.done)
		_ = cn.nc.Close()
	})
}

// connectErr is the error Connect returns when the connection closed before
// CONNACK.
func (cn *conn) connectErr() error {
	<-cn.done
	if cn.userDisconnect.Load() {
		return ErrDisconnected
	}
	return cn.cause
}

// write encodes and sends pk. A zero deadline means one keepalive period
// from now, or none when keepalive is off: a write that cannot finish in
// that time means the connection is dead, and it must not block the reader
// or keepalive goroutines for longer. A network error closes the connection.
func (cn *conn) write(pk *packets.Packet, deadline time.Time) error {
	if cn.isClosed() {
		return ErrNoConn
	}
	b, err := encodePacket(pk)
	if err != nil {
		return err
	}
	if ka := time.Duration(cn.keepalive.Load()); deadline.IsZero() && ka > 0 {
		deadline = time.Now().Add(ka)
	}
	cn.wmu.Lock()
	defer cn.wmu.Unlock()
	_ = cn.nc.SetWriteDeadline(deadline)
	if _, err := cn.nc.Write(b); err != nil {
		err = fmt.Errorf("%w: %v", ErrConnectionLost, err)
		cn.close(err)
		return err
	}
	cn.nextOut.Store(time.Now().UnixNano() + cn.keepalive.Load())
	return nil
}

// sendDisconnectReason sends a best-effort MQTT 5 DISCONNECT before the
// connection is closed for an error (handle__packet).
func (cn *conn) sendDisconnectReason(reason byte) {
	pk := newDisconnect(MQTT5, reason, nil)
	b, err := encodePacket(&pk)
	if err != nil {
		return
	}
	cn.wmu.Lock()
	defer cn.wmu.Unlock()
	_ = cn.nc.SetWriteDeadline(time.Now().Add(time.Second))
	_, _ = cn.nc.Write(b)
}

func (cn *conn) readLoop() {
	defer cn.finish()
	for {
		pk, err := readPacket(cn.br, cn.version, cn.maxIn)
		if err != nil {
			cn.fail(err)
			return
		}
		cn.lastIn.Store(time.Now().UnixNano())
		if err := cn.handle(&pk); err != nil {
			cn.fail(err)
			return
		}
	}
}

// fail closes the connection because of err. For MQTT 5 protocol errors the
// server is told why first, as libmosquitto does.
func (cn *conn) fail(err error) {
	if cn.isClosed() {
		return
	}
	var refused *ConnRefusedError
	var de *disconnectError
	switch {
	case errors.As(err, &refused), errors.Is(err, ErrServerDisconnect):
	case errors.As(err, &de):
		if cn.version == MQTT5 {
			cn.sendDisconnectReason(de.reason)
		}
	case errors.Is(err, ErrOversizePacket):
		if cn.version == MQTT5 {
			cn.sendDisconnectReason(reasonPacketTooLarge)
		}
	case errors.Is(err, ErrProtocol):
		if cn.version == MQTT5 {
			cn.sendDisconnectReason(reasonProtocolError)
		}
	case errors.Is(err, ErrMalformedPacket):
		if cn.version == MQTT5 {
			cn.sendDisconnectReason(reasonMalformedPacket)
		}
	default:
		err = fmt.Errorf("%w: %v", ErrConnectionLost, err)
	}
	cn.c.log.Debug("connection closing", "client_id", cn.c.ClientID(), "err", err)
	cn.close(err)
}

// finish runs on the reader goroutine once the connection is closing: it
// waits for the keepalive goroutine, detaches the connection from the client
// and queues OnDisconnect.
func (cn *conn) finish() {
	cn.close(ErrConnectionLost) // no-op unless the loop exited unexpectedly
	<-cn.done
	<-cn.kaDone

	c := cn.c
	cause := cn.cause
	if cn.userDisconnect.Load() {
		cause = ErrDisconnected
	}
	c.sess.disconnected(cn, cause)
	c.mu.Lock()
	if c.cn == cn {
		c.cn = nil
	}
	c.mu.Unlock()

	var ev DisconnectEvent
	if !cn.userDisconnect.Load() {
		ev = DisconnectEvent{Err: cn.cause, ReasonCode: cn.serverReason, Properties: cn.serverProps}
	}
	if h := c.h.OnDisconnect; h != nil {
		c.disp.post(func() { h(c, ev) })
	}
	close(cn.finished)
}

func (cn *conn) handle(pk *packets.Packet) error {
	t := pk.FixedHeader.Type
	if t == packets.Connack {
		return cn.handleConnack(pk)
	}
	if !cn.active.Load() {
		return fmt.Errorf("%w: %s before CONNACK", ErrProtocol, packetName(t))
	}
	switch t {
	case packets.Pingreq:
		resp := newPingresp(cn.version)
		return cn.write(&resp, time.Time{})
	case packets.Pingresp:
		cn.pingOutstanding.Store(false)
		return nil
	case packets.Disconnect:
		return cn.handleDisconnect(pk)
	case packets.Publish:
		return cn.handlePublish(pk)
	case packets.Puback:
		return cn.handlePuback(pk)
	case packets.Pubrec:
		return cn.handlePubrec(pk)
	case packets.Pubrel:
		return cn.handlePubrel(pk)
	case packets.Pubcomp:
		return cn.handlePubcomp(pk)
	case packets.Suback, packets.Unsuback:
		return cn.handleSubUnsuback(pk)
	default:
		return fmt.Errorf("%w: %s is not handled yet", ErrProtocol, packetName(t))
	}
}

// handleConnack follows handle__connack.
func (cn *conn) handleConnack(pk *packets.Packet) error {
	c := cn.c
	if cn.connackSeen {
		return fmt.Errorf("%w: duplicate CONNACK", ErrProtocol)
	}
	cn.connackSeen = true

	if pk.ProtocolVersion != cn.version {
		// A 3.x server answered our MQTT 5 CONNECT with a 3.x CONNACK.
		if pk.ReasonCode == 0 {
			return fmt.Errorf("%w: MQTT 5 CONNACK without properties", ErrMalformedPacket)
		}
		code := pk.ReasonCode
		if code == 1 { // unacceptable protocol version
			code = 0x84 // unsupported protocol version
		}
		return cn.connackRefused(ConnAck{ReasonCode: code})
	}
	if c.opts.CleanStart && pk.SessionPresent {
		return fmt.Errorf("%w: CONNACK with session present when clean start was set", ErrProtocol)
	}

	ack := ConnAck{ReasonCode: pk.ReasonCode, SessionPresent: pk.SessionPresent}
	if cn.version == MQTT5 {
		props := pk.Properties
		ack.Properties = &props
		c.mu.Lock()
		if props.AssignedClientID != "" {
			if c.clientID != "" {
				c.mu.Unlock()
				return fmt.Errorf("%w: server assigned a client id but we already have one", ErrProtocol)
			}
			c.clientID = props.AssignedClientID
		}
		if props.RetainAvailableFlag {
			c.retainAvailable = props.RetainAvailable != 0
		}
		c.mu.Unlock()
		if props.ServerKeepAliveFlag {
			cn.keepalive.Store(int64(time.Duration(props.ServerKeepAlive) * time.Second))
			select {
			case cn.kick <- struct{}{}:
			default:
			}
		}
	}
	c.log.Debug("received CONNACK", "client_id", c.ClientID(), "reason_code", ack.ReasonCode)

	if ack.ReasonCode != 0 {
		return cn.connackRefused(ack)
	}
	// Resend what was in flight before OnConnect runs, so that messages
	// keep their order.
	if err := c.sess.connected(cn, cn.sendMaximum(ack.Properties), ack.Properties, ack.SessionPresent); err != nil {
		return err
	}
	if h := c.h.OnConnect; h != nil {
		c.disp.post(func() { h(c, ack) })
	}
	cn.connack <- connackResult{ack: ack}
	return nil
}

// sendMaximum is the number of QoS 1/2 messages that may be in flight to the
// server: its Receive Maximum when it sends one, otherwise MaxInflight (as
// libmosquitto keeps msgs_out.inflight_maximum).
func (cn *conn) sendMaximum(props *Properties) uint16 {
	if props != nil && props.ReceiveMaximum > 0 {
		return props.ReceiveMaximum
	}
	return cn.c.opts.MaxInflight
}

func (cn *conn) connackRefused(ack ConnAck) error {
	c := cn.c
	err := &ConnRefusedError{ReasonCode: ack.ReasonCode, ProtocolVersion: cn.version}
	if h := c.h.OnConnect; h != nil {
		c.disp.post(func() { h(c, ack) })
	}
	cn.connack <- connackResult{ack: ack, err: err}
	return err
}

// handleDisconnect follows handle__disconnect: only MQTT 5 servers may send
// DISCONNECT.
func (cn *conn) handleDisconnect(pk *packets.Packet) error {
	if cn.version != MQTT5 {
		return fmt.Errorf("%w: DISCONNECT from server in MQTT 3.x", ErrProtocol)
	}
	props := pk.Properties
	cn.serverReason = pk.ReasonCode
	cn.serverProps = &props
	return fmt.Errorf("%w: %s (0x%02x)", ErrServerDisconnect, ReasonCodeString(pk.ReasonCode), pk.ReasonCode)
}

// keepaliveLoop follows mosquitto__check_keepalive: when nothing has been
// sent or received for a keepalive period, send PINGREQ; if that period
// passes again without a reply (or before CONNACK), close the connection.
func (cn *conn) keepaliveLoop() {
	defer close(cn.kaDone)
	timer := time.NewTimer(time.Hour)
	defer timer.Stop()
	for {
		wait := time.Hour
		if ka := time.Duration(cn.keepalive.Load()); ka > 0 {
			now := time.Now()
			nextOut := time.Unix(0, cn.nextOut.Load())
			lastIn := time.Unix(0, cn.lastIn.Load())
			if !now.Before(nextOut) || now.Sub(lastIn) >= ka {
				if !cn.active.Load() || cn.pingOutstanding.Load() {
					cn.close(ErrKeepalive)
					return
				}
				cn.pingOutstanding.Store(true)
				ping := newPingreq(cn.version)
				if err := cn.write(&ping, now.Add(ka)); err != nil {
					return
				}
				cn.lastIn.Store(now.UnixNano())
				continue
			}
			wait = min(nextOut.Sub(now), lastIn.Add(ka).Sub(now))
		}
		timer.Reset(wait)
		select {
		case <-cn.done:
			return
		case <-cn.kick:
		case <-timer.C:
		}
	}
}
