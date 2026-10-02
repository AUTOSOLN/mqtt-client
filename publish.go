package mqttclient

import (
	"context"
	"fmt"
)

// maxPayload is the largest payload a PUBLISH can carry (MQTT_MAX_PAYLOAD).
const maxPayload = 268435455

// Pending tracks a message from Publish until it is complete.
type Pending struct {
	Mid uint16

	done chan struct{}
	res  Result
	err  error
}

// Result is the server's answer to a published message.
type Result struct {
	ReasonCode byte        // reason code of the final acknowledgement (MQTT 5); 0 for QoS 0
	Properties *Properties // properties of the final acknowledgement (MQTT 5)
}

func newPending(mid uint16) *Pending {
	return &Pending{Mid: mid, done: make(chan struct{})}
}

func (p *Pending) complete(res Result, err error) {
	p.res, p.err = res, err
	close(p.done)
}

// Done is closed when the message is complete.
func (p *Pending) Done() <-chan struct{} { return p.done }

// Wait blocks until the message is complete or ctx ends. It returns a
// *ReasonCodeError if the server refused the message (MQTT 5 reason code
// 0x80 or above). A ctx error does not cancel the message: it stays queued
// and is still sent and resent on later connections.
func (p *Pending) Wait(ctx context.Context) (Result, error) {
	select {
	case <-p.done:
		return p.res, p.err
	case <-ctx.Done():
		return Result{}, ctx.Err()
	}
}

// Publish sends m (mosquitto_publish_v5). QoS 0 requires a connection and is
// complete once written. QoS 1 and 2 messages are queued: they are sent
// when the send quota allows, and are kept across connections until
// acknowledged, so a message published while disconnected is sent after the
// next successful Connect. ctx bounds the network write, not the
// acknowledgement; use the returned Pending to wait for that.
//
// As in libmosquitto, the retain flag is cleared when the server does not
// support retain, and a QoS above the server's Maximum QoS fails with
// ErrQoSNotSupported. A message larger than the server's Maximum Packet Size
// fails with ErrOversizePacket.
func (c *Client) Publish(ctx context.Context, m *Message) (*Pending, error) {
	if err := c.checkPublish(m); err != nil {
		return nil, err
	}
	msg := *m
	c.mu.Lock()
	if !c.retainAvailable {
		msg.Retain = false
	}
	c.mu.Unlock()

	s := &c.sess
	s.mu.Lock()
	defer s.mu.Unlock()
	if msg.QoS > s.maxQoS {
		return nil, fmt.Errorf("%w: qos %d, server maximum %d", ErrQoSNotSupported, msg.QoS, s.maxQoS)
	}
	if err := s.checkSize(c.opts.ProtocolVersion, &msg); err != nil {
		return nil, err
	}
	if msg.QoS == 0 && s.cn == nil {
		return nil, ErrNoConn
	}
	mid, err := s.nextMid()
	if err != nil {
		return nil, err
	}
	msg.Mid = mid
	p := newPending(mid)
	deadline, _ := ctx.Deadline()

	if msg.QoS == 0 {
		pk := newPublish(c.opts.ProtocolVersion, &msg, false)
		if err := s.cn.write(&pk, deadline); err != nil {
			return nil, err
		}
		c.log.Debug("sent PUBLISH", "mid", mid, "qos", 0)
		p.complete(Result{}, nil)
		c.postPublish(mid, 0, nil)
		return p, nil
	}

	// The message may be resent later; keep it independent of the caller's.
	msg.Payload = append([]byte(nil), msg.Payload...)
	if msg.Properties != nil {
		props := *msg.Properties
		msg.Properties = &props
	}
	om := &outMsg{m: msg, p: p}
	s.out = append(s.out, om)
	s.outByMid[mid] = om
	if s.cn != nil {
		if err := s.release(deadline); err != nil {
			// The connection is closing; the message stays queued.
			c.log.Debug("publish deferred", "mid", mid, "err", err)
		}
	}
	return p, nil
}

// checkPublish applies the argument checks of mosquitto_publish_v5 that do
// not depend on the server.
func (c *Client) checkPublish(m *Message) error {
	if m == nil {
		return fmt.Errorf("%w: nil message", ErrInvalid)
	}
	if m.QoS > 2 {
		return fmt.Errorf("%w: qos %d", ErrInvalid, m.QoS)
	}
	if err := checkPublishTopic(m.Topic); err != nil {
		return fmt.Errorf("%w: topic: %v", ErrInvalid, err)
	}
	if len(m.Payload) > maxPayload {
		return fmt.Errorf("%w: payload of %d bytes", ErrInvalid, len(m.Payload))
	}
	if p := m.Properties; p != nil {
		if c.opts.ProtocolVersion != MQTT5 {
			return fmt.Errorf("%w: PUBLISH properties require MQTT 5", ErrNotSupported)
		}
		if p.TopicAlias != 0 || p.TopicAliasFlag {
			return fmt.Errorf("%w: topic alias", ErrNotSupported)
		}
		if len(p.SubscriptionIdentifier) > 0 {
			return fmt.Errorf("%w: a client may not send a subscription identifier", ErrInvalid)
		}
	}
	return nil
}

// checkSize rejects a PUBLISH larger than the server's Maximum Packet Size,
// counting the whole packet as MQTT 5 §3.2.2.3.6 defines it. s.mu must be
// held.
func (s *session) checkSize(version byte, m *Message) error {
	if s.maxPacketSize == 0 {
		return nil
	}
	probe := *m
	probe.Mid = 1 // only its presence matters for the size
	pk := newPublish(version, &probe, false)
	b, err := encodePacket(&pk)
	if err != nil {
		return err
	}
	if uint64(len(b)) > uint64(s.maxPacketSize) {
		return fmt.Errorf("%w: PUBLISH of %d bytes, server maximum %d", ErrOversizePacket, len(b), s.maxPacketSize)
	}
	return nil
}

func (c *Client) postPublish(mid uint16, reason byte, props *Properties) {
	if h := c.h.OnPublish; h != nil {
		c.disp.post(func() { h(c, mid, reason, props) })
	}
}

func (c *Client) postMessage(m *Message) {
	if h := c.h.OnMessage; h != nil {
		c.disp.post(func() { h(c, m) })
	}
}
