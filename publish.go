package mqttclient

import (
	"context"
	"fmt"

	"github.com/wind-c/comqtt/v2/mqtt/packets"
)

// maxPayload is the largest payload a PUBLISH can carry (MQTT_MAX_PAYLOAD).
const maxPayload = 268435455

// Pending tracks a request (Publish, Subscribe or Unsubscribe) until it is
// complete.
type Pending struct {
	Mid uint16

	done chan struct{}
	res  Result
	err  error
}

// Result is the server's answer to a request.
type Result struct {
	// ReasonCode is the reason code of a publish's final acknowledgement
	// (MQTT 5); 0 for QoS 0.
	ReasonCode byte

	// ReasonCodes has one entry per topic filter of a Subscribe (the granted
	// QoS, or a failure code of 0x80 or above) or an Unsubscribe.
	ReasonCodes []byte

	Properties *Properties // properties of the acknowledgement (MQTT 5)
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

// Wait blocks until the request is complete or ctx ends. It returns a
// *ReasonCodeError, with the Result, if the server refused the message or
// a topic filter (reason code 0x80 or above; for several filters, the first
// refused one). A ctx error does not cancel the request: a published
// message stays queued and is still sent and resent on later connections.
// A Subscribe or Unsubscribe fails with the connection's error if the
// connection closes before the acknowledgement.
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
	probe := msg
	probe.Mid = 1 // only its presence matters for the size
	probePk := newPublish(c.opts.ProtocolVersion, &probe, false)
	if err := s.checkSize(&probePk); err != nil {
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
	if err := checkPublishTopic(m.Topic, c.opts.NoTopicCheck); err != nil {
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
		if p.ResponseTopic != "" {
			// The codec would silently drop an invalid one.
			if err := checkPublishTopic(p.ResponseTopic, c.opts.NoTopicCheck); err != nil {
				return fmt.Errorf("%w: response topic: %v", ErrInvalid, err)
			}
		}
	}
	return nil
}

// checkSize rejects a packet larger than the server's Maximum Packet Size,
// counting the whole packet as MQTT 5 §3.2.2.3.6 defines it. The packet
// identifier only needs to be non-zero. s.mu must be held.
func (s *session) checkSize(pk *packets.Packet) error {
	if s.maxPacketSize == 0 {
		return nil
	}
	b, err := encodePacket(pk)
	if err != nil {
		return err
	}
	if uint64(len(b)) > uint64(s.maxPacketSize) {
		return fmt.Errorf("%w: %s of %d bytes, server maximum %d", ErrOversizePacket, packetName(pk.FixedHeader.Type), len(b), s.maxPacketSize)
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
