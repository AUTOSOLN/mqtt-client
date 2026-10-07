package mqttclient

import (
	"context"
	"fmt"
	"slices"

	"github.com/wind-c/comqtt/v2/mqtt/packets"
)

// maxSubscriptionID is the largest Subscription Identifier (a variable byte
// integer).
const maxSubscriptionID = 268435455

// Subscription is one topic filter in a SUBSCRIBE.
type Subscription struct {
	Topic string // topic filter; may contain + and # wildcards
	QoS   byte

	// MQTT 5 subscription options. MQTT 3.x has none; they are ignored there,
	// as libmosquitto does.
	NoLocal           bool // do not receive this client's own publications
	RetainAsPublished bool // keep the retain flag of forwarded messages
	RetainHandling    byte // 0 send retained messages, 1 only for a new subscription, 2 never
}

// Subscribe sends SUBSCRIBE for subs (mosquitto_subscribe_multiple, with
// options per filter). It needs a connection: subscriptions are not queued
// or resent, so subscribe again from OnConnect after reconnecting. The
// Pending completes on SUBACK with one granted QoS or failure reason code
// per filter. props may carry a Subscription Identifier and User
// Properties (MQTT 5).
func (c *Client) Subscribe(ctx context.Context, subs []Subscription, props *Properties) (*Pending, error) {
	if err := c.checkSubscribe(subs, props); err != nil {
		return nil, err
	}
	filters := make(packets.Subscriptions, len(subs))
	for i, sub := range subs {
		filters[i] = packets.Subscription{Filter: sub.Topic, Qos: sub.QoS}
		if c.opts.ProtocolVersion == MQTT5 {
			filters[i].NoLocal = sub.NoLocal
			filters[i].RetainAsPublished = sub.RetainAsPublished
			filters[i].RetainHandling = sub.RetainHandling
		}
	}
	return c.sendSubUnsub(ctx, packets.Subscribe, filters, props)
}

// Unsubscribe sends UNSUBSCRIBE for topics (mosquitto_unsubscribe_multiple).
// It needs a connection. The Pending completes on UNSUBACK with one reason
// code per filter; MQTT 3.x has none, so they are all reported as 0.
func (c *Client) Unsubscribe(ctx context.Context, topics []string, props *Properties) (*Pending, error) {
	if len(topics) == 0 {
		return nil, fmt.Errorf("%w: no topic filters", ErrInvalid)
	}
	for _, t := range topics {
		if err := checkSubscribeTopic(t, c.opts.NoTopicCheck); err != nil {
			return nil, fmt.Errorf("%w: topic filter %q: %v", ErrInvalid, t, err)
		}
	}
	if err := c.checkSubUnsubProperties(props); err != nil {
		return nil, err
	}
	if props != nil && len(props.SubscriptionIdentifier) > 0 {
		return nil, fmt.Errorf("%w: UNSUBSCRIBE cannot carry a subscription identifier", ErrInvalid)
	}
	filters := make(packets.Subscriptions, len(topics))
	for i, t := range topics {
		filters[i] = packets.Subscription{Filter: t}
	}
	return c.sendSubUnsub(ctx, packets.Unsubscribe, filters, props)
}

func (c *Client) checkSubscribe(subs []Subscription, props *Properties) error {
	if len(subs) == 0 {
		return fmt.Errorf("%w: no subscriptions", ErrInvalid)
	}
	for _, sub := range subs {
		if err := checkSubscribeTopic(sub.Topic, c.opts.NoTopicCheck); err != nil {
			return fmt.Errorf("%w: topic filter %q: %v", ErrInvalid, sub.Topic, err)
		}
		if sub.QoS > 2 {
			return fmt.Errorf("%w: qos %d", ErrInvalid, sub.QoS)
		}
		if sub.RetainHandling > 2 {
			return fmt.Errorf("%w: retain handling %d", ErrInvalid, sub.RetainHandling)
		}
	}
	if err := c.checkSubUnsubProperties(props); err != nil {
		return err
	}
	if props != nil {
		if ids := props.SubscriptionIdentifier; len(ids) > 1 || slices.ContainsFunc(ids, func(id int) bool { return id < 1 || id > maxSubscriptionID }) {
			return fmt.Errorf("%w: subscription identifier %v: want one value from 1 to %d", ErrInvalid, ids, maxSubscriptionID)
		}
	}
	return nil
}

func (c *Client) checkSubUnsubProperties(props *Properties) error {
	if props != nil && c.opts.ProtocolVersion != MQTT5 {
		return fmt.Errorf("%w: properties require MQTT 5", ErrNotSupported)
	}
	return nil
}

// sendSubUnsub sends a SUBSCRIBE or UNSUBSCRIBE and registers it to wait
// for its acknowledgement.
func (c *Client) sendSubUnsub(ctx context.Context, typ byte, filters packets.Subscriptions, props *Properties) (*Pending, error) {
	pk := packets.Packet{
		FixedHeader:     packets.FixedHeader{Type: typ, Qos: 1}, // flags 0010 [MQTT-3.8.1-1] [MQTT-3.10.1-1]
		ProtocolVersion: c.opts.ProtocolVersion,
		Filters:         filters,
		PacketID:        1, // for the size check; replaced below
	}
	if props != nil {
		pk.Properties = *props
	}
	ack := byte(packets.Suback)
	if typ == packets.Unsubscribe {
		ack = packets.Unsuback
	}

	s := &c.sess
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cn == nil {
		return nil, ErrNoConn
	}
	if err := s.checkSize(&pk); err != nil {
		return nil, err
	}
	mid, err := s.nextMid()
	if err != nil {
		return nil, err
	}
	pk.PacketID = mid
	p := newPending(mid)
	deadline, _ := ctx.Deadline()
	if err := s.cn.write(&pk, deadline); err != nil {
		return nil, err
	}
	c.log.Debug("sent "+packetName(typ), "mid", mid, "filters", len(filters))
	s.acks[mid] = &ackWait{typ: ack, n: len(filters), p: p}
	return p, nil
}

// handleSubUnsuback completes a SUBSCRIBE or UNSUBSCRIBE
// (handle__suback, handle__unsuback). An acknowledgement for an unknown
// packet identifier is ignored.
func (cn *conn) handleSubUnsuback(pk *packets.Packet) error {
	typ := pk.FixedHeader.Type
	if pk.PacketID == 0 {
		return fmt.Errorf("%w: %s with packet identifier 0", ErrProtocol, packetName(typ))
	}
	if typ == packets.Suback && len(pk.ReasonCodes) == 0 {
		return fmt.Errorf("%w: SUBACK without reason codes", ErrProtocol)
	}
	s := &cn.c.sess
	s.mu.Lock()
	a := s.acks[pk.PacketID]
	if a == nil {
		s.mu.Unlock()
		cn.c.log.Debug("acknowledgement for unknown packet identifier", "packet", packetName(typ), "mid", pk.PacketID)
		return nil
	}
	if a.typ != typ {
		s.mu.Unlock()
		return fmt.Errorf("%w: %s for a %s", ErrProtocol, packetName(typ), packetName(a.typ))
	}
	delete(s.acks, pk.PacketID)
	s.mu.Unlock()

	codes := slices.Clone(pk.ReasonCodes)
	if typ == packets.Unsuback && cn.version != MQTT5 {
		codes = make([]byte, a.n) // codec gap 9: MQTT 3.x UNSUBACK means success
	}
	var props *Properties
	if cn.version == MQTT5 {
		p := pk.Properties
		props = &p
	}
	var err error
	if i := slices.IndexFunc(codes, func(c byte) bool { return c >= 0x80 }); i >= 0 {
		err = &ReasonCodeError{Packet: packetName(typ), ReasonCode: codes[i]}
	}
	a.p.complete(Result{ReasonCodes: codes, Properties: props}, err)

	c := cn.c
	if typ == packets.Suback {
		if h := c.h.OnSubscribe; h != nil {
			c.disp.post(func() { h(c, pk.PacketID, codes, props) })
		}
	} else if h := c.h.OnUnsubscribe; h != nil {
		c.disp.post(func() { h(c, pk.PacketID, codes, props) })
	}
	return nil
}
