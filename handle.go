// Handlers for PUBLISH and its acknowledgements, following libmosquitto's
// handle_publish.c, handle_pubackcomp.c, handle_pubrec.c and handle_pubrel.c.
// They run on the reader goroutine.

package mqttclient

import (
	"fmt"
	"slices"
	"time"

	"github.com/wind-c/comqtt/v2/mqtt/packets"
)

// handlePublish accepts a message from the server. QoS 1 is acknowledged
// and delivered at once; QoS 2 is stored, acknowledged with PUBREC and
// delivered on PUBREL.
func (cn *conn) handlePublish(pk *packets.Packet) error {
	c := cn.c
	qos := pk.FixedHeader.Qos
	if err := cn.checkInboundPublish(pk); err != nil {
		return err
	}
	m := &Message{Topic: pk.TopicName, Payload: pk.Payload, QoS: qos, Retain: pk.FixedHeader.Retain, Mid: pk.PacketID}
	if cn.version == MQTT5 {
		props := pk.Properties
		m.Properties = &props
	}
	c.log.Debug("received PUBLISH", "mid", m.Mid, "qos", qos, "topic", m.Topic, "bytes", len(m.Payload))

	switch qos {
	case 0:
		c.postMessage(m)
		return nil
	case 1:
		if err := cn.checkReceiveQuota(); err != nil {
			return err
		}
		ack := newAck(cn.version, packets.Puback, m.Mid, 0)
		if err := cn.write(&ack, time.Time{}); err != nil {
			return err
		}
		c.postMessage(m)
		return nil
	default:
		s := &c.sess
		s.mu.Lock()
		// A resend of a message still waiting for PUBREL replaces it, rather
		// than queueing a second copy as mosquitto does.
		if _, dup := s.in[m.Mid]; !dup {
			if cn.version == MQTT5 && s.recvMax > 0 {
				if s.recvQuota == 0 {
					s.mu.Unlock()
					return protocolError(reasonReceiveMaximumExceeded, "more than %d QoS 2 messages in flight", s.recvMax)
				}
				s.recvQuota--
			}
		}
		s.in[m.Mid] = m
		s.mu.Unlock()
		ack := newAck(cn.version, packets.Pubrec, m.Mid, 0)
		return cn.write(&ack, time.Time{})
	}
}

func (cn *conn) checkInboundPublish(pk *packets.Packet) error {
	if cn.version == MQTT5 && (pk.Properties.TopicAlias != 0 || pk.Properties.TopicAliasFlag) {
		// CONNECT never allows topic aliases (Topic Alias Maximum 0).
		return protocolError(reasonTopicAliasInvalid, "PUBLISH with a topic alias")
	}
	if pk.TopicName == "" {
		return fmt.Errorf("%w: PUBLISH without a topic", ErrProtocol)
	}
	if err := checkPublishTopic(pk.TopicName); err != nil {
		return fmt.Errorf("%w: PUBLISH topic: %v", ErrMalformedPacket, err)
	}
	if pk.FixedHeader.Qos > 0 && pk.PacketID == 0 {
		return fmt.Errorf("%w: PUBLISH with packet identifier 0", ErrProtocol)
	}
	if slices.Contains(pk.Properties.SubscriptionIdentifier, 0) {
		return fmt.Errorf("%w: subscription identifier 0", ErrProtocol)
	}
	return nil
}

// checkReceiveQuota enforces the Receive Maximum the client sent: QoS 1
// messages are acknowledged at once, so only stored QoS 2 messages use it.
func (cn *conn) checkReceiveQuota() error {
	if cn.version != MQTT5 {
		return nil
	}
	s := &cn.c.sess
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.recvMax > 0 && s.recvQuota == 0 {
		return protocolError(reasonReceiveMaximumExceeded, "more than %d QoS 1/2 messages in flight", s.recvMax)
	}
	return nil
}

// handlePubrel completes an inbound QoS 2 message. PUBCOMP is sent even for
// an unknown packet identifier, which a resent PUBREL may carry.
func (cn *conn) handlePubrel(pk *packets.Packet) error {
	if err := cn.checkAckReason(pk); err != nil {
		return err
	}
	s := &cn.c.sess
	s.mu.Lock()
	m, ok := s.in[pk.PacketID]
	if ok {
		delete(s.in, pk.PacketID)
		if s.recvQuota < s.recvMax {
			s.recvQuota++
		}
	}
	s.mu.Unlock()
	ack := newAck(cn.version, packets.Pubcomp, pk.PacketID, 0)
	if err := cn.write(&ack, time.Time{}); err != nil {
		return err
	}
	if ok {
		cn.c.postMessage(m)
	}
	return nil
}

// handlePuback completes an outbound QoS 1 message, and handlePubcomp a
// QoS 2 one. An acknowledgement for an unknown packet identifier is ignored.
func (cn *conn) handlePuback(pk *packets.Packet) error  { return cn.handleFinalAck(pk, 1) }
func (cn *conn) handlePubcomp(pk *packets.Packet) error { return cn.handleFinalAck(pk, 2) }

func (cn *conn) handleFinalAck(pk *packets.Packet, qos byte) error {
	if err := cn.checkAckReason(pk); err != nil {
		return err
	}
	s := &cn.c.sess
	s.mu.Lock()
	defer s.mu.Unlock()
	om := s.outByMid[pk.PacketID]
	if om == nil {
		cn.c.log.Debug("acknowledgement for unknown packet identifier", "packet", packetName(pk.FixedHeader.Type), "mid", pk.PacketID)
		return nil
	}
	if om.m.QoS != qos {
		return fmt.Errorf("%w: %s for a QoS %d message", ErrProtocol, packetName(pk.FixedHeader.Type), om.m.QoS)
	}
	cn.complete(om, pk)
	return s.release(time.Time{})
}

// handlePubrec moves an outbound QoS 2 message on to PUBREL, or completes
// it if the server refused it (MQTT 5 reason code 0x80 or above). PUBREL is
// sent even for an unknown packet identifier, as libmosquitto does.
func (cn *conn) handlePubrec(pk *packets.Packet) error {
	if err := cn.checkAckReason(pk); err != nil {
		return err
	}
	s := &cn.c.sess
	s.mu.Lock()
	defer s.mu.Unlock()
	om := s.outByMid[pk.PacketID]
	if om != nil && om.m.QoS != 2 {
		return fmt.Errorf("%w: PUBREC for a QoS %d message", ErrProtocol, om.m.QoS)
	}
	if cn.version == MQTT5 && pk.ReasonCode >= 0x80 {
		if om != nil {
			cn.complete(om, pk)
		}
		return s.release(time.Time{})
	}
	if om != nil {
		if !om.state.inFlight() && s.sendQuota > 0 {
			s.sendQuota--
		}
		om.state = outWaitPubcomp
	} else {
		cn.c.log.Debug("PUBREC for unknown packet identifier", "mid", pk.PacketID)
	}
	rel := newAck(cn.version, packets.Pubrel, pk.PacketID, 0)
	return cn.write(&rel, time.Time{})
}

// complete removes a finished outbound message and reports it. s.mu must be
// held.
func (cn *conn) complete(om *outMsg, pk *packets.Packet) {
	cn.c.sess.remove(om)
	var props *Properties
	if cn.version == MQTT5 {
		p := pk.Properties
		props = &p
	}
	res := Result{ReasonCode: pk.ReasonCode, Properties: props}
	var err error
	if pk.ReasonCode >= 0x80 {
		err = &ReasonCodeError{Packet: packetName(pk.FixedHeader.Type), ReasonCode: pk.ReasonCode}
	}
	om.p.complete(res, err)
	cn.c.postPublish(om.m.Mid, pk.ReasonCode, props)
}

func (cn *conn) checkAckReason(pk *packets.Packet) error {
	if cn.version != MQTT5 {
		return nil
	}
	if !validAckReason(pk.FixedHeader.Type, pk.ReasonCode) {
		return fmt.Errorf("%w: %s with reason code 0x%02x", ErrProtocol, packetName(pk.FixedHeader.Type), pk.ReasonCode)
	}
	return nil
}

// validAckReason reports whether a server may send code in an
// acknowledgement of type typ (MQTT 5 §3.4.2.1, §3.5.2.1, §3.6.2.1,
// §3.7.2.1), as handle__pubackcomp, handle__pubrec and handle__pubrel check.
func validAckReason(typ, code byte) bool {
	switch typ {
	case packets.Puback, packets.Pubrec:
		switch code {
		case 0x00, 0x10, 0x80, 0x83, 0x87, 0x90, 0x91, 0x97, 0x99:
			return true
		}
	case packets.Pubrel, packets.Pubcomp:
		return code == 0x00 || code == reasonPacketIDNotFound
	}
	return false
}
