// Manual acknowledgement of received messages (Options.ManualAck).

package mqttclient

import (
	"time"

	"github.com/wind-c/comqtt/v2/mqtt/packets"
)

// holdAck records a received QoS 1 message whose PUBACK waits for Ack. It
// uses a slot of the Receive Maximum until then.
func (cn *conn) holdAck(m *Message) {
	s := &cn.c.sess
	s.mu.Lock()
	defer s.mu.Unlock()
	if cn.version == MQTT5 && s.recvMax > 0 && s.recvQuota > 0 {
		s.recvQuota--
	}
	m.ack = &pendingAck{mid: m.Mid, qos: 1, cn: cn}
	s.pending = append(s.pending, m.ack)
}

// Ack acknowledges a message received with Options.ManualAck: it sends the
// PUBACK (QoS 1) or PUBCOMP (QoS 2) for m, after those of any earlier
// message still waiting for Ack, which go first. Acking a QoS 0 message, a
// message received without ManualAck, or one already acked does nothing.
//
// If the connection m arrived on has ended, Ack returns ErrNoConn: the
// server resends a QoS 1 message it has not had a PUBACK for, and completes
// a QoS 2 one without delivering it again.
func (c *Client) Ack(m *Message) error {
	if m == nil || m.ack == nil {
		return nil
	}
	s := &c.sess
	s.mu.Lock()
	defer s.mu.Unlock()
	p := m.ack
	if p.acked {
		return nil
	}
	p.acked = true
	if p.cn != s.cn || s.cn == nil {
		return ErrNoConn
	}
	return s.flushAcks(time.Time{})
}

// flushAcks sends the acknowledgements at the head of pending that have
// been acked, stopping at the first that has not. s.mu must be held.
func (s *session) flushAcks(deadline time.Time) error {
	for len(s.pending) > 0 && s.pending[0].acked {
		p := s.pending[0]
		s.pending[0] = nil
		s.pending = s.pending[1:]
		if p.cn != s.cn || s.cn == nil {
			continue
		}
		typ := byte(packets.Puback)
		if p.qos == 2 {
			typ = packets.Pubcomp
		}
		ack := newAck(s.cn.version, typ, p.mid, 0)
		if err := s.cn.write(&ack, deadline); err != nil {
			return err
		}
		if s.recvQuota < s.recvMax {
			s.recvQuota++
		}
	}
	return nil
}
