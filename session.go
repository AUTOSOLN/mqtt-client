// Session state follows libmosquitto's messages_mosq.c and util_mosq.c:
// outbound messages wait in one ordered list, and a send quota (the
// server's Receive Maximum) bounds how many are in flight.

package mqttclient

import (
	"slices"
	"sync"
	"time"

	"github.com/wind-c/comqtt/v2/mqtt/packets"
)

// outState is where an outbound QoS 1 or 2 message is in its flow
// (mosquitto's mosq_ms_* states).
type outState uint8

const (
	outQueued       outState = iota // PUBLISH to send when the quota allows
	outQueuedPubrel                 // QoS 2 PUBREC already received; PUBREL to send when the quota allows
	outWaitPuback
	outWaitPubrec
	outWaitPubcomp
)

// inFlight reports whether the message counts against the send quota.
func (s outState) inFlight() bool { return s >= outWaitPuback }

type outMsg struct {
	m     Message // owned copy; m.Mid is set
	state outState
	sent  bool // PUBLISH was sent at least once, so a resend sets DUP
	p     *Pending
}

// session is the message state of a Client. It outlives individual network
// connections: outbound messages are kept and resent after the next
// CONNACK, as libmosquitto does.
type session struct {
	mu sync.Mutex

	lastMid  uint16
	out      []*outMsg // in publish order
	outByMid map[uint16]*outMsg

	// in holds inbound QoS 2 messages waiting for PUBREL.
	in map[uint16]*Message

	// Set from each CONNACK.
	cn            *conn  // accepted connection, nil when there is none
	sendQuota     uint16 // more QoS 1/2 PUBLISH packets that may be in flight
	maxQoS        byte
	maxPacketSize uint32 // 0 means no limit

	recvMax   uint16 // Receive Maximum sent in CONNECT (MQTT 5); 0 means no limit
	recvQuota uint16
}

func (s *session) init() {
	s.outByMid = make(map[uint16]*outMsg)
	s.in = make(map[uint16]*Message)
	s.maxQoS = 2
}

// nextMid allocates a packet identifier: the one after the last, skipping 0
// and any still in use.
func (s *session) nextMid() (uint16, error) {
	for range 65535 {
		s.lastMid++
		if s.lastMid == 0 {
			s.lastMid = 1
		}
		if _, used := s.outByMid[s.lastMid]; !used {
			return s.lastMid, nil
		}
	}
	return 0, ErrNoMid
}

func (s *session) remove(om *outMsg) {
	delete(s.outByMid, om.m.Mid)
	if i := slices.Index(s.out, om); i >= 0 {
		s.out = slices.Delete(s.out, i, i+1)
	}
	if om.state.inFlight() {
		s.sendQuota++
	}
}

// release sends queued messages, oldest first, while the send quota allows
// (message__release_to_inflight). s.mu must be held and s.cn set.
func (s *session) release(deadline time.Time) error {
	for _, om := range s.out {
		if s.sendQuota == 0 {
			return nil
		}
		var pk packets.Packet
		next := om.state
		switch om.state {
		case outQueued:
			pk = newPublish(s.cn.version, &om.m, om.sent)
			om.sent = true
			next = outWaitPuback
			if om.m.QoS == 2 {
				next = outWaitPubrec
			}
		case outQueuedPubrel:
			pk = newAck(s.cn.version, packets.Pubrel, om.m.Mid, 0)
			next = outWaitPubcomp
		default:
			continue
		}
		if err := s.cn.write(&pk, deadline); err != nil {
			return err
		}
		s.cn.c.log.Debug("sent "+packetName(pk.FixedHeader.Type), "mid", om.m.Mid, "qos", om.m.QoS, "dup", pk.FixedHeader.Dup)
		om.state = next
		s.sendQuota--
	}
	return nil
}

// connected starts using cn after a successful CONNACK: it applies the
// server's limits and resends what was in flight, then queued messages
// (message__reconnect_reset and message__retry_check).
func (s *session) connected(cn *conn, sendMax uint16, props *Properties, sessionPresent bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cn = cn
	s.sendQuota = sendMax
	s.maxQoS = 2
	s.maxPacketSize = 0
	if props != nil {
		if props.MaximumQosFlag {
			s.maxQoS = props.MaximumQos
		}
		s.maxPacketSize = props.MaximumPacketSize
	}
	// Without a session on the server, a PUBREL for a stored QoS 2 message
	// will never come.
	if !sessionPresent {
		clear(s.in)
	}
	s.recvQuota = s.recvMax - min(s.recvMax, uint16(len(s.in)))
	cn.active.Store(true)
	return s.release(time.Time{})
}

// disconnected rewinds in-flight messages so that the next connection sends
// them again: PUBLISH (with DUP) or PUBREL.
func (s *session) disconnected(cn *conn) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cn != cn {
		return
	}
	s.cn = nil
	for _, om := range s.out {
		switch om.state {
		case outWaitPuback, outWaitPubrec:
			om.state = outQueued
		case outWaitPubcomp:
			om.state = outQueuedPubrel
		}
	}
}
