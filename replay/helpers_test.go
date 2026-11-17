package replay_test

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/netip"
	"sync"
	"testing"
	"time"

	"github.com/wind-c/comqtt/v2/mqtt/packets"

	mqttclient "github.com/AUTOSOLN/mqtt-client"
	"github.com/AUTOSOLN/mqtt-client/capture"
	"github.com/AUTOSOLN/mqtt-client/capture/capturetest"
	"github.com/AUTOSOLN/mqtt-client/replay"
	"github.com/AUTOSOLN/mqtt-client/sparkplug"
)

var (
	t0         = time.Date(2026, 11, 17, 12, 0, 0, 0, time.UTC)
	hostAddr   = netip.MustParseAddrPort("10.0.0.1:50000")
	brokerAddr = netip.MustParseAddrPort("10.0.0.2:1883")
	edgeAddr   = netip.MustParseAddrPort("10.0.0.3:40000")
)

// enc encodes an MQTT packet with comqtt, at protocol level 4 unless pk
// says otherwise.
func enc(t testing.TB, pk packets.Packet) []byte {
	t.Helper()
	if pk.ProtocolVersion == 0 {
		pk.ProtocolVersion = 4
	}
	if pk.FixedHeader.Type == packets.Connect && pk.Connect.ProtocolName == nil {
		pk.Connect.ProtocolName = []byte("MQTT")
	}
	var buf bytes.Buffer
	var err error
	switch pk.FixedHeader.Type {
	case packets.Connect:
		err = pk.ConnectEncode(&buf)
	case packets.Connack:
		err = pk.ConnackEncode(&buf)
	case packets.Publish:
		err = pk.PublishEncode(&buf)
	case packets.Puback:
		err = pk.PubackEncode(&buf)
	case packets.Pubrec:
		err = pk.PubrecEncode(&buf)
	case packets.Pubrel:
		pk.FixedHeader.Qos = 1 // the required flags 0010
		err = pk.PubrelEncode(&buf)
	case packets.Pubcomp:
		err = pk.PubcompEncode(&buf)
	case packets.Pingreq:
		err = pk.PingreqEncode(&buf)
	case packets.Pingresp:
		err = pk.PingrespEncode(&buf)
	case packets.Subscribe:
		pk.FixedHeader.Qos = 1 // the required flags 0010
		err = pk.SubscribeEncode(&buf)
	case packets.Suback:
		err = pk.SubackEncode(&buf)
	case packets.Disconnect:
		err = pk.DisconnectEncode(&buf)
	default:
		t.Fatalf("enc: no encoder for type %d", pk.FixedHeader.Type)
	}
	if err != nil {
		t.Fatalf("enc: %v", err)
	}
	return buf.Bytes()
}

func fh(typ byte) packets.FixedHeader { return packets.FixedHeader{Type: typ} }

func publishPk(topic string, qos byte, id uint16, payload []byte) packets.Packet {
	return packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: qos}, TopicName: topic, PacketID: id, Payload: payload}
}

// session builds a capture.Conn one packet at a time, a millisecond apart.
type session struct {
	t    testing.TB
	conn capture.Conn
	ms   int
}

func newSession(t testing.TB, client, server netip.AddrPort) *session {
	return &session{t: t, conn: capture.Conn{Client: client, Server: server}}
}

func (s *session) now() time.Time { return t0.Add(time.Duration(s.ms) * time.Millisecond) }

func (s *session) add(d capture.Dir, pk packets.Packet) *session {
	s.ms++
	s.conn.Segments = append(s.conn.Segments, capture.Segment{Dir: d, Time: s.now(), Data: enc(s.t, pk)})
	return s
}

func (s *session) client(pk packets.Packet) *session { return s.add(capture.FromClient, pk) }
func (s *session) server(pk packets.Packet) *session { return s.add(capture.FromServer, pk) }

func (s *session) clientClose() *session {
	s.ms++
	t := s.now()
	s.conn.ClientClose = &t
	return s
}

func (s *session) serverClose() *session {
	s.ms++
	t := s.now()
	s.conn.ServerClose = &t
	return s
}

// throughFile writes conn to a pcap file and reads it back, so that a test
// also goes through the capture file path.
func throughFile(t testing.TB, conn capture.Conn) capture.Conn {
	t.Helper()
	var buf bytes.Buffer
	if err := capturetest.Write(&buf, []capture.Conn{conn}); err != nil {
		t.Fatal(err)
	}
	c, err := capture.Read(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if len(c.Conns) != 1 {
		t.Fatalf("got %d connections from the file, want 1", len(c.Conns))
	}
	return c.Conns[0]
}

func decode(t testing.TB, c capture.Conn) replay.Timeline {
	t.Helper()
	tl, err := replay.Decode(c, 0)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	return tl
}

type message struct {
	topic   string
	payload []byte
}

// edgeOutput is what one Sparkplug edge node (plant1/press7) sends: its
// NDEATH Will, then NBIRTH and NDATA with seq 1 to 3. Every NBIRTH and
// NDATA carries Temperature and Pressure.
func edgeOutput(t testing.TB) (death message, msgs []message) {
	t.Helper()
	e := sparkplug.NewEdge("plant1", "press7", 0)
	metrics := func(i int) []sparkplug.Metric {
		return []sparkplug.Metric{
			{Name: "Temperature", DataType: sparkplug.Double, Value: 21.5 + float64(i)},
			{Name: "Pressure", DataType: sparkplug.Double, Value: 1.01 + float64(i)/100},
		}
	}
	dt, dp := e.Death()
	death = message{dt, dp}
	bt, bp := e.Birth(metrics(0)...)
	msgs = append(msgs, message{bt, bp})
	for i := 1; i <= 3; i++ {
		topic, payload := e.Data(metrics(i)...)
		msgs = append(msgs, message{topic, payload})
	}
	return death, msgs
}

// hostSession is a host application's session: CONNECT, SUBSCRIBE to
// spBv1.0/# at QoS 1, the edge node's messages from the broker at QoS 1,
// each acknowledged, then DISCONNECT and close.
func hostSession(t testing.TB) capture.Conn {
	t.Helper()
	_, msgs := edgeOutput(t)
	s := newSession(t, hostAddr, brokerAddr)
	s.client(packets.Packet{FixedHeader: fh(packets.Connect),
		Connect: packets.ConnectParams{ClientIdentifier: "host1", Clean: true, Keepalive: 60}})
	s.server(packets.Packet{FixedHeader: fh(packets.Connack)})
	s.client(packets.Packet{FixedHeader: fh(packets.Subscribe), PacketID: 1,
		Filters: packets.Subscriptions{{Filter: "spBv1.0/#", Qos: 1}}})
	s.server(packets.Packet{FixedHeader: fh(packets.Suback), PacketID: 1, ReasonCodes: []byte{1}})
	for i, m := range msgs {
		id := uint16(i + 1)
		s.server(publishPk(m.topic, 1, id, m.payload))
		s.client(packets.Packet{FixedHeader: fh(packets.Puback), PacketID: id})
	}
	s.client(packets.Packet{FixedHeader: fh(packets.Disconnect)})
	return s.clientClose().conn
}

// hostServer is the client configuration that matches hostSession.
func hostServer() mqttclient.Server {
	return mqttclient.Server{Address: brokerAddr.String(), ProtocolVersion: mqttclient.MQTT311,
		ClientID: "host1", CleanStart: true, KeepAlive: 60}
}

// edgeSession is an edge node's own session, as captured at the edge:
// CONNECT with death as the Will, msgs at QoS 1, each acknowledged, then
// the connection drops with no DISCONNECT. Pass the output of one
// edgeOutput call: its payloads carry the time they were built.
func edgeSession(t testing.TB, death message, msgs []message) capture.Conn {
	t.Helper()
	s := newSession(t, edgeAddr, brokerAddr)
	s.client(packets.Packet{FixedHeader: fh(packets.Connect), Connect: packets.ConnectParams{
		ClientIdentifier: "press7", Clean: true, Keepalive: 30,
		WillFlag: true, WillTopic: death.topic, WillPayload: death.payload, WillQos: 1}})
	s.server(packets.Packet{FixedHeader: fh(packets.Connack)})
	for i, m := range msgs {
		id := uint16(i + 1)
		s.client(publishPk(m.topic, 1, id, m.payload))
		s.server(packets.Packet{FixedHeader: fh(packets.Puback), PacketID: id})
	}
	return s.serverClose().conn
}

// run is a replay in progress against a live client.
type run struct {
	t    *testing.T
	ctx  context.Context
	c    *mqttclient.Client
	msgs chan *mqttclient.Message
	wg   sync.WaitGroup
	res  replay.Result
	err  error
}

// startReplay starts a Replayer for tls and a client for srv that dials
// it. Call finish to wait for the replay.
func startReplay(t *testing.T, tls []replay.Timeline, srv mqttclient.Server) *run {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)
	rp, err := replay.New(tls)
	if err != nil {
		t.Fatal(err)
	}
	r := &run{t: t, ctx: ctx, msgs: make(chan *mqttclient.Message, 100)}
	r.wg.Go(func() { r.res, r.err = rp.Run(ctx) })
	t.Cleanup(r.wg.Wait) // after cancel, so Run has returned
	srv.Dial = rp.Dial
	r.c, err = mqttclient.New(mqttclient.Options{Servers: []mqttclient.Server{srv}},
		mqttclient.Handlers{OnMessage: func(_ *mqttclient.Client, m *mqttclient.Message) { r.msgs <- m }})
	if err != nil {
		t.Fatal(err)
	}
	return r
}

// subscribe connects and subscribes to filter at QoS 1.
func (r *run) subscribe(filter string) { r.subscribeQoS(filter, 1) }

// subscribeQoS connects and subscribes to filter at qos.
func (r *run) subscribeQoS(filter string, qos byte) {
	r.t.Helper()
	if _, err := r.c.Connect(r.ctx); err != nil {
		r.t.Fatalf("Connect: %v", err)
	}
	if _, err := r.c.Subscribe(r.ctx, []mqttclient.Subscription{{Topic: filter, QoS: qos}}, nil); err != nil {
		r.t.Fatalf("Subscribe: %v", err)
	}
}

// receive waits for n messages.
func (r *run) receive(n int) []*mqttclient.Message {
	r.t.Helper()
	var got []*mqttclient.Message
	for len(got) < n {
		select {
		case m := <-r.msgs:
			got = append(got, m)
		case <-r.ctx.Done():
			r.t.Fatalf("got %d messages, want %d: %v", len(got), n, r.ctx.Err())
		}
	}
	return got
}

// finish disconnects the client and waits for the replay to end.
func (r *run) finish() replay.Result {
	r.t.Helper()
	if err := r.c.Disconnect(r.ctx, 0, nil); err != nil {
		r.t.Errorf("Disconnect: %v", err)
	}
	r.wg.Wait()
	if r.err != nil {
		r.t.Errorf("Run: %v", r.err)
	}
	return r.res
}

// rawConn is a hand-driven client connection to a Replayer, for tests
// that send exact bytes.
type rawConn struct {
	t  *testing.T
	nc net.Conn
}

// dialRaw starts rp.Run and dials it. Call wait to close the connection
// and get Run's result.
func dialRaw(t *testing.T, rp *replay.Replayer) (*rawConn, func() replay.Result) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)
	var wg sync.WaitGroup
	var res replay.Result
	var runErr error
	wg.Go(func() { res, runErr = rp.Run(ctx) })
	t.Cleanup(wg.Wait)
	nc, err := rp.Dial(ctx, "tcp", brokerAddr.String())
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	if err := nc.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	rc := &rawConn{t: t, nc: nc}
	return rc, func() replay.Result {
		t.Helper()
		_ = nc.Close()
		wg.Wait()
		if runErr != nil {
			t.Errorf("Run: %v", runErr)
		}
		return res
	}
}

func (rc *rawConn) send(b []byte) {
	rc.t.Helper()
	if _, err := rc.nc.Write(b); err != nil {
		rc.t.Fatalf("write: %v", err)
	}
}

// expect reads len(want) bytes and checks they are want.
func (rc *rawConn) expect(want []byte) {
	rc.t.Helper()
	got := make([]byte, len(want))
	if _, err := io.ReadFull(rc.nc, got); err != nil {
		rc.t.Fatalf("read: %v", err)
	}
	if !bytes.Equal(got, want) {
		rc.t.Fatalf("got % x, want % x", got, want)
	}
}
