package mqttclient

import (
	"errors"
	"testing"
	"time"
)

// expectSilence checks the client sends nothing for a moment.
func (f *fakeConn) expectSilence() {
	f.t.Helper()
	if b, err := f.tryReadRaw(200 * time.Millisecond); err == nil {
		f.t.Fatalf("unexpected packet % x", b)
	}
}

func manualAckClient(t *testing.T, b *fakeBroker, srv Server) (*Client, *pubRecorder, *fakeConn) {
	t.Helper()
	c, rec := newPubClientWith(t, b, srv, Options{ManualAck: true})
	fc := handshake(t, b, c, connackBytes(t, max(srv.ProtocolVersion, MQTT311), 0, false, nil))
	return c, rec, fc
}

// PUBACKs wait for Ack and go out in delivery order, whatever order Ack is
// called in.
func TestManualAckQoS1InOrder(t *testing.T) {
	b := newFakeBroker(t)
	c, rec, fc := manualAckClient(t, b, Server{ProtocolVersion: MQTT311})

	fc.send(0x32, 0x06, 0x00, 0x01, 'a', 0x00, 0x01, 'x')
	fc.send(0x32, 0x06, 0x00, 0x01, 'a', 0x00, 0x02, 'y')
	m1, m2 := rec.message(t), rec.message(t)
	fc.expectSilence()

	if err := c.Ack(m2); err != nil {
		t.Fatal(err)
	}
	fc.expectSilence() // m1 first
	if err := c.Ack(m1); err != nil {
		t.Fatal(err)
	}
	fc.expect(0x40, 0x02, 0x00, 0x01)
	fc.expect(0x40, 0x02, 0x00, 0x02)

	if err := c.Ack(m1); err != nil { // already acked
		t.Fatal(err)
	}
	fc.send(0x30, 0x04, 0x00, 0x01, 'a', 'z') // QoS 0: nothing to ack
	if err := c.Ack(rec.message(t)); err != nil {
		t.Fatal(err)
	}
	fc.expectSilence()
}

// A QoS 2 message is delivered on PUBREL; PUBCOMP waits for Ack.
func TestManualAckQoS2(t *testing.T) {
	b := newFakeBroker(t)
	c, rec, fc := manualAckClient(t, b, Server{ProtocolVersion: MQTT311})

	fc.send(0x34, 0x06, 0x00, 0x01, 'a', 0x00, 0x07, 'x')
	fc.expect(0x50, 0x02, 0x00, 0x07)
	fc.send(0x62, 0x02, 0x00, 0x07)
	m := rec.message(t)
	fc.expectSilence()
	if err := c.Ack(m); err != nil {
		t.Fatal(err)
	}
	fc.expect(0x70, 0x02, 0x00, 0x07)
}

// An unacknowledged message keeps its Receive Maximum slot: with Receive
// Maximum 1, a second QoS 1 message before Ack is a protocol violation, and
// after Ack it is not.
func TestManualAckReceiveMaximum(t *testing.T) {
	b := newFakeBroker(t)
	c, rec, fc := manualAckClient(t, b, Server{ProtocolVersion: MQTT5, ReceiveMaximum: 1})

	fc.send(0x32, 0x07, 0x00, 0x01, 'a', 0x00, 0x01, 0x00, 'x')
	if err := c.Ack(rec.message(t)); err != nil {
		t.Fatal(err)
	}
	fc.expect(0x40, 0x02, 0x00, 0x01)
	fc.send(0x32, 0x07, 0x00, 0x01, 'a', 0x00, 0x02, 0x00, 'y') // slot free again
	rec.message(t)
	fc.send(0x32, 0x07, 0x00, 0x01, 'a', 0x00, 0x03, 0x00, 'z') // slot still held by mid 2
	fc.expect(0xE0, 0x01, 0x93)
	if ev := rec.disconnect(t); ev.Err == nil {
		t.Fatal("OnDisconnect without error")
	}
}

// A message from a connection that has ended can't be acknowledged.
func TestManualAckAfterReconnect(t *testing.T) {
	b := newFakeBroker(t)
	c, rec, fc := manualAckClient(t, b, Server{ProtocolVersion: MQTT311})
	fc.send(0x32, 0x06, 0x00, 0x01, 'a', 0x00, 0x01, 'x')
	m := rec.message(t)

	fc.nc.Close()
	rec.disconnect(t)
	fc = handshake(t, b, c, connackBytes(t, 4, 0, false, nil))
	if err := c.Ack(m); !errors.Is(err, ErrNoConn) {
		t.Fatalf("Ack after reconnect: %v, want ErrNoConn", err)
	}
	fc.expectSilence()
}
