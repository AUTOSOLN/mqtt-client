package replay_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	mqttclient "github.com/AUTOSOLN/mqtt-client"
	"github.com/AUTOSOLN/mqtt-client/replay"
)

func TestReplay_LiveSubscribeDiffers_RecordsDivergenceAndGoesOn(t *testing.T) {
	tl := decode(t, hostSession(t))
	r := startReplay(t, []replay.Timeline{tl}, hostServer())
	r.subscribe("spBv1.0/plant1/#") // the capture has spBv1.0/#
	r.receive(len(brokerPublishes(tl)))
	res := r.finish()

	if len(res.Divergences) != 1 || !strings.Contains(res.Divergences[0].Detail, "SUBSCRIBE filters") {
		t.Errorf("got divergences %v, want one about the SUBSCRIBE filters", res.Divergences)
	}
}

func TestReplay_LiveClientSkipsSubscribe_EndsConnectionWithDivergence(t *testing.T) {
	tl := decode(t, hostSession(t))
	r := startReplay(t, []replay.Timeline{tl}, hostServer())
	if _, err := r.c.Connect(r.ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	res := r.finish() // DISCONNECT where the capture has SUBSCRIBE

	if len(res.Divergences) == 0 || !strings.Contains(res.Divergences[0].Detail, "sent DISCONNECT, capture has SUBSCRIBE") {
		t.Errorf("got divergences %v, want the first to say DISCONNECT came where the capture has SUBSCRIBE", res.Divergences)
	}
}

func TestReplay_WrongProtocolVersion_FatalDivergence(t *testing.T) {
	tl := decode(t, hostSession(t))
	srv := hostServer()
	srv.ProtocolVersion = mqttclient.MQTT5
	r := startReplay(t, []replay.Timeline{tl}, srv)
	// The replay ends the connection before CONNACK, so Connect fails.
	if _, err := r.c.Connect(r.ctx); err == nil {
		t.Errorf("Connect: got no error, want one")
	}
	r.wg.Wait()

	if len(r.res.Divergences) != 1 || !strings.Contains(r.res.Divergences[0].Detail, "protocol version 5, the capture 4") {
		t.Errorf("got divergences %v, want one about the protocol version", r.res.Divergences)
	}
}

func TestReplayer_DialAfterLastTimeline_ErrExhausted(t *testing.T) {
	rp, err := replay.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rp.Run(t.Context()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if _, err := rp.Dial(t.Context(), "tcp", brokerAddr.String()); !errors.Is(err, replay.ErrExhausted) {
		t.Errorf("Dial: got %v, want ErrExhausted", err)
	}
	if _, err := rp.Run(t.Context()); !errors.Is(err, replay.ErrRunning) {
		t.Errorf("second Run: got %v, want ErrRunning", err)
	}
}

func TestReplayer_RunCancelled_ReturnsContextError(t *testing.T) {
	rp, err := replay.New([]replay.Timeline{decode(t, hostSession(t))})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel() // before any Dial
	if _, err := rp.Run(ctx); !errors.Is(err, context.Canceled) {
		t.Errorf("got %v, want context.Canceled", err)
	}
}
