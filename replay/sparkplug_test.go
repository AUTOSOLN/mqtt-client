package replay_test

// The proof of concept from docs/pcap-replay-plan.md: Sparkplug checks on
// a captured session, at the three levels the plan describes, plus an edge
// node's capture delivered to a subscriber.

import (
	"bytes"
	"fmt"
	"slices"
	"testing"

	mqttclient "github.com/AUTOSOLN/mqtt-client"
	"github.com/AUTOSOLN/mqtt-client/capture"
	"github.com/AUTOSOLN/mqtt-client/replay"
	"github.com/AUTOSOLN/mqtt-client/sparkplug"
	"github.com/AUTOSOLN/mqtt-client/sparkplug/verify"
)

// sparkplugTimeline is the host-application session, through a pcap file.
func sparkplugTimeline(t *testing.T) replay.Timeline {
	t.Helper()
	return decode(t, throughFile(t, hostSession(t)))
}

// Level 1: what the capture holds, with no client involved.
func TestDecode_SparkplugNDATA_HasTemperatureAndPressure(t *testing.T) {
	tl := sparkplugTimeline(t)
	var n int
	for _, p := range tl.Packets {
		if p.Dir != capture.FromServer || p.Type != replay.Publish {
			continue
		}
		top, err := sparkplug.ParseTopic(p.Topic)
		if err != nil || top.Type != sparkplug.NDATA {
			continue
		}
		pl, err := sparkplug.Unmarshal(p.Payload)
		if err != nil {
			t.Errorf("NDATA at %v: %v", p.Time, err)
			continue
		}
		for _, name := range []string{"Temperature", "Pressure"} {
			if pl.Metric(name) == nil {
				t.Errorf("NDATA seq %d: no %s metric", *pl.Seq, name)
			}
		}
		n++
	}
	if n != 3 {
		t.Errorf("got %d NDATA, want 3", n)
	}
}

// brokerPublishes returns the broker PUBLISHes of a timeline, in order.
func brokerPublishes(tl replay.Timeline) []message {
	var ms []message
	for _, p := range tl.Packets {
		if p.Dir == capture.FromServer && p.Type == replay.Publish {
			ms = append(ms, message{p.Topic, p.Payload})
		}
	}
	return ms
}

// feed checks delivered messages with a Sparkplug verifier.
func feed(msgs []*mqttclient.Message) verify.Report {
	v := verify.New(verify.Options{})
	for _, m := range msgs {
		v.Feed(m.Topic, m.Payload, t0)
	}
	return v.Report()
}

// Level 2: the client delivers the captured stream unchanged.
func TestReplay_SparkplugSession_DeliversEveryMessageUnchanged(t *testing.T) {
	tl := sparkplugTimeline(t)
	want := brokerPublishes(tl)

	r := startReplay(t, []replay.Timeline{tl}, hostServer())
	r.subscribe("spBv1.0/#")
	got := r.receive(len(want))
	res := r.finish()

	for i, m := range got {
		if m.Topic != want[i].topic || !bytes.Equal(m.Payload, want[i].payload) {
			t.Errorf("message %d: got %q with %d bytes, want %q with the %d bytes captured",
				i, m.Topic, len(m.Payload), want[i].topic, len(want[i].payload))
		}
	}
	if rep := feed(got); !rep.OK() {
		t.Errorf("verifier: got %v, want no violations", rep)
	}
	if len(res.Divergences) != 0 {
		t.Errorf("got divergences %v, want none", res.Divergences)
	}
}

// publishWithSeq finds the broker PUBLISH whose Sparkplug payload has seq.
func publishWithSeq(tl replay.Timeline, seq uint64) (int, error) {
	for i, p := range tl.Packets {
		if p.Dir != capture.FromServer || p.Type != replay.Publish {
			continue
		}
		if pl, err := sparkplug.Unmarshal(p.Payload); err == nil && pl.Seq != nil && *pl.Seq == seq {
			return i, nil
		}
	}
	return 0, fmt.Errorf("no broker PUBLISH with seq %d", seq)
}

// Level 3: an edited seq order is delivered as edited, and the verifier
// catches it.
func TestReplay_SparkplugSeqEdited_VerifierReportsViolation(t *testing.T) {
	tests := []struct {
		name      string
		edit      func(tl *replay.Timeline) error
		wantSeq   []uint64
		wantKinds []verify.Kind
	}{
		{
			name: "swapped",
			edit: func(tl *replay.Timeline) error {
				i, err := publishWithSeq(*tl, 2)
				if err != nil {
					return err
				}
				j, err := publishWithSeq(*tl, 3)
				if err != nil {
					return err
				}
				tl.Packets[i], tl.Packets[j] = tl.Packets[j], tl.Packets[i]
				return nil
			},
			wantSeq:   []uint64{0, 1, 3, 2},
			wantKinds: []verify.Kind{verify.KindGap, verify.KindReorder},
		},
		{
			name: "dropped",
			edit: func(tl *replay.Timeline) error {
				i, err := publishWithSeq(*tl, 2)
				if err != nil {
					return err
				}
				tl.Packets = slices.Delete(tl.Packets, i, i+1)
				return nil
			},
			wantSeq:   []uint64{0, 1, 3},
			wantKinds: []verify.Kind{verify.KindGap},
		},
		{
			name: "rewritten",
			edit: func(tl *replay.Timeline) error {
				i, err := publishWithSeq(*tl, 3)
				if err != nil {
					return err
				}
				pl, err := sparkplug.Unmarshal(tl.Packets[i].Payload)
				if err != nil {
					return err
				}
				two := uint64(2)
				pl.Seq = &two
				b, err := pl.Marshal()
				if err != nil {
					return err
				}
				tl.Packets[i], err = tl.Packets[i].WithPayload(b)
				return err
			},
			wantSeq:   []uint64{0, 1, 2, 2},
			wantKinds: []verify.Kind{verify.KindDuplicate},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tl := sparkplugTimeline(t)
			if err := tt.edit(&tl); err != nil {
				t.Fatalf("edit: %v", err)
			}

			r := startReplay(t, []replay.Timeline{tl}, hostServer())
			r.subscribe("spBv1.0/#")
			got := r.receive(len(tt.wantSeq))
			res := r.finish()

			var seqs []uint64
			for _, m := range got {
				pl, err := sparkplug.Unmarshal(m.Payload)
				if err != nil || pl.Seq == nil {
					t.Fatalf("delivered %q: payload %v, err %v", m.Topic, pl, err)
				}
				seqs = append(seqs, *pl.Seq)
			}
			if !slices.Equal(seqs, tt.wantSeq) {
				t.Errorf("delivered seq: got %v, want %v", seqs, tt.wantSeq)
			}
			var kinds []verify.Kind
			for _, v := range feed(got).Violations {
				kinds = append(kinds, v.Kind)
			}
			if !slices.Equal(kinds, tt.wantKinds) {
				t.Errorf("violations: got %v, want %v", kinds, tt.wantKinds)
			}
			if len(res.Divergences) != 0 {
				t.Errorf("got divergences %v, want none", res.Divergences)
			}
		})
	}
}

// An edge node's capture, delivered to a subscriber, ends with its NDEATH.
func TestInbound_EdgeSessionCapture_SubscriberReceivesStreamAndDeath(t *testing.T) {
	death, published := edgeOutput(t)
	want := append(slices.Clone(published), death)
	in, err := replay.Inbound([]replay.Timeline{decode(t, throughFile(t, edgeSession(t, death, published)))})
	if err != nil {
		t.Fatalf("Inbound: %v", err)
	}

	// Any client ID and keepalive: the handshake is Auto.
	r := startReplay(t, []replay.Timeline{in}, mqttclient.Server{Address: brokerAddr.String(),
		ProtocolVersion: mqttclient.MQTT311, ClientID: "host-under-test", CleanStart: true, KeepAlive: 15})
	r.subscribe("spBv1.0/#")
	got := r.receive(len(want))
	res := r.finish()

	for i, m := range got {
		if m.Topic != want[i].topic || !bytes.Equal(m.Payload, want[i].payload) {
			t.Errorf("message %d: got %q with %d bytes, want %q with the %d bytes the edge node sent",
				i, m.Topic, len(m.Payload), want[i].topic, len(want[i].payload))
		}
		if m.Retain || m.Dup {
			t.Errorf("message %d %q: got retain %v dup %v, want both clear", i, m.Topic, m.Retain, m.Dup)
		}
	}
	rep := feed(got)
	if !rep.OK() || rep.Totals.Births != 1 || rep.Totals.Deaths != 1 {
		t.Errorf("verifier: got %v with %d births and %d deaths, want no violations, 1 and 1",
			rep, rep.Totals.Births, rep.Totals.Deaths)
	}
	if e := rep.Edges["plant1/press7"]; e.Online {
		t.Errorf("edge plant1/press7: got online after NDEATH, want offline")
	}
	if len(res.Divergences) != 0 {
		t.Errorf("got divergences %v, want none", res.Divergences)
	}
}
