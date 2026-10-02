# mqtt-client — feasibility and plan

Status: draft · 2026-10-02 · `github.com/AUTOSOLN/mqtt-client`

## Goal

Replace `github.com/eclipse/paho.golang` (`paho` + `autopaho`) in swarmy-mqtt with an in-house
Go client that:

- follows the semantics of the **libmosquitto** client API (v2.1.2 at `~/mycode/mosquitto/lib`):
  connect, subscribe, publish, unsubscribe and disconnect, for MQTT 3.1.1 and 5.0
- uses the **AUTOSOLN comqtt fork** packet codec (`~/mycode/comqtt/mqtt/packets`) for all wire
  encoding and decoding.

## Decisions (2026-10-02)

1. **Separate module in its own public repository:** `github.com/AUTOSOLN/mqtt-client`,
   package `mqttclient`, BSD-3-Clause. We vet it in swarmy-mqtt first. Later the private broker (`bitbucket.org/autosoln/broker`)
   consumes it, starting with `libs/mqtt-client`, which today is a 186-line wrapper over paho
   (v3 and v5).
2. **No websocket support.** TCP and TLS only, so the module has no third-party dependency
   besides comqtt.
3. **Behave as mosquitto does** wherever mosquitto and paho differ. For example, inbound QoS 2
   is delivered on PUBREL, and reconnect and resend follow mosquitto's rules.

## Verdict

**Feasible, with moderate effort.** The codec does the hardest byte-level work and the mosquitto
library is a clear reference for behaviour. Most of the new work is the session state machine:
QoS 1/2 inflight tracking, reconnecting, keepalive, flow control and a safe shutdown. That
logic is what autopaho gives us today, and it is where the risk is.

Rough size: about 2–3k lines of Go plus tests for parity with what swarmy uses today.

### Why the codec fits

- One `packets.Packet` struct covers every control packet. Each packet type has both an
  `XxxEncode(*bytes.Buffer)` and an `XxxDecode([]byte)`, so the codec works in both directions,
  not just broker-side. CONNECT, SUBSCRIBE and UNSUBSCRIBE (client → server) encode, and
  CONNACK, SUBACK, UNSUBACK and PUBLISH (server → client) decode.
- It handles v3.1.1 and v5 through `pk.ProtocolVersion`. Properties are validated per packet
  type by `validPacketProperties`.
- It needs nothing outside the standard library except `mqtt/mempool`, which is also
  stdlib-only. The binary gains no third-party code.
- `tpackets.go` is a non-test file with byte-exact fixtures (`TPacketData`) for every packet
  type and version. We can reuse it directly in client codec tests.
- License: MIT (mochi-mqtt). Keep the SPDX headers.

### Why mosquitto is a good *reference*, but not something to transliterate

libmosquitto is about 10k lines of C. Roughly half of it is code we don't need in Go:

| mosquitto lib area | Go equivalent |
|---|---|
| `packet_mosq.c`, `packet_datatypes.c`, `property_mosq.c`, `send_*.c` (encoding) | comqtt `packets` |
| `net_mosq.c`, `tls_mosq.c`, `net_mosq_ocsp.c` | `net.Dialer` + `crypto/tls` |
| `thread_mosq.c`, `pthread_compat.h`, socketpair wake-ups in `loop.c` | goroutines + channels |
| `srv_mosq.c`, `socks_mosq.c`, `http_client.c`, `net_ws.c` | out of scope (no websocket, by decision) |

The part worth porting is its **behaviour and API shape**:

- `connect.c`, `loop.c`: connect, reconnect backoff (`reconnect_delay_set`), keepalive
  (`loop_misc`)
- `messages_mosq.c`: outbound and inbound message queues, inflight limit, and resend on reconnect
  (`message__reconnect_reset`)
- `handle_*.c`: CONNACK, PUBACK/PUBREC/PUBREL/PUBCOMP, SUBACK, UNSUBACK, DISCONNECT, PINGRESP
- `actions_*.c`: argument checks and message ID (mid) allocation for publish, subscribe and
  unsubscribe
- `alias_mosq.c`, `extended_auth.c`: v5 extras (later phases)
- `libcommon_topic.h`: `pub_topic_check` / `sub_topic_check` (small; port these)

License: mosquitto is `EPL-2.0 OR BSD-3-Clause`. Choose **BSD-3-Clause (EDL-1.0)** for any code
derived from it, and add an attribution header to those files.

## Codec gaps found (client direction)

The fork was written as a broker codec. These problems show up when a client uses it:

| # | Issue | Where | Action |
|---|---|---|---|
| 1 | `DisconnectDecode` only reads the reason code when `Remaining > 1`. A v5 DISCONNECT with `Remaining == 1` (reason code, no properties, which the spec allows) loses its reason code. | `packets.go:568` | **Fixed in fork** (branch `BB-724`; `> 0`, test `TDisconnectReasonCodeOnly`). Also read the reason byte directly in the client, so the module works with whatever comqtt commit a consumer pins. |
| 2 | `Properties.Decode` returns EOF on an empty buffer. A broker that refuses v5 often sends a v3-style CONNACK (`Remaining == 2`, rc `0x01`), which then fails to decode. | `properties.go:372`, `ConnackDecode` | In the client: when `Remaining == 2`, decode the CONNACK as v4 |
| 3 | `ResponseTopic` and `CorrelationData` (PUBLISH and Will) are silently dropped unless `Mods.AllowResponseInfo` is true. | `properties.go` Encode | Client always sets `AllowResponseInfo = true` on outbound packets |
| 4 | `Mods.MaxSize` silently drops ReasonString and User properties. Nothing checks the total packet size against the server's Maximum Packet Size. | Encode | Client sets `MaxSize` from CONNACK and rejects oversize PUBLISH with an error (mosquitto `MOSQ_ERR_OVERSIZE_PACKET`) |
| 5 | `FixedHeader.Type`, `ProtocolVersion` and `Connect.ProtocolName` ("MQTT") must be set before encoding. `Type` selects the properties table. | all | Private constructors in the client (`newConnect`, `newPublish`, …) |
| 6 | Flag-gated properties: `SessionExpiryIntervalFlag`, `RequestProblemInfoFlag`, `PayloadFormatFlag`, `TopicAliasFlag`. Setting a value without its flag does nothing. | Encode | Builders set the flags |
| 7 | `PublishDecode` and `SubackDecode` slice the input buffer. | decode | Allocate a new buffer per packet. Don't use a shared read buffer. |
| 8 | `*Validate()` functions check server-side rules. | — | Client does its own outbound checks (port of the topic checks, QoS ≤ server MaximumQoS, RetainAvailable) |
| 9 | A v3 UNSUBACK has no reason codes. | `UnsubackDecode` | Client fills in Success codes |

Only #1 needs a change in the fork. The rest are handled in the client. The client depends only
on codec API that is the same in upstream `wind-c/comqtt` and the fork. It must never require
fork-only behaviour.

## Module layout and wiring

The module lives in its own public repository, `github.com/AUTOSOLN/mqtt-client`.

```
github.com/AUTOSOLN/mqtt-client     (package mqttclient, BSD-3-Clause)
  go.mod
    module github.com/AUTOSOLN/mqtt-client
    go 1.25.5                         // not higher: fork needs 1.25.5, broker libs are on 1.25.5
    require github.com/wind-c/comqtt/v2 v2.6.1
    replace github.com/wind-c/comqtt/v2 => github.com/AUTOSOLN/comqtt/v2 v2.6.2-0.20261002195201-5268a0644cd6
  go.upstream.mod, go.upstream.sum    // same, without the replace (CI compatibility job)
  .github/workflows/ci.yml
  doc.go, LICENSE, README.md, PLAN.md
  *.go                                // the client (see Internal design)
  conformance/                        // Go ports of mosquitto test/lib/c programs
  integration/go.mod                  // nested module; real mosquitto + in-process comqtt broker
```

**We keep the import path `github.com/wind-c/comqtt/v2` and do not rename the fork's module
path.** The broker's `src/go.mod` already imports it under that path, with
`replace github.com/wind-c/comqtt/v2 => github.com/AUTOSOLN/comqtt/v2 …28e9277eb8b4`. Renaming the
fork would break that.

Go only honours `replace` in the **main** module. The `replace` above therefore applies only when
building or testing this repository. Each consumer adds its own:

- **swarmy-mqtt**, during vetting: `go.work` (already git-ignored there) lets you develop against
  a local checkout without committing a path `replace`:
  ```
  go work init . ../mqtt-client
  ```
  To ship, require a tagged version and add the comqtt redirect:
  ```
  require github.com/AUTOSOLN/mqtt-client v0.1.0
  replace github.com/wind-c/comqtt/v2 => github.com/AUTOSOLN/comqtt/v2 <pseudo-version>
  ```
- **broker**: `go get github.com/AUTOSOLN/mqtt-client@v0.1.0`. Its existing comqtt `replace`
  decides which fork commit is used. Its pin (`28e9277`) is older than ours (`5268a06`, branch `BB-724`). The
  upstream CI job is what proves we don't depend on anything that differs between commits. The
  repository is public, so no `GOPRIVATE` is needed.

**Releases:** plain semver tags (`v0.1.0`, `v0.2.0`, …). Stay on v0.x until the broker has run it
in production.

**Dependency footprint:** graph pruning keeps comqtt's broker dependencies (raft, redis, badger,
…) out of consumer builds. Only `packets` and `mempool` (both stdlib-only) are compiled in.

**CI:** in addition to the normal job, run one job against upstream `wind-c/comqtt/v2` with no
`replace`, using `-modfile=go.upstream.mod`. That proves the client doesn't depend on
fork-only behaviour.

## API

The client keeps mosquitto's model: callbacks report what happens on the connection, and options
and setters mirror `mosquitto_*_set`. It adds Go idioms: `context`, blocking calls that wait for
the server, and goroutine safety. Callbacks run one at a time, in order, on a dispatcher goroutine,
the same as callbacks on mosquitto's loop thread. The dispatcher goroutine exists only while
callbacks are queued.

### Implemented (phases 1 and 2)

```go
func New(opts Options, h Handlers) (*Client, error)               // mosquitto_new + option setters
func (c *Client) Connect(ctx context.Context) (ConnAck, error)    // dial, CONNECT, wait for CONNACK; no reconnect
func (c *Client) Disconnect(ctx context.Context, reason byte, props *Properties) error  // mosquitto_disconnect_v5; waits for teardown
func (c *Client) SetCredentials(username string, password []byte) error               // mosquitto_username_pw_set
func (c *Client) SetWill(w *Message) error                                            // will_set_v5; nil = will_clear
func (c *Client) ClientID() string                                                    // includes a server-assigned id
func (c *Client) IsConnected() bool
func (c *Client) Publish(ctx context.Context, m *Message) (*Pending, error)          // mosquitto_publish_v5 (phase 2)

type Pending struct{ Mid uint16 /* … */ }
func (p *Pending) Wait(ctx context.Context) (Result, error) // *ReasonCodeError for reason >= 0x80; ctx does not cancel the message
func (p *Pending) Done() <-chan struct{}
type Result struct {
    ReasonCode byte        // final acknowledgement (MQTT 5); 0 for QoS 0
    Properties *Properties
}

type Message struct {
    Topic      string
    Payload    []byte
    QoS        byte
    Retain     bool
    Properties *Properties // MQTT 5
    Mid        uint16      // set on received QoS 1/2 messages
}

type Options struct {
    Server            string        // one URL: mqtt:// tcp:// (1883), mqtts:// ssl:// tls:// (8883)
    ProtocolVersion   byte          // MQTT31, MQTT311 (default, as mosquitto), MQTT5
    ClientID          string        // empty only with CleanStart; MQTT31 gets a random mosq-… id
    CleanStart        bool
    KeepAlive         uint16        // seconds; 0 or >= 5
    Username          string
    Password          []byte
    Will              *Message
    ConnectProperties *Properties   // MQTT 5
    ReceiveMaximum    uint16        // default 20; always sent in MQTT 5 CONNECT, as mosquitto does
    MaxInflight       uint16        // default 20; outbound QoS 1/2 in flight unless the server sends Receive Maximum
    TLSConfig         *tls.Config
    Logger            *slog.Logger
}

type Handlers struct {
    OnPreConnect func(c *Client)                     // synchronous, before dialling
    OnConnect    func(c *Client, ack ConnAck)        // every CONNACK, accepted or refused
    OnDisconnect func(c *Client, ev DisconnectEvent) // once per established connection
    OnMessage    func(c *Client, m *Message)         // QoS 0/1 on arrival (after PUBACK), QoS 2 on PUBREL (after PUBCOMP)
    OnPublish    func(c *Client, mid uint16, reason byte, props *Properties) // QoS 0 written, PUBACK, PUBCOMP, or failed PUBREC
}

type DisconnectEvent struct {
    Err        error       // nil for Disconnect(); else ErrConnectionLost, ErrKeepalive, ErrProtocol,
                           // ErrMalformedPacket, ErrServerDisconnect, *ConnRefusedError, ctx error
    ReasonCode byte        // server DISCONNECT (MQTT 5)
    Properties *Properties
}
```

`Server` is a single URL rather than a list: mosquitto connects to one host, and so does swarmy.

Publish follows `mosquitto_publish_v5`. Every message takes a mid, QoS 0 included. Argument
errors (`ErrInvalid`), a QoS above the server's Maximum QoS (`ErrQoSNotSupported`) and a packet
over the server's Maximum Packet Size (`ErrOversizePacket`) are returned before a mid is taken.
Retain is silently cleared when the server does not support it. A QoS 0 message needs a
connection (`ErrNoConn`). QoS 1 and 2 messages are queued and sent while the send quota allows
(the server's Receive Maximum, else `MaxInflight`). They are kept across connections until
acknowledged, so a message published while disconnected goes out after the next `Connect`.

### Still to come

```go
// Handlers additions
OnSubscribe   func(c *Client, mid uint16, granted []byte, props *Properties) // phase 3
OnUnsubscribe func(c *Client, mid uint16, reasons []byte, props *Properties) // phase 3

func (c *Client) Subscribe(ctx context.Context, subs []Subscription, props *Properties) (*Pending, error) // phase 3
func (c *Client) Unsubscribe(ctx context.Context, topics []string, props *Properties) (*Pending, error)   // phase 3
func (c *Client) Start(ctx context.Context) error   // phase 4: connect_async + loop_forever semantics (reconnect)

// Result additions (phase 3): granted QoS / reason codes per topic for SUBACK and UNSUBACK

// Options additions (phase 4): ReconnectDelay, ReconnectDelayMax, ReconnectExponential
```

## Internal design

```
            ┌──────────── Client ────────────┐
 Publish ──▶│ session: mid alloc, outbound   │──▶ write (mutex) ───────▶ conn
 Subscribe  │   inflight (QoS1/2), inbound    │                          │
            │   QoS2 awaiting-PUBREL, pending │◀── reader goroutine ◀────┘
            │   sub/unsub, receive-max sema   │        (ReadFixedHeader / ReadPacket port)
            └──────┬─────────────────────────┘
                   ├──▶ dispatcher goroutine → Handlers (in order)
                   └──▶ keepalive ticker (PINGREQ / PINGRESP deadline)
            supervisor goroutine: dial → CONNECT/CONNACK → run → on error back off → redial
```

Files at the repository root (✓ = exists):

- ✓ `client.go`: `Client`, `New`, `Connect`, `Disconnect`, dialling (tcp, tls); later `Start` and
  the supervisor loop
- ✓ `options.go`: `Options`, `Handlers`, `Message`, defaults, validation, topic checks
- ✓ `conn.go`: one network connection: reader goroutine, keepalive (PINGREQ on idle, PINGRESP
  timeout), CONNACK and DISCONNECT handling, teardown
- ✓ `wire.go`: `readPacket` around comqtt (ported from comqtt `mqtt/clients.go`
  `ReadFixedHeader`/`ReadPacket`), packet builders, and the fixes for codec gaps 2–9
- ✓ `session.go`: mid allocator (skip mids in use, wrap at 65535), the ordered outbound queue
  with its send quota, the inbound QoS 2 store and receive quota, and the per-connection
  reset and resend (`messages_mosq.c`). The session lives on the `Client`, so it outlives each
  connection.
- ✓ `publish.go`: `Publish`, `Pending`, argument and size checks (`actions_publish.c`)
- ✓ `handle.go`: PUBLISH, PUBACK, PUBREC, PUBREL and PUBCOMP handlers (the `handle_*.c`
  equivalents), including unexpected acknowledgements
- ✓ `dispatch.go`: the callback dispatcher
- ✓ `errors.go`: typed errors and reason-code text

Every write has a deadline: the caller's ctx, or else one keepalive period. A stalled socket then
closes the connection instead of blocking the reader or keepalive goroutine. Packets are written
while holding the session lock, so messages leave in mid order.

Behaviour follows mosquitto (decision 3). Where mosquitto and paho differ, mosquitto wins:

- **QoS 2 inbound:** store the PUBLISH, send PUBREC, and call `OnMessage` when PUBREL arrives
  (`handle_publish.c` / `handle_pubrel.c`). When a PUBLISH arrives with a mid that is still awaiting
  PUBREL, we send PUBREC again, the same as mosquitto. **Planned deviation:** we replace the
  stored message instead of queueing a second copy. Mosquitto (`handle_publish.c:207`) appends a
  second entry under the same mid, and only one is removed when PUBREL arrives, so the extra
  entry is never cleared. swarmy will therefore show a QoS 2 message only
  after the PUBREL.
- **Reconnect** (`handle_connack.c`, `message__reconnect_reset`, `message__retry_check`):
  - Outbound inflight messages are **kept** across reconnects, whether or not the broker reports
    Session Present.
  - Their state is rewound: a QoS 1 or QoS 2 PUBLISH waiting for an ack goes back to "publish",
    and a QoS 2 message waiting for PUBCOMP goes back to "resend PUBREL".
  - After a successful CONNACK, and before `OnConnect` runs, outbound messages are sent again in
    order, up to the quota: PUBLISH with DUP=1 (DUP=0 if it was never sent) and PUBREL. PUBREC
    is not resent for inbound messages. Neither mosquitto nor the spec does this: the server
    resends its PUBLISH or PUBREL.
  - Messages beyond the inflight quota stay queued.
  - Inbound: QoS 2 messages awaiting PUBREL are kept when CONNACK reports Session Present and
    dropped otherwise (see Deviations).
  - The inflight quota is reset to the server's Receive Maximum.
  - **Done in phase 2** for an explicit `Connect` after a lost connection. Phase 4's supervisor
    reuses it.
- **Retry on the same connection:** none. In mosquitto 2.x, `mosquitto_message_retry_set` does
  nothing (`messages_mosq.c:328`), and resends happen only after a CONNACK.
- **v5 topic alias:** send `TopicAliasMaximum = 0` so the broker never sends us aliases. Outbound
  aliasing comes in phase 6.
- **Server-supplied values (v5):** apply Assigned Client ID, Server Keep Alive, Receive Maximum,
  Maximum QoS, Retain Available and Maximum Packet Size from CONNACK.

## What swarmy needs (parity checklist)

From `main.go` today:

- [ ] Multiple concurrent connections, each with its own context and cancel
- [ ] `mqtt://` URL (the UI default); `mqtts://` should be supported too
- [ ] KeepAlive, clean start, session expiry, client ID, username and password
- [ ] Will message (topic, payload, QoS, retain); empty Will properties
- [ ] Auto-reconnect, plus a connection-up callback that re-subscribes (`onConnectionUp`)
- [ ] Connect-error, client-error and server-disconnect callbacks (with the v5 reason string)
- [ ] Incoming message callback (topic, payload, QoS, retain)
- [ ] Blocking publish with a 5s context timeout
- [ ] Graceful disconnect with a 5s context timeout

New capability once paho is gone: paho.golang is **v5-only**, so swarmy can now talk to
**3.1.1 brokers** too. Add `ProtocolVersion` to `ConnectionItem` (default 5) and a selector in
the UI.

## Testing strategy

1. **Codec round-trip:** encode client → server packets and decode server → client packets
   using comqtt's `TPacketData` fixtures.
2. **Unit tests** for the session state machine with an in-memory `net.Pipe` scripted broker:
   QoS 1/2 flows, out-of-order acks, unexpected acks, and receive-maximum blocking.
3. **Mosquitto conformance (byte-exact):** `mosquitto/test/lib/*.py` runs a Python mock broker
   that drives a small client program and checks every byte. Port the matching
   `test/lib/c/*.c` programs to tiny Go `main`s under `conformance/` and run them in the
   same harness. The suites that cover our scope:
   - `01-*` connect, will, credentials, keepalive, no-clean-session, extended auth
   - `02-*` subscribe and unsubscribe (v3 and v5, multiple)
   - `03-*` publish c2b and b2c at QoS 0/1/2, receive-maximum, maximum-qos, unexpected acks,
     request/response
   - `04-*` retain; `08-*` TLS; `11-*` properties and oversize packets
4. **Integration:** run against a real `mosquitto` broker (built from the local repo) and an
   in-process comqtt broker, both v3.1.1 and v5. These live in a **nested module**,
   `integration/go.mod`, so the comqtt broker's dependencies (raft, badger, …) never
   enter the root module's `go.mod` or `go.sum`.
5. Run everything with `go test -race`. Add a fuzz target on `readPacket`.

## Phases

| Phase | Scope | Exit criteria |
|---|---|---|
| 0 | **Done.** Repository, LICENSE, README; comqtt gap #1 fixed on branch `BB-724` (`5268a06`, not yet merged to main); `go.upstream.mod`; CI workflow (tests with both modfiles, mosquitto conformance) | `go vet`, `go test -race` pass with both modfiles |
| 1 | **Done.** Dial (tcp, tls), CONNECT/CONNACK, keepalive, PINGREQ/PINGRESP both ways, DISCONNECT both ways, protocol-error DISCONNECT, v3.1, v3.1.1 and v5 | all 12 non-auth `01-*` conformance cases pass; unit tests with a scripted broker pass 20× under `-race` |
| 2 | **Done.** PUBLISH out and in at QoS 0/1/2, mid allocator, inflight queue, receive maximum both ways, max packet size, resend after reconnect, `Pending.Wait`; `cmd/mqttpub` test tool | 19 of the 22 `03-*` scripts and all 6 `11-*` scripts pass (37 conformance cases in total; `11-prop-oversize-packet` with an override); the other three `03-*` scripts subscribe, so they move to phase 3; unit tests pass 20× under `-race` |
| 3 | SUBSCRIBE and UNSUBSCRIBE (multiple, v5 options and properties), `Pending` for SUBACK/UNSUBACK, the SUBSCRIBE/UNSUBSCRIBE size checks of `11-prop-oversize-packet` | `02-*`, `03-publish-loop`, `03-request-response*` pass |
| 4 | Supervisor: reconnect backoff, session resumption, `OnConnect` re-subscribe pattern | `01-no-clean-session` and `03-*-disconnect` still pass with the supervisor instead of the ports' manual `Connect`; integration tests that kill the broker pass |
| 5 | **Swap swarmy over:** add the `require` and the comqtt `replace` (see Module wiring), replace autopaho in `main.go`, add `ProtocolVersion` to `ConnectionItem` and the UI, remove paho from `go.mod` and run `go mod tidy` | parity checklist done; manual run against mosquitto and comqtt brokers; tag `v0.1.0` |
| 6 | **Broker adoption:** reimplement `libs/mqtt-client`'s `MQTTClient` interface (V3 and V5) on this module, then move `tests/broker-throughput`, `tests/functional` and `tests/sparkplug-client` over | those test tools run unchanged; paho removed from `libs/mqtt-client` |
| 7 (optional) | Outbound topic alias, extended AUTH (`01-extended-auth-*`), persistent inflight store | as needed |

Phases 1–4 are the bulk of the work. Phase 5 is small: about 150 lines in `main.go` change,
almost all of it in `connectOne`, `buildConnConfig`, `onConnectionUp`, `onPublishReceived`,
`mqttPublish` and `mqttDisconnectOne`.

## Deviations from mosquitto (so far)

| Behaviour | mosquitto | this client | Why |
|---|---|---|---|
| CONNECT property order | application properties first, then Receive Maximum | comqtt's fixed order | MQTT does not define an order. One conformance script is overridden for this (`conformance/overrides/01-con-discon-success-v5.py`). |
| MQTT 5 CONNACK refusal (reason ≥ 0x80) | returns `MOSQ_ERR_PROTOCOL`, so `handle__packet` sends DISCONNECT 0x82 | closes without sending DISCONNECT; error is `*ConnRefusedError` | The server already closed the session; replying with a protocol error is noise. |
| 3.x CONNACK to an MQTT 5 CONNECT | `on_connect(0x84)`, then protocol error | `OnConnect(0x84)`, `*ConnRefusedError{0x84}` | Same report, clearer error type. |
| `mosquitto_connect` | returns after sending CONNECT; CONNACK arrives in the loop | `Connect` waits for CONNACK (or ctx) | Go callers want the result; OnConnect still fires. |
| Retain Available reset | reset to "available" by `mosquitto_connect`, kept across `mosquitto_reconnect` | reset by every `Connect` | Matches mosquitto for explicit connects; phase 4's reconnect loop must keep the server's last value. |
| Maximum QoS, Maximum Packet Size, Receive Maximum from CONNACK | kept from an earlier CONNACK when absent | reset to the defaults (2, none, `MaxInflight`) on every CONNACK | The values describe the current connection. |
| Inbound QoS 2 duplicate | queues a second copy under the same mid | replaces the stored copy | See Internal design. |
| Maximum Packet Size check | leaves out the fixed header byte (`packet__check_oversize`), so it sends packets 1 byte over the limit | counts the whole packet (MQTT 5 §3.2.2.3.6) | A strict server would disconnect us. `conformance/overrides/11-prop-oversize-packet.py` shortens both payloads by one byte. |
| DUP after reconnect | set on every message released after a reconnect, even ones never sent | set only on messages sent before | [MQTT-3.3.1-1]: DUP marks a re-delivery. |
| Inbound QoS 2 store when CONNACK Session Present = 0 | kept; each one still uses up receive quota | dropped | Their PUBREL can never come. With mosquitto, the stale entries use up Receive Maximum. |
| Too many inbound QoS 1/2 messages (MQTT 5) | protocol error, so DISCONNECT 0x82 | DISCONNECT 0x93 (Receive Maximum exceeded) | The spec's reason code; mosquitto has a FIXME for it. |

## Risks

- **Correctness of the state machine.** autopaho is battle-tested; ours won't be at first. The
  mosquitto conformance suite and `-race` testing are the main mitigation. Keep paho in a
  branch until phase 5 has been used for a while.
- **Coupling to the fork.** Reusing a broker codec ties the client to its quirks (gaps 2–9).
  The private builders in `wire.go` keep that contained in one file. Consumers pin different
  fork commits through their own `replace`. The upstream CI job, and using only the shared
  codec API, protect against that drift.
- **Forgotten `replace` in a consumer.** The build still works and silently uses upstream
  wind-c. This is safe only because the client never relies on fork-only behaviour. Keep it
  that way.
- **Shutdown races** (Disconnect during reconnect, ctx cancel during a write). Use one owner
  goroutine per connection, and make `Close` idempotent with `sync.Once`, the same way
  comqtt's `Client.Stop` does.
- **Scope creep** toward the full mosquitto surface (SRV, SOCKS, OCSP, PSK). Keep those
  explicitly out of scope until a real need appears.

