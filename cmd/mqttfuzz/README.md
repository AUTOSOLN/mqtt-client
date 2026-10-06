# mqttfuzz

A packet-ordering fuzzer for hardening a broker. It sends MQTT control packets
in whatever order a YAML scenario specifies, over a raw connection, with **no
client-side state machine in the way**. That means it can send orders the
protocol forbids, which the regular client would refuse to construct:

- UNSUBSCRIBE with no prior SUBSCRIBE
- DISCONNECT, PUBLISH or SUBSCRIBE before CONNECT
- two CONNECTs on one connection
- malformed frames: illegal reserved bits, QoS 3, hand-crafted bytes

Each step can also state what it expects back (a packet type, a connection
close, or silence), so a scenario doubles as a regression check: it exits
non-zero when the broker does not behave as the scenario asserts.

> This is an offensive-testing tool for **brokers you operate**. Point it only
> at your own infrastructure.

## Build

From the repository root:

```sh
go build -o build/mqttfuzz ./cmd/mqttfuzz
```

`build/` is git-ignored. You can also run it directly:
`go run ./cmd/mqttfuzz -f scenario.yaml`.

## Usage

```sh
mqttfuzz -f scenario.yaml [-server URL] [-v 31|311|5] [-set name=value ...]
```

| Flag | Default | Meaning |
|---|---|---|
| `-f` | — | Scenario YAML file (required) |
| `-server` | scenario's `server:`, else `mqtt://localhost:1883` | Broker URL: `mqtt://`, `tcp://`, `mqtts://`, `ssl://`, `tls://` |
| `-v` | scenario's `version:`, else `311` | Protocol version: `31`, `311` or `5` |
| `-set name=value` | — | Override a token (repeatable) |
| `-recv-timeout` | `2s` | Default wait for a `recv` / `expect-close` step |
| `-dial-timeout` | `10s` | Connection and TLS handshake timeout |
| `-write-timeout` | `5s` | Per-packet write timeout |
| `-cafile`, `-cert`, `-key`, `-insecure`, `-servername` | — | TLS options (same meaning as `mqttconnect`) |

Exit status: `0` all expectations held, `1` an expectation failed, `2` a setup
or I/O error.

## Scenario format

```yaml
version: 5            # 31 | 311 | 5; -v overrides
server: mqtt://localhost:1883   # -server overrides
vars:                 # token defaults; -set overrides; values may nest tokens
  id: fuzz-${rand}
  topic: test/topic
steps:
  - type: connect
    client-id: "${id}"
    clean: true
    keepalive: 30
  - type: recv
    expect: connack
  - type: unsubscribe       # no SUBSCRIBE first
    packet-id: 1
    filters: ["${topic}"]
  - type: recv
    expect: unsuback
  - type: disconnect
    reason: 0
```

### Tokens

Write `${name}` anywhere. Values come from `vars:`, overridden by `-set
name=value`. A value may itself contain tokens (`id: fuzz-${rand}`); these are
expanded before use. An undefined token is an error. Built-ins:

| Token | Value |
|---|---|
| `${rand}` | random hex suffix, unique per run |
| `${pid}` | process id |
| `${time}` | unix seconds |
| `${date}` | `YYYYMMDD-HHMMSS` |

### Step types

Packet steps encode and send one control packet:

| Type | Key fields |
|---|---|
| `connect` | `client-id`, `clean`, `keepalive`, `username`, `password`, `protocol-name`, `will-topic`/`will-payload`/`will-qos`/`will-retain`, `properties` |
| `publish` | `topic`, `payload` or `payload-size`/`payload-pattern`, `qos`, `retain`, `dup`, `packet-id`, `properties` |
| `subscribe` | `packet-id`, `filters`, `properties` |
| `unsubscribe` | `packet-id`, `filters` (filter strings) |
| `puback`/`pubrec`/`pubrel`/`pubcomp` | `packet-id`, `reason` |
| `suback`/`unsuback` | `packet-id`, `filters` (per-filter `qos` becomes the returned code), or `reason` |
| `pingreq`/`pingresp` | — |
| `disconnect` | `reason`, `properties` |
| `auth` | `reason`, `properties` (`auth-method`, `auth-data`) |

`filters` is a list of strings, or of maps with `filter`, `qos`, `nolocal`,
`rap`, `retain-handling`.

Control steps drive the harness:

| Type | Fields | Behaviour |
|---|---|---|
| `recv` | `timeout`, `count`, `expect`, `payload-pattern`, `payload-size` | Read `count` packets (default 1); log them. `expect` may be a packet type name, `close`, or `none` (silence). With `payload-pattern` or `payload-size`, every PUBLISH received must carry that pattern; `payload-size` 0 accepts any length. |
| `expect-close` | `timeout` | Assert the server closes within the timeout, draining any packets it sends first. |
| `sleep` | `duration` | Pause. |
| `raw` | `hex` | Send arbitrary bytes (whitespace and `:` ignored) — for framing that no encoder would produce. |

Generated payloads: `payload-size: N` sends N bytes of `payload-pattern`
(`ascii`, the default, `alpha`, `01` or `binary`; the same patterns as
`mqttpub -pattern`), in place of `payload`.

Every step also accepts `label` (shown in the log), `delay` (pause before the
step), and `flags` (0–15: replaces the fixed-header low nibble after encoding,
for reserved-bit and QoS-3 probes). MQTT 5 `properties` keys: `session-expiry`,
`receive-maximum`, `maximum-packet-size`, `topic-alias-maximum`, `topic-alias`,
`payload-format`, `message-expiry`, `content-type`, `response-topic`,
`subscription-identifier`, `reason-string`, `auth-method`, `auth-data`, and
`user` (a list of `{key, val}`).

## Example scenarios

The `scenarios/` directory holds ready-to-run cases:

| File | Probes |
|---|---|
| `unsubscribe-without-subscribe.yaml` | UNSUBSCRIBE for a never-subscribed filter; expects UNSUBACK (allowed) |
| `disconnect-before-connect.yaml` | DISCONNECT as the first packet; expects close (violation) |
| `publish-before-connect.yaml` | PUBLISH before CONNECT; expects close (violation) |
| `double-connect.yaml` | Second CONNECT on one connection; expects close (violation) |
| `malformed-reserved-bits.yaml` | PUBLISH with QoS 3; expects close / DISCONNECT 0x81 |
| `large-publish.yaml` | Sparkplug-shaped 3.1.1 QoS 0 PUBLISH of `size` bytes (default 256 KiB of `binary`), received back and checked byte for byte; fails on a drop, a close or a damaged payload |

```sh
build/mqttfuzz -f cmd/mqttfuzz/scenarios/unsubscribe-without-subscribe.yaml
build/mqttfuzz -f cmd/mqttfuzz/scenarios/disconnect-before-connect.yaml
build/mqttfuzz -f cmd/mqttfuzz/scenarios/double-connect.yaml -set id=probe-$$
build/mqttfuzz -f cmd/mqttfuzz/scenarios/large-publish.yaml -set size=1048576 -set pattern=ascii
```

To find a broker's size limit, sweep `size` (the scenario's header has a loop).

## Reading the output

Each step prints what it sent (type, byte count, a hex preview) or received
(type, flags, packet id, reason codes, a hex preview). A line beginning `OK`
is a met expectation; `FAIL` is a missed one (and sets exit 1); `ABORT` is an
I/O or encoding error (exit 2). A broker that correctly rejects a violation
shows a close — a TCP reset, an EOF, or an MQTT 5 DISCONNECT with a reason code
such as `0x81` (Malformed Packet) or `0x82` (Protocol Error) — before it hangs
up.
