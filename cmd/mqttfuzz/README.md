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

The engine is the [`fuzz`](../../fuzz) package. Tests import it to run
scenarios in-process (`fuzz.Load`/`fuzz.LoadFS`, then `(*Scenario).Run`) and
get a JSON-serializable `fuzz.Result` with every packet sent and received.
The example scenarios below are embedded in it as `fuzz.Scenarios`.

## Usage

```sh
mqttfuzz -f scenario.yaml [-address host:port] [-tls] [-v 31|311|5] [-u user] [-P pass] [-set name=value ...]
```

| Flag | Default | Meaning |
|---|---|---|
| `-f` | — | Scenario YAML file (required) |
| `-address` | scenario's `address:`, else `localhost:1883` | Broker address as `host:port` (IP address or hostname; the port is required) |
| `-tls` | scenario's `tls:`, else `false` | Connect with TLS. Never inferred from the port |
| `-v` | scenario's `version:`, else `311` | Protocol version: `31`, `311` or `5` |
| `-u`, `-P` | — | Username and password sent by every `connect` step that sets neither `username:` nor `password:` |
| `-set name=value` | — | Override a token (repeatable) |
| `-recv-timeout` | `2s` | Default wait for a `recv` / `expect-close` step |
| `-dial-timeout` | `10s` | Connection and TLS handshake timeout |
| `-write-timeout` | `5s` | Per-packet write timeout |
| `-cafile`, `-cert`, `-key`, `-insecure`, `-servername` | — | TLS options (same meaning as `mqttconnect`) |

Exit status: `0` all expectations held, `1` an expectation failed, `2` a setup
or I/O error (`fuzz.ErrExpectation` and `fuzz.ErrSetup` in the package). The
server closing the connection, or a write that times out because it stopped
reading, counts as broker behaviour, not as an I/O error: after a close, later
sending steps are skipped and later reads see the close.

## Scenario format

```yaml
version: 5            # 31 | 311 | 5; -v overrides
address: localhost:1883   # -address overrides
tls: false            # -tls overrides
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
| `connect` | `client-id`, `clean`, `keepalive`, `username`, `password`, `protocol-name`, `protocol-level`, `connect-flags`, `will-topic`/`will-payload`/`will-qos`/`will-retain`, `properties` |
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

On `connect`, `protocol-level` replaces only the protocol level byte (the rest
of the packet is still encoded for the run's version), and `connect-flags`
replaces the whole connect flags byte, reserved bit included. `username` and
`password` default to the `fuzz.RunOptions` credentials when the step sets
neither; `username: ""` sends none.

Control steps drive the harness:

| Type | Fields | Behaviour |
|---|---|---|
| `recv` | `timeout`, `count`, `expect`, `reason`, `reasons`, `payload-pattern`, `payload-size` | Read `count` packets (default 1); log them. `expect` may be a packet type name, `close` (same as `expect-close`), or `none` (silence). `reason` is the reason code the first packet must carry (CONNACK, PUBACK/REC/REL/COMP, DISCONNECT, AUTH); `reasons` the list a SUBACK or UNSUBACK must carry, in order. With `payload-pattern` or `payload-size`, every PUBLISH received must carry that pattern; `payload-size` 0 accepts any length. |
| `expect-close` | `timeout`, `reason`, `reason-optional` | Assert the server closes within the timeout, draining any packets it sends first. With `reason`, a DISCONNECT (or a refusing CONNACK) carrying that code must come before the close; with `reason-optional: true` as well, a bare close is also accepted. |
| `sleep` | `duration` | Pause. |
| `raw` | `hex` | Send arbitrary bytes (whitespace and `:` ignored) — for framing that no encoder would produce. |

Generated payloads: `payload-size: N` sends N bytes of `payload-pattern`
(`ascii`, the default, `alpha`, `01` or `binary`; the same patterns as
`mqttpub -pattern`), in place of `payload`.

Every step also accepts `label` (shown in the log and in the result), `delay`
(pause before the step), and `flags` (0–15: replaces the fixed-header low
nibble after encoding, for reserved-bit and QoS-3 probes). Packet and `raw`
steps accept `repeat: N` to send N copies; a nonzero `packet-id` goes up by one
each time.

Reason codes are numbers, so hex works: `reason: 0x8D`, `reasons: [0x00, 0x8F]`.
For bytes that are not valid UTF-8 (a topic or client ID probe), use YAML's
`!!binary` tag with base64: `topic: !!binary ZTUv/w==` is `e5/` and `0xFF`. MQTT 5 `properties` keys: `session-expiry`,
`receive-maximum`, `maximum-packet-size`, `topic-alias-maximum`, `topic-alias`,
`payload-format`, `message-expiry`, `content-type`, `response-topic`,
`subscription-identifier`, `reason-string`, `auth-method`, `auth-data`, and
`user` (a list of `{key, val}`).

## Example scenarios

[`fuzz/scenarios/`](../../fuzz/scenarios) holds ready-to-run cases:

| File | Probes |
|---|---|
| `unsubscribe-without-subscribe.yaml` | UNSUBSCRIBE for a never-subscribed filter; expects UNSUBACK (allowed) |
| `disconnect-before-connect.yaml` | DISCONNECT as the first packet; expects close (violation) |
| `publish-before-connect.yaml` | PUBLISH before CONNECT; expects close (violation) |
| `double-connect.yaml` | Second CONNECT on one connection; expects close (violation) |
| `malformed-reserved-bits.yaml` | PUBLISH with QoS 3; expects close / DISCONNECT 0x81 |
| `large-publish.yaml` | Sparkplug-shaped 3.1.1 QoS 0 PUBLISH of `size` bytes (default 256 KiB of `binary`), received back and checked byte for byte; fails on a drop, a close or a damaged payload |

```sh
# Against localhost:1883, with each scenario's own version
build/mqttfuzz -f fuzz/scenarios/unsubscribe-without-subscribe.yaml
build/mqttfuzz -f fuzz/scenarios/disconnect-before-connect.yaml
build/mqttfuzz -f fuzz/scenarios/publish-before-connect.yaml
build/mqttfuzz -f fuzz/scenarios/malformed-reserved-bits.yaml

# Pick the client ID; run the 3.1.1 case as MQTT 5 instead
build/mqttfuzz -f fuzz/scenarios/double-connect.yaml -set id=probe-$$
build/mqttfuzz -f fuzz/scenarios/publish-before-connect.yaml -v 5 -set topic=fuzz/early/v5

# A remote broker that requires a login, over TLS
build/mqttfuzz -f fuzz/scenarios/unsubscribe-without-subscribe.yaml \
  -address broker.example.com:8883 -tls -cafile ca.pem -u fuzzer -P secret

# 1 MiB of printable ASCII on a custom Sparkplug group and node
build/mqttfuzz -f fuzz/scenarios/large-publish.yaml \
  -set size=1048576 -set pattern=ascii -set group=plant1 -set node=edge-07
```

To find a broker's size limit, sweep `size` in `large-publish.yaml` (its header
has a loop).

To run every example and list which failed:

```sh
for f in fuzz/scenarios/*.yaml; do
  build/mqttfuzz -f "$f" -address localhost:1883 >/dev/null 2>&1 \
    && echo "ok    $f" || echo "FAIL  $f (exit $?)"
done
```

### Spec-level library

[`fuzz/scenarios/spec/`](../../fuzz/scenarios/spec) holds one scenario per
MQTT rule a broker must enforce. Each says in its header what the spec
requires and where the server may choose (close with or without a reason
code). Check the reason code in the result, or set `reason` on the close step,
for the behaviour you want to pin.

| File | Probes |
|---|---|
| `bad-credentials.yaml` | Refused user name/password: CONNACK 0x86 (`refusal` var; 0x04 on 3.1.1) |
| `bad-protocol-name.yaml`, `bad-protocol-level.yaml` | `MQIsdp` at the run's level; level 6 |
| `keepalive-timeout.yaml` | Silent client closed after 1.5 × `keepalive`; MQTT 5 DISCONNECT 0x8D |
| `wildcard-publish-topic.yaml` | `+` or `#` in a PUBLISH topic (`topic` var) |
| `empty-topic-no-alias.yaml` | MQTT 5 PUBLISH with an empty topic and no alias |
| `invalid-utf8-topic.yaml`, `null-char-topic.yaml`, `invalid-utf8-client-id.yaml`, `invalid-utf8-username.yaml` | Ill-formed UTF-8 and U+0000 |
| `remaining-length-overflow.yaml` | A five-byte Remaining Length |
| `subscribe-no-filters.yaml` | SUBSCRIBE with no topic filters |
| `reserved-bits-subscribe.yaml`, `-unsubscribe`, `-pubrel`, `-connect-header`, `-connect-flags` | Wrong fixed-header flags; the reserved CONNECT flag |
| `duplicate-property.yaml`, `unknown-property.yaml` | MQTT 5 Content Type twice; property ID 0x7F |
| `oversize-packet.yaml` | PUBLISH over the server's Maximum Packet Size (`size` var): DISCONNECT 0x95 |

All default to MQTT 5. Every one except `duplicate-property.yaml`,
`empty-topic-no-alias.yaml` and `unknown-property.yaml` (MQTT 5 only) can also
run with `-v 311`.

```sh
# Run one probe as MQTT 5, then as 3.1.1
build/mqttfuzz -f fuzz/scenarios/spec/remaining-length-overflow.yaml
build/mqttfuzz -f fuzz/scenarios/spec/remaining-length-overflow.yaml -v 311

# Bad credentials: the scenario sets username and password itself, so use
# -set rather than -u/-P; 3.1.1 refuses with 0x04 instead of 0x86
build/mqttfuzz -f fuzz/scenarios/spec/bad-credentials.yaml -set username=alice
build/mqttfuzz -f fuzz/scenarios/spec/bad-credentials.yaml -v 311 -set refusal=0x04
# A broker that answers 0x87 Not authorized instead
build/mqttfuzz -f fuzz/scenarios/spec/bad-credentials.yaml -set refusal=0x87

# Keepalive: a 10 s keepalive must close by 15 s (the close step waits 20 s,
# so keep keepalive at 13 or less)
build/mqttfuzz -f fuzz/scenarios/spec/keepalive-timeout.yaml -set keepalive=10

# Wildcards: '+' (the default) and '#'
build/mqttfuzz -f fuzz/scenarios/spec/wildcard-publish-topic.yaml
build/mqttfuzz -f fuzz/scenarios/spec/wildcard-publish-topic.yaml -set 'topic=fuzz/#'

# Oversize: a broker with a 1 MiB Maximum Packet Size
build/mqttfuzz -f fuzz/scenarios/spec/oversize-packet.yaml -set size=1048577

# A broker that requires a login for the probes that get past CONNECT
build/mqttfuzz -f fuzz/scenarios/spec/reserved-bits-subscribe.yaml -u fuzzer -P secret
```

To run the whole library, at both versions where it applies:

```sh
for f in fuzz/scenarios/spec/*.yaml; do
  for v in 5 311; do
    case "$v:$(basename "$f")" in
      311:duplicate-property.yaml|311:empty-topic-no-alias.yaml|311:unknown-property.yaml) continue ;;
    esac
    case "$v:$(basename "$f")" in
      311:bad-credentials.yaml) extra="-set refusal=0x04" ;;
      *) extra="" ;;
    esac
    build/mqttfuzz -f "$f" -v "$v" $extra >/dev/null 2>&1 \
      && echo "ok    v$v $f" || echo "FAIL  v$v $f (exit $?)"
  done
done
```

## Reading the output

Each step prints what it sent (type, byte count, a hex preview) or received
(type, flags, packet id, reason codes, a hex preview). A line beginning `OK`
is a met expectation; `FAIL` is a missed one (and sets exit 1); `ABORT` is an
I/O or encoding error (exit 2). A broker that correctly rejects a violation
shows a close — a TCP reset, an EOF, or an MQTT 5 DISCONNECT with a reason code
such as `0x81` (Malformed Packet) or `0x82` (Protocol Error) — before it hangs
up.
