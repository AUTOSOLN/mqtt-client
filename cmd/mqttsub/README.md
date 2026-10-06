# mqttsub

A manual test tool for subscribing with `mqttclient` (phase 3). The tool:

1. Connects.
2. Optionally unsubscribes from some filters first (`-U`).
3. Subscribes to the `-t` topic filters and prints the broker's answer for each filter.
4. Prints received messages until `-C` messages have arrived, `-W` has passed, or you press
   Ctrl-C.
5. Optionally unsubscribes (`-unsub`), then disconnects.

It uses the same connection flags as [mqttconnect](../mqttconnect/README.md) and
[mqttpub](../mqttpub/README.md). All three tools get them from
[cmd/internal/cli](../internal/cli/cli.go). Use `mqttpub` to send the messages.

## Build

From the repository root:

```sh
go build -o build/mqttsub ./cmd/mqttsub
```

`build/` is git-ignored. You can also run the tool without building it:
`go run ./cmd/mqttsub [flags]`.

## Flags

The connection flags are listed in the [mqttpub README](../mqttpub/README.md#flags). They are
`-server`, `-v`, `-id`, `-clean`, `-session-expiry`, `-keepalive`, `-u`, `-P`, `-timeout`,
`-debug`, the TLS flags and the Will flags. `-timeout` also limits how long the tool waits for
SUBACK and UNSUBACK.

| Flag | Default | Meaning |
|---|---|---|
| `-t` | | Topic filter to subscribe to. Repeat the flag for more than one; all go in one SUBSCRIBE. |
| `-U` | | Topic filter to unsubscribe from right after connecting, before subscribing. Repeatable. Useful with `-clean=false` to remove a subscription from a stored session. |
| `-q` | `0` | Requested QoS for every filter |
| `-no-local` | `false` | No Local: don't receive this client's own messages (needs `-v 5`) |
| `-retain-as-published` | `false` | Retain As Published: keep the retain flag on forwarded messages (needs `-v 5`) |
| `-retain-handling` | `0` | 0 send retained messages, 1 only for a new subscription, 2 never (needs `-v 5`) |
| `-sub-id` | `0` | Subscription Identifier; the broker attaches it to matching messages (needs `-v 5`) |
| `-user` | | SUBSCRIBE user property `key=value` (needs `-v 5`). Repeatable. |
| `-unsub` | `false` | Unsubscribe from the `-t` filters before disconnecting |
| `-C` | `0` | Exit after this many messages |
| `-W` | `0` | Exit after this long. With neither `-C` nor `-W`, run until Ctrl-C. |
| `-reason` | `0` | DISCONNECT reason code (needs `-v 5`) |
| `-quiet` | `false` | Don't print each message |
| `-verify` | | Check every payload is this `mqttpub -pattern` (`alpha`, `ascii`, `01`, `binary`) and print its size, even with `-quiet`. A payload that doesn't match fails the run, with the offset of the first bad byte. |
| `-verify-size` | | With `-verify`, also require payloads of exactly this many bytes |

Exit status:
- **0:** success.
- **1:** a step failed, every filter was refused, or `-W` ran out before `-C` messages arrived.
- **2:** invalid arguments.

## Reading the output

```
SUBACK mid=1
  "test/s5/+": granted QoS 2
  "test/other": refused 0x87 (not authorized)
OnMessage mid=1 qos=1 retain=false topic="test/s5/x" payload="hello 1" content_type="text/plain" subscription_ids=[7] user["a"]="b"
UNSUBACK mid=2
  "test/s5/+": 0x00 (success)
```

- **SUBACK and UNSUBACK** list one line per filter, in request order. The granted QoS can be
  lower than `-q` requested. If only some filters are refused, the tool continues; if all are,
  it fails.
- **`OnMessage` lines** show the message, then any MQTT 5 properties. `subscription_ids` comes
  from `-sub-id`. `retain=true` marks a message from the retained store. `mid` is 0 for every
  QoS 0 message.
- **MQTT 3.x UNSUBACK** carries no reason codes, so every filter shows `0x00`.

## How to test

These examples assume a mosquitto broker on `localhost:1883` and are run from the repository
root. Start `mqttsub` in one terminal and publish from another.

### Subscribe and receive at each QoS

```sh
build/mqttsub -t 'test/s/#' -q 2 -C 3 -W 10s
build/mqttpub -t test/s/a -q 2 -n 3                 # in the other terminal
```

The QoS of each delivered message is the lower of the published QoS and the granted QoS. Repeat
with `-q 0` and `-q 1`, and with `-v 31`, `-v 311` and `-v 5`.

### Several filters, MQTT 5 properties, and unsubscribe

```sh
build/mqttsub -v 5 -t 'test/s5/+' -t test/other -q 2 -sub-id 7 -user k=v -C 1 -W 10s -unsub
build/mqttpub -v 5 -t test/s5/x -q 1 -content-type text/plain -user a=b
```

The message shows `subscription_ids=[7]`, and the UNSUBACK lists both filters.

### Retained messages and Retain Handling

```sh
build/mqttpub -t test/r/a -r -m kept
build/mqttsub -v 5 -t 'test/r/#' -W 1s -retain-handling 0    # receives "kept" with retain=true
build/mqttsub -v 5 -t 'test/r/#' -W 1s -retain-handling 2    # receives nothing
build/mqttpub -t test/r/a -r -m ""                           # clears the retained message
```

### No Local

No Local only matters for messages a client publishes itself. `mqttsub` doesn't publish, so let
`mqttpub` publish on the same session:

```sh
build/mqttsub -v 5 -id nl -clean=false -session-expiry 60 -t test/nl -q 1 -no-local -W 100ms
build/mqttpub -v 5 -id nl -clean=false -session-expiry 60 -t test/nl -q 1 -n 2 -listen 1s   # received 0 messages
build/mqttconnect -v 5 -id nl                                                               # remove the session
```

Without `-no-local`, the same `mqttpub` run receives its 2 messages.

### Large payloads

```sh
build/mqttsub -t 'spBv1.0/#' -verify binary -quiet -C 8 -W 60s
build/mqttpub -t spBv1.0/g/DBIRTH/n/d -pattern binary -size 131072 -size-step 131072 -n 8 -sync -quiet
```

Each message prints `verified message N … bytes=…` or `BAD message …`. A message that
the broker dropped doesn't show up at all, so `-C` isn't reached and the run fails with
`received X of 8 messages`. The last `verified` size is the largest the broker passed.

### Persistent sessions

```sh
build/mqttsub -id inbox -clean=false -t 'test/in/#' -q 2 -W 100ms   # create the session
build/mqttpub -id sender -t test/in/q -q 2 -n 2                     # queued while inbox is offline
build/mqttsub -id inbox -clean=false -U 'test/in/#' -W 1s           # receives the 2 queued messages,
                                                                    # then removes the subscription
build/mqttconnect -id inbox                                         # remove the session
```

### Unsubscribe reason codes (MQTT 5)

```sh
build/mqttsub -v 5 -U never/subscribed -W 1ms     # 0x11 (no subscription existed)
```

### Failure cases

```sh
build/mqttsub                                     # no filter: exit 2
build/mqttsub -t a -no-local                      # MQTT 5 option without -v 5: exit 2
build/mqttsub -t 'a/#/b'                          # invalid filter, refused before sending
build/mqttsub -t test/none -C 1 -W 1s             # times out: "received 0 of 1 messages", exit 1
```

To see a refused filter, use a mosquitto ACL that denies the topic. The SUBACK shows `0x80`
(MQTT 3.1.1) or `0x87` (MQTT 5) for that filter.

### Subscriptions after a lost connection

SUBSCRIBE is not queued or resent, as in libmosquitto. A Subscribe still waiting for SUBACK when
the connection drops fails with the connection's error. Phase 4 adds automatic reconnect, with
re-subscribing from `OnConnect`.
