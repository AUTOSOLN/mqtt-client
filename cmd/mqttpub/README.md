# mqttpub

A manual test tool for publishing with `mqttclient` (phase 2). It connects, publishes `-n` messages
at QoS 0, 1 or 2, waits until each one is complete, and then disconnects. With `-listen` it stays
connected and prints any messages the broker sends.

The client cannot subscribe yet (that comes in phase 3), but it can still receive messages: when
it resumes a persistent session that already has subscriptions, the broker delivers the session's
queued and new messages. [Receiving](#receiving-messages) below shows how to set that up.

It uses the same connection flags as [mqttconnect](../mqttconnect/README.md). Both tools get them
from [cmd/internal/cli](../internal/cli/cli.go).

## Build

From the repository root:

```sh
go build -o build/mqttpub ./cmd/mqttpub
```

`build/` is git-ignored. You can also run the tool without building it:
`go run ./cmd/mqttpub [flags]`.

## Flags

Connection flags (shared with `mqttconnect`):

| Flag | Default | Meaning |
|---|---|---|
| `-server` | `mqtt://localhost:1883` | Broker URL: `mqtt://` or `tcp://` (default port 1883), `mqtts://`, `ssl://` or `tls://` (default port 8883) |
| `-v` | `311` | Protocol version: `31`, `311` or `5` |
| `-id` | empty | Client id. An empty id needs `-clean`. |
| `-clean` | `true` | Clean session (3.x) or clean start (5). Use `-clean=false` to resume a session. |
| `-session-expiry` | `0` | Session Expiry Interval in seconds (needs `-v 5`) |
| `-keepalive` | `60` | Seconds; 0 or at least 5 |
| `-u`, `-P` | empty | Username and password |
| `-timeout` | `10s` | Timeout for connecting and disconnecting. Waiting for acknowledgements gives up after this long without progress. |
| `-debug` | `false` | Print the client's debug log to stderr |
| `-cafile`, `-cert`, `-key`, `-insecure`, `-servername` | | TLS, as in `mqttconnect` |
| `-will-topic`, `-will-payload`, `-will-qos`, `-will-retain` | | Will, as in `mqttconnect` |

Publish flags:

| Flag | Default | Meaning |
|---|---|---|
| `-t` | `mqttclient/test` | Topic |
| `-m` | `hello {n}` | Payload. `{n}` is replaced by the message number, counting from 1. |
| `-size` | `0` | Send a generated payload of this many bytes instead of `-m` |
| `-q` | `0` | QoS: 0, 1 or 2 |
| `-r` | `false` | Retain flag |
| `-n` | `1` | Number of messages. Use 0 to publish nothing, for example with `-listen`. |
| `-interval` | `0` | Pause between messages |
| `-sync` | `false` | Wait for each message to complete before sending the next. By default, messages are pipelined up to the broker's Receive Maximum. |
| `-listen` | `0` | After publishing, stay connected this long and print received messages. Ctrl-C ends it early. |
| `-reason` | `0` | DISCONNECT reason code (needs `-v 5`) |
| `-quiet` | `false` | Print only failures and the summary |
| `-content-type` | empty | Content Type property (needs `-v 5`) |
| `-utf8` | `false` | Payload Format Indicator = 1 (needs `-v 5`) |
| `-response-topic` | empty | Response Topic property (needs `-v 5`) |
| `-correlation` | empty | Correlation Data property (needs `-v 5`) |
| `-expiry` | `0` | Message Expiry Interval in seconds (needs `-v 5`) |
| `-user` | | User property `key=value` (needs `-v 5`). Repeat the flag to add more than one. |

Exit status: 0 on success, 1 if any step fails or the broker refuses a message (MQTT 5 reason
code 0x80 or above), 2 for invalid arguments.

## Reading the output

| Line | Meaning |
|---|---|
| `Publish mid=…` | `Client.Publish` accepted the message. QoS 0 messages are already written; QoS 1 and 2 messages may still be queued behind the send quota. |
| `OnPublish mid=… reason=…` | The message is complete: QoS 0 written, QoS 1 PUBACK, QoS 2 PUBCOMP, or a failed PUBREC. |
| `OnMessage mid=… qos=… topic=… payload=…` | A message from the broker, followed by any MQTT 5 properties. QoS 1 messages are printed after PUBACK is sent, QoS 2 after PUBREL arrives. |
| `completed N of M in … (… msg/s), K refused` | Summary, counted from the `Pending` results |

Callbacks run on the client's own goroutine, so the lines from `OnConnect`, `OnPublish` and
`OnMessage` can appear interleaved with the tool's own lines.

## How to test

The examples assume a mosquitto broker on `localhost:1883` and are run from the repository root.
To see what arrives at the broker, run this in a second terminal:

```sh
mosquitto_sub -h localhost -V 5 -q 2 -t 'test/#' -v -F '%t %p qos=%q props=%P'
```

### Publish at each QoS level

```sh
build/mqttpub -t test/a -m "q0 {n}" -n 2
build/mqttpub -t test/a -m "q1 {n}" -q 1 -n 2
build/mqttpub -v 5 -t test/a -m "q2 {n}" -q 2 -n 2
build/mqttpub -v 31 -t test/a -q 1
build/mqttpub -t test/a -r -m "retained"           # retained; a new subscriber gets it
build/mqttpub -t test/a -r -m "" -n 1              # clears the retained message
```

### MQTT 5 properties

```sh
build/mqttpub -v 5 -t test/a -q 1 -content-type text/plain -utf8 -response-topic test/reply \
    -correlation abc -expiry 60 -user k=v -user k2=v2
```

### Throughput and the send quota

```sh
build/mqttpub -v 5 -t test/a -q 1 -n 10000 -quiet
build/mqttpub -t test/a -q 2 -n 10000 -quiet
build/mqttpub -t test/a -q 1 -n 100 -sync -quiet    # one message at a time
build/mqttpub -t test/a -q 1 -n 2000 -size 100000 -quiet
```

The client keeps at most the broker's Receive Maximum (`-v 5`), or 20 for 3.x, QoS 1 and 2
messages in flight, and queues the rest. With `-debug`, you can see each PUBLISH leave as an
acknowledgement frees a slot.

A run with only a few messages may show about 40 ms per acknowledgement. That delay is Nagle's
algorithm on the broker side: mosquitto's `set_tcp_nodelay` option is off by default. It does
not show up with larger `-n` values.

### Receiving messages

A persistent session with a subscription lets the client receive messages without subscribing
itself.

1. Create the session. Ctrl-C (or the `timeout`) leaves the session on the broker:

   ```sh
   timeout 1 mosquitto_sub -h localhost -c -i inbox -q 2 -t 'test/in/#'
   ```

2. While `inbox` is offline, publish to it from another client id. The broker queues the messages
   for the session:

   ```sh
   build/mqttpub -id sender -t test/in/queued -q 2 -n 3 -m "queued {n}"
   ```

3. Resume the session with this client. The queued messages arrive at QoS 2 (PUBREC, PUBREL,
   PUBCOMP, then `OnMessage`):

   ```sh
   build/mqttpub -id inbox -clean=false -n 0 -listen 1s
   ```

4. Loopback: the session's subscription also matches its own publications, so this tests both
   directions in one run:

   ```sh
   build/mqttpub -id inbox -clean=false -t test/in/self -q 1 -n 2 -listen 1s
   ```

5. MQTT 5, with properties on the received message. The session needs an expiry interval to
   outlive the connection:

   ```sh
   timeout 1 mosquitto_sub -h localhost -V 5 -c -x 300 -i inbox5 -q 2 -t 'test/in5/#'
   mosquitto_pub -h localhost -V 5 -q 2 -t test/in5/p -m '{"a":1}' \
       -D publish content-type application/json -D publish user-property k v
   build/mqttpub -v 5 -id inbox5 -clean=false -session-expiry 300 -n 0 -listen 1s
   ```

6. Remove the sessions by connecting once with a clean session:

   ```sh
   build/mqttconnect -id inbox
   build/mqttconnect -v 5 -id inbox5
   ```

### Resending after a lost connection

`mqttpub` does not reconnect, but the client keeps unacknowledged QoS 1 and 2 messages across
connections. On the next successful `Connect`, it resends them with DUP set, or resends PUBREL if
PUBREC had already arrived. The unit test `TestResendAfterReconnect` and the conformance cases
`03-publish-c2b-qos1-disconnect` and `03-publish-c2b-qos2-disconnect` cover this behaviour.
Automatic reconnect comes in phase 4.

### Failure cases

```sh
build/mqttpub -t 'test/#'                          # wildcard topic: refused before sending
build/mqttpub -t test/a -content-type x            # properties without -v 5: exit 2
build/mqttpub -v 5 -t test/a -q 1 -reason 4 -will-topic test/will   # disconnect with Will
```

To see a refusal from the broker (MQTT 5 reason code 0x87, not authorized), use a mosquitto ACL
that denies the topic and publish with `-v 5 -q 1`. The tool prints the reason code and exits
with 1.
