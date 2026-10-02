# mqttconnect

A manual test tool for `mqttclient`. It runs a chosen combination of steps against a real broker:

| Step | What it does |
|---|---|
| `tcp` | Opens a TCP connection, plus a TLS handshake for `mqtts://`, then closes it. This step uses Go's standard `net` and `crypto/tls` packages directly, not the client, because the client has no dial-only call. |
| `connect` | Dials, sends CONNECT and waits for CONNACK (`Client.Connect`). |
| `disconnect` | Sends DISCONNECT and waits for the connection to close (`Client.Disconnect`). |

Valid `-steps` values are `tcp`, `connect`, and `connect,disconnect` (the default).

With `connect` alone, the tool exits without sending DISCONNECT. The broker sees the connection
drop and publishes the Will, if one was set.

## Build

From the repository root:

```sh
go build -o build/mqttconnect ./cmd/mqttconnect
```

`build/` is git-ignored. You can also run the tool without building it:
`go run ./cmd/mqttconnect [flags]`.

## Flags

| Flag | Default | Meaning |
|---|---|---|
| `-server` | `mqtt://localhost:1883` | Broker URL: `mqtt://` or `tcp://` (default port 1883), `mqtts://`, `ssl://` or `tls://` (default port 8883) |
| `-steps` | `connect,disconnect` | `tcp`, `connect` or `connect,disconnect` |
| `-v` | `311` | Protocol version: `31`, `311` or `5` |
| `-id` | empty | Client id. An empty id needs `-clean`. MQTT 3.1 then gets a random `mosq-…` id; MQTT 5 uses the id the broker assigns. |
| `-clean` | `true` | Clean session (3.x) or clean start (5) |
| `-session-expiry` | `0` | Session Expiry Interval in seconds (needs `-v 5`) |
| `-keepalive` | `60` | Seconds; 0 or at least 5 |
| `-u`, `-P` | empty | Username and password |
| `-hold` | `0` | How long to stay connected after CONNACK. Ctrl-C ends it early. |
| `-reason` | `0` | DISCONNECT reason code (needs `-v 5`) |
| `-repeat` | `1` | Run the steps this many times |
| `-timeout` | `10s` | Timeout for each step |
| `-debug` | `false` | Print the client's debug log to stderr |
| `-cafile` | empty | PEM file of CA certificates to trust |
| `-cert`, `-key` | empty | Client certificate and private key (PEM) |
| `-insecure` | `false` | Skip server certificate verification |
| `-servername` | URL host | TLS server name |
| `-will-topic` | empty | Sets a Will on this topic |
| `-will-payload`, `-will-qos`, `-will-retain` | empty, 0, false | The rest of the Will |

Exit status: 0 on success, 1 when a step fails, 2 for invalid arguments.

## How to test

These examples assume a mosquitto broker listening on `localhost:1883`. Run them from the
repository root.

### Reachability

```sh
build/mqttconnect -steps tcp
build/mqttconnect -steps tcp -server mqtts://broker.example:8883 -cafile ca.pem   # also prints the TLS version and server cert
```

### Connect and disconnect for each protocol version

```sh
build/mqttconnect -v 31
build/mqttconnect -v 311 -id t311
build/mqttconnect -v 5 -id t5          # prints the CONNACK properties the broker sent
build/mqttconnect -v 5                 # no -id: the broker assigns one
```

### Credentials

```sh
build/mqttconnect -v 5 -u user -P secret
```

If the broker rejects the credentials, the step fails with an error such as
`connection refused: bad user name or password (0x86)`. MQTT 3.x reports return code `0x04`
instead.

### Keepalive

```sh
build/mqttconnect -v 5 -keepalive 5 -hold 20s -debug
```

The connection should stay up for the whole hold. If PINGREQ were not sent, the broker would
drop the client after 1.5 × keepalive. The tool would then print `OnDisconnect` and
`FAIL: connection lost while holding`.

### Will

In one terminal, watch the Will topics:

```sh
mosquitto_sub -h localhost -t 't/will/#' -v
```

In another terminal:

```sh
build/mqttconnect -steps connect -will-topic t/will/a -will-payload dropped          # Will published
build/mqttconnect -v 5 -reason 4 -will-topic t/will/b -will-payload reason4          # Will published (0x04 = disconnect with Will)
build/mqttconnect -will-topic t/will/c -will-payload clean                           # no Will: normal DISCONNECT
```

### TLS

```sh
build/mqttconnect -server mqtts://broker.example:8883 -cafile ca.pem
build/mqttconnect -server mqtts://localhost:8883 -cafile ca.pem -servername broker.example
build/mqttconnect -server mqtts://localhost:8883 -cert client.pem -key client.key -cafile ca.pem   # mutual TLS
build/mqttconnect -server mqtts://localhost:8883 -insecure                                         # self-signed, no verification
```

To test against a local broker, add a TLS listener to `mosquitto.conf`:

```
listener 8883
cafile   /path/ca.pem
certfile /path/server.pem
keyfile  /path/server.key
# require_certificate true      # for mutual TLS
```

### Repeated connections

```sh
build/mqttconnect -repeat 100 -id loop
```

### Failure cases

```sh
build/mqttconnect -server mqtt://localhost:1999 -timeout 2s               # connection refused
build/mqttconnect -server mqtts://localhost:1883 -steps tcp -insecure     # TLS against a plain port
```

## Reading the output

Lines starting with `OnConnect` and `OnDisconnect` come from the client's callbacks. Callbacks run
on the client's own goroutine, so an `OnConnect` line can appear just before or just after the
`connected in …` line. On a clean disconnect, the tool waits for `OnDisconnect` before it exits.
