# mqtt-client

An MQTT 3.1.1 / 5.0 client for Go, modelled on the
[libmosquitto](https://github.com/eclipse-mosquitto/mosquitto) client API and
built on the [comqtt](https://github.com/AUTOSOLN/comqtt) packet codec.

Status: pre-alpha. Connect, keepalive and disconnect work for MQTT 3.1, 3.1.1
and 5 over TCP and TLS; publish and subscribe are next. See [PLAN.md](PLAN.md).

```go
c, err := mqttclient.New(mqttclient.Options{
	Server:          "mqtt://localhost:1883",
	ProtocolVersion: mqttclient.MQTT5,
	ClientID:        "example",
	CleanStart:      true,
	KeepAlive:       60,
}, mqttclient.Handlers{
	OnDisconnect: func(_ *mqttclient.Client, ev mqttclient.DisconnectEvent) {
		log.Println("disconnected:", ev.Err)
	},
})
if err != nil {
	log.Fatal(err)
}
if _, err := c.Connect(ctx); err != nil {
	log.Fatal(err)
}
defer c.Disconnect(ctx, 0, nil)
```

## Testing

```
go test -race ./...
./conformance/run.sh      # libmosquitto's client test suite; needs python3 and
                          # a mosquitto checkout (MOSQUITTO_SRC, default ../mosquitto)
```

## Using it

```
go get github.com/AUTOSOLN/mqtt-client
```

Go applies `replace` directives only in the main module, so add this to your
own `go.mod` to build against the AUTOSOLN comqtt fork:

```
replace github.com/wind-c/comqtt/v2 => github.com/AUTOSOLN/comqtt/v2 <pseudo-version>
```

Without the `replace`, the client still builds and works against upstream
`github.com/wind-c/comqtt/v2`.

## License

BSD-3-Clause. Portions are derived from Eclipse Mosquitto (used under EDL-1.0 /
BSD-3-Clause) and comqtt / mochi-mqtt (MIT); derived files carry attribution
headers.
