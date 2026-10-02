# mqtt-client

An MQTT 3.1.1 / 5.0 client for Go, modelled on the
[libmosquitto](https://github.com/eclipse-mosquitto/mosquitto) client API and
built on the [comqtt](https://github.com/AUTOSOLN/comqtt) packet codec.

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
	OnMessage: func(_ *mqttclient.Client, m *mqttclient.Message) {
		log.Printf("%s: %s", m.Topic, m.Payload)
	},
	OnConnect: func(c *mqttclient.Client, ack mqttclient.ConnAck) {
		// Subscriptions are not resent on a new connection: subscribe here.
		_, _ = c.Subscribe(context.Background(), []mqttclient.Subscription{{Topic: "a/#", QoS: 1}}, nil)
	},
})
if err != nil {
	log.Fatal(err)
}
// Either keep the connection up, reconnecting as needed (blocks until ctx
// ends or Disconnect):
//     go func() { log.Println("run:", c.Run(ctx)) }()
// or connect once:
if _, err := c.Connect(ctx); err != nil {
	log.Fatal(err)
}
defer c.Disconnect(ctx, 0, nil)

p, err := c.Publish(ctx, &mqttclient.Message{Topic: "a/b", Payload: []byte("hi"), QoS: 1})
if err != nil {
	log.Fatal(err)
}
if _, err := p.Wait(ctx); err != nil { // PUBACK received, or refused
	log.Fatal(err)
}
```

## Testing

```
go test -race ./...
(cd integration && go test -race ./...)   # starts its own mosquitto; skips without one
./conformance/run.sh      # libmosquitto's client test suite; needs python3 and
                          # a mosquitto checkout (MOSQUITTO_SRC, default ../mosquitto)
```

For manual tests against a real broker, see [cmd/mqttconnect](cmd/mqttconnect)
(connect and disconnect), [cmd/mqttpub](cmd/mqttpub) (publish),
[cmd/mqttsub](cmd/mqttsub) (subscribe and unsubscribe) and
[cmd/interactiveclient](cmd/interactiveclient) (a long-running app on `Run`
that reconnects by itself).

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
