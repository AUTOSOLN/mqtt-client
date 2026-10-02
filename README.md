# mqtt-client

An MQTT 3.1.1 / 5.0 client for Go, modelled on the
[libmosquitto](https://github.com/eclipse-mosquitto/mosquitto) client API and
built on the [comqtt](https://github.com/AUTOSOLN/comqtt) packet codec.

Status: pre-alpha, under construction. See [PLAN.md](PLAN.md).

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
