# mqttpcap

Shows what a packet capture holds: the TCP connections in it, and the MQTT packets of one of them
with their headers. Use it to look at a capture a user sent us, and to pick the connections to
replay with the other tools' `-replay` and `-replay-conn` flags (see the
[mqttpub README](../mqttpub/README.md#replaying-a-capture--replay)).

It reads classic pcap and pcapng, either of them gzip-compressed, as written by tcpdump, dumpcap and
Wireshark. It reads Ethernet, Linux cooked capture (`tcpdump -i any`), raw IP and loopback captures,
over IPv4 or IPv6. TLS connections are listed but can't be decoded.

It never prints passwords. Payloads are printed only with `-payload`, because a user's capture holds
their process data.

## Build

From the repository root:

```sh
go build -o build/mqttpcap ./cmd/mqttpcap
```

## Usage

```sh
mqttpcap -list FILE
mqttpcap -conn N [-v VERSION] [-payload] FILE
```

| Flag | Default | Meaning |
|---|---|---|
| `-list` | `false` | List the TCP connections: index, addresses, bytes each way, client ID and protocol version from the CONNECT, number of packets, and any problem. |
| `-conn` | | Print the MQTT packets of connection `N` (an index from `-list`), one per line: time since the first packet, direction, type and header fields. |
| `-v` | from the CONNECT | Protocol version (`31`, `311` or `5`) for a connection whose CONNECT wasn't captured, such as a capture started in the middle of a session. |
| `-payload` | `false` | With `-conn`, also print PUBLISH and Will payloads, quoted. |

Example:

```text
$ mqttpcap -list capture.pcap
2 connections, 0 frames ignored (not IPv4 or IPv6 TCP)
0  10.0.0.1:50000 -> 10.0.0.2:1883  client 112 B, server 284 B  client_id="host1" v=4  14 packets
1  10.0.0.3:40000 -> 10.0.0.2:1883  client 301 B, server 16 B  client_id="press7" v=4  10 packets

$ mqttpcap -conn 0 capture.pcap
10.0.0.1:50000 -> 10.0.0.2:1883
+0.000000s client CONNECT v=4 client_id="host1" clean=true keepalive=60
+0.001000s server CONNACK reason=00
+0.002000s client SUBSCRIBE id=1 "spBv1.0/#":qos1
+0.003000s server SUBACK id=1 reason=01
+0.004000s server PUBLISH id=1 qos=1 retain=false dup=false topic="spBv1.0/plant1/NBIRTH/press7" bytes=71
+0.005000s client PUBACK id=1
...
```

Exit status:

- **0:** everything shown decoded without problems.
- **1:** a connection shown is encrypted, its streams end early (a gap in the capture or a truncated
  frame), or a packet is malformed. The output says which.
- **2:** a setup error: bad arguments, or a file that isn't a capture `mqttpcap` can read.
