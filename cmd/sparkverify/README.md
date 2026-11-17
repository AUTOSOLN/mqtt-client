# sparkverify

Subscribes to Sparkplug B traffic and checks it with the
[`sparkplug/verify`](../../sparkplug/verify) package, per edge node:

- **seq**: goes up by one, modulo 256, from NBIRTH through every NDATA,
  DBIRTH, DDATA and DDEATH. A jump forward is a **gap** (its skipped seqs are
  **lost** until they arrive); a skipped seq that arrives later is a
  **reorder**; a seq seen again is a **duplicate**.
- **NBIRTH** has seq 0 (`-any-birth-seq` relaxes this, as Sparkplug 3.0 does)
  and a `bdSeq` metric.
- **NDEATH** carries the bdSeq of the NBIRTH it ends; one with another bdSeq
  while a newer session is online is **stale**.
- No NDATA before NBIRTH or after NDEATH, and no DDATA for a device before its
  DBIRTH.

It prints each violation as it is found, and a summary per edge node at the
end. Start it before the edge nodes connect, or their first messages count as
DATA before BIRTH.

## Usage

```sh
sparkverify [connection flags] [-t filter ...] [-q 1] [-W 10m] [-any-birth-seq] [-report file|-]
```

The connection flags (`-address`, `-v`, `-id`, `-u`/`-P`, TLS, `-config`,
`-replay`, `-replay-conn`) are the same as `mqttsub`'s. `-t` defaults to
`spBv1.0/#`. `-report` writes the `verify.Report` as JSON.

| Flag | Default | Meaning |
|---|---|---|
| `-t` | `spBv1.0/#` | Topic filter to subscribe to (repeatable) |
| `-q` | `1` | Requested QoS: 0, 1 or 2 |
| `-W` | `0` | Stop after this long (0: until Ctrl-C, or until a `-replay` capture has been played) |
| `-any-birth-seq` | `false` | Accept an NBIRTH seq other than 0, as Sparkplug 3.0 does |
| `-report` | empty | Write the `verify.Report` as JSON to this file on exit; `-` writes it to stdout |
| `-replay-inbound` | `false` | With `-replay`, the capture is of the edge nodes themselves: check their messages as a broker would deliver them, including the NDEATH Will of a node whose connection dropped |

Exit status: `0` no violations, `1` violations (or the client diverged from a
`-replay` capture), `2` a setup error.

## Checking a capture

To check the Sparkplug traffic in a packet capture a user sent, without a
broker:

```sh
sparkverify -replay host.pcap -id <the captured client ID>    # captured at a host application
sparkverify -replay edge.pcap -replay-inbound                 # captured at an edge node
```

See the [mqttpub README](../mqttpub/README.md#replaying-a-capture--replay)
for how replay works and how to ask a user for a capture, and
[mqttpcap](../mqttpcap/README.md) to see what a capture holds.

To generate matching traffic from Go, use `sparkplug.Edge`, which emits
NBIRTH, NDATA, DBIRTH, DDATA, DDEATH and the NDEATH Will with correct seq and
bdSeq.
