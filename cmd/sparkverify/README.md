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

The connection flags (`-address`, `-v`, `-id`, `-u`/`-P`, TLS, `-config`) are
the same as `mqttsub`'s. `-t` defaults to `spBv1.0/#`. `-report` writes the
`verify.Report` as JSON.

Exit status: `0` no violations, `1` violations, `2` a setup error.

To generate matching traffic from Go, use `sparkplug.Edge`, which emits
NBIRTH, NDATA, DBIRTH, DDATA, DDEATH and the NDEATH Will with correct seq and
bdSeq.
