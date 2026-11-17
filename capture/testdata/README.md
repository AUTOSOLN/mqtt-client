# Capture fixtures

Real capture files that `capture/fixtures_test.go` reads, so that a misunderstanding of a file format
shared by `capture` and `capturetest` can't hide behind tests that only read files `capturetest`
wrote. Each holds one short exchange on localhost, with test data only:

- `fixture-sub` subscribes to `fixture/#` at QoS 1.
- `fixture-pub` publishes `hello` to `fixture/a` three times at QoS 1.
- `fixture-sub` receives the three messages.

| File | Recorded with | Covers |
| --- | --- | --- |
| `any.pcap` | `tcpdump -i any -y LINUX_SLL` | Linux cooked capture v1, as libpcap before 1.10 writes it |
| `any-sll2.pcap` | `tcpdump -i any -y LINUX_SLL2` | Linux cooked capture v2, the default for `-i any` with libpcap 1.10 and later |
| `lo-nano.pcap` | `tcpdump -i lo --time-stamp-precision=nano` | Ethernet framing on loopback, nanosecond pcap |
| `lo-nano.pcap.gz` | `gzip` of `lo-nano.pcap` | gzip-compressed captures, as Wireshark saves them |
| `dumpcap-2if.pcapng` | `dumpcap -i lo -i any` | pcapng with two interfaces of different link types |
| `windows-npcap.pcapng` | Wireshark on Windows, Npcap loopback adapter | Null/loopback link type in pcapng |

## Recording

The first four come from `record.sh`. It needs Go, a `mosquitto` broker binary, `tcpdump` and
`gzip`, and asks for your sudo password once for `tcpdump`. It finds mosquitto through
`$MOSQUITTO`, then `PATH`, then `/usr/sbin` and `/usr/local/sbin`. Run it from a terminal:

```sh
capture/testdata/record.sh
```

The last two need tools `record.sh` doesn't use:

- `dumpcap-2if.pcapng`: start `mosquitto -p 18830`, run
  `dumpcap -i lo -i any -f 'tcp port 18830' -w capture/testdata/dumpcap-2if.pcapng`, run the
  exchange above with `mqttsub` and `mqttpub` (see the commands in `record.sh`), then stop dumpcap.
- `windows-npcap.pcapng`: on Windows, start mosquitto on port 18830, capture on the Npcap loopback
  adapter in Wireshark with the capture filter `tcp port 18830`, run the exchange, and save as
  pcapng.

Until a file exists, its test is skipped with these instructions. Tests built with `capturetest`
cover the same link types and blocks in the meantime.

Commit only captures of this exchange. Never commit a capture from a user's site: it may hold their
passwords, topics and process data.
