#!/bin/sh
# Records the real capture fixtures that capture/fixtures_test.go reads.
# See README.md in this directory.
#
# Needs: Go, a mosquitto broker binary, tcpdump, gzip, and sudo for
# tcpdump. Run it as yourself from any directory; it asks for your sudo
# password once. It uses only localhost and test data.
set -eu

cd "$(dirname "$0")"
repo=$(cd ../.. && pwd)
port=18830
tmp=$(mktemp -d)
pids=""
cleanup() {
	for p in $pids; do
		sudo kill -INT "$p" 2>/dev/null || kill "$p" 2>/dev/null || true
	done
	rm -rf "$tmp"
}
trap cleanup EXIT

echo "building mqttpub and mqttsub"
(cd "$repo" && go build -o "$tmp/mqttpub" ./cmd/mqttpub && go build -o "$tmp/mqttsub" ./cmd/mqttsub)

mosquitto -p "$port" >"$tmp/mosquitto.log" 2>&1 &
pids="$pids $!"

sudo -v
filter="tcp port $port"
sudo tcpdump -i any -s 0 -U -w any.pcap "$filter" 2>"$tmp/any.log" &
pids="$pids $!"
sudo tcpdump -i any -y LINUX_SLL2 -s 0 -U -w any-sll2.pcap "$filter" 2>"$tmp/sll2.log" &
pids="$pids $!"
sudo tcpdump -i lo -s 0 -U --time-stamp-precision=nano -w lo-nano.pcap "$filter" 2>"$tmp/lo.log" &
pids="$pids $!"
sleep 2

# The exchange the test expects: fixture-sub receives three QoS 1
# messages "hello" on fixture/a, published by fixture-pub.
"$tmp/mqttsub" -address "localhost:$port" -id fixture-sub -t 'fixture/#' -q 1 -C 3 -W 10s -quiet &
sub=$!
sleep 1
"$tmp/mqttpub" -address "localhost:$port" -id fixture-pub -t fixture/a -m hello -q 1 -n 3 -quiet
wait "$sub"
sleep 2

cleanup
pids=""
trap - EXIT
sudo chown "$(id -u):$(id -g)" any.pcap any-sll2.pcap lo-nano.pcap
gzip -kf lo-nano.pcap
echo "recorded: any.pcap any-sll2.pcap lo-nano.pcap lo-nano.pcap.gz"
