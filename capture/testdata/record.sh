#!/bin/sh
# Records the real capture fixtures that capture/fixtures_test.go reads.
# See README.md in this directory.
#
# Needs: Go, a mosquitto broker binary, tcpdump, gzip, and sudo for
# tcpdump. Run it as yourself from any directory; it asks for your sudo
# password once. It uses only 127.0.0.1 and test data.
#
# mosquitto is looked for as the integration tests do: $MOSQUITTO, then
# PATH, then /usr/sbin and /usr/local/sbin (where packages install it,
# often outside a normal user's PATH).
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

broker=${MOSQUITTO:-}
if [ -z "$broker" ]; then
	broker=$(command -v mosquitto || true)
fi
for p in /usr/sbin/mosquitto /usr/local/sbin/mosquitto; do
	if [ -z "$broker" ] && [ -x "$p" ]; then
		broker=$p
	fi
done
if [ -z "$broker" ]; then
	echo "record.sh: mosquitto not found; install it or set MOSQUITTO to its path" >&2
	exit 1
fi

echo "starting $broker on 127.0.0.1:$port"
"$broker" -p "$port" >"$tmp/mosquitto.log" 2>&1 &
pids="$pids $!"
# Wait until it is listening, and show its log if it never is.
i=0
until grep -q "running" "$tmp/mosquitto.log" 2>/dev/null; do
	i=$((i + 1))
	if [ "$i" -gt 50 ]; then
		echo "record.sh: mosquitto didn't start within 5s; its log:" >&2
		cat "$tmp/mosquitto.log" >&2
		exit 1
	fi
	sleep 0.1
done

sudo -v
filter="tcp port $port"
sudo tcpdump -i any -y LINUX_SLL -s 0 -U -w any.pcap "$filter" 2>"$tmp/any.log" &
pids="$pids $!"
sudo tcpdump -i any -y LINUX_SLL2 -s 0 -U -w any-sll2.pcap "$filter" 2>"$tmp/sll2.log" &
pids="$pids $!"
sudo tcpdump -i lo -s 0 -U --time-stamp-precision=nano -w lo-nano.pcap "$filter" 2>"$tmp/lo.log" &
pids="$pids $!"
sleep 2
for log in any sll2 lo; do
	if ! grep -q "listening on" "$tmp/$log.log"; then
		echo "record.sh: a tcpdump didn't start; its output:" >&2
		cat "$tmp/$log.log" >&2
		exit 1
	fi
done

# The exchange the test expects: fixture-sub receives three QoS 1
# messages "hello" on fixture/a, published by fixture-pub.
"$tmp/mqttsub" -address "127.0.0.1:$port" -id fixture-sub -t 'fixture/#' -q 1 -C 3 -W 10s -quiet &
sub=$!
sleep 1
"$tmp/mqttpub" -address "127.0.0.1:$port" -id fixture-pub -t fixture/a -m hello -q 1 -n 3 -quiet
wait "$sub"
sleep 2

cleanup
pids=""
trap - EXIT
sudo chown "$(id -u):$(id -g)" any.pcap any-sll2.pcap lo-nano.pcap
gzip -kf lo-nano.pcap
echo "recorded: any.pcap any-sll2.pcap lo-nano.pcap lo-nano.pcap.gz"
