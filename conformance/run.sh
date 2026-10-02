#!/usr/bin/env bash
# Runs libmosquitto's client test suite (mosquitto/test/lib) against this
# client. Usage: run.sh [case ...]   (default: every case in main.go)
#
# MOSQUITTO_SRC points at a mosquitto checkout (default: ../../mosquitto,
# next to this repository). Needs python3.
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
mosq=$(cd "${MOSQUITTO_SRC:-$here/../../mosquitto}" && pwd)
build=$(mktemp -d)
trap 'rm -rf "$build"' EXIT

go build -o "$build/conformance" "$here"

if [ $# -gt 0 ]; then
	cases=("$@")
else
	mapfile -t cases < <("$build/conformance" -list | sort)
fi

# The harness runs $BUILD_ROOT/test/lib/{c,cpp}/<case>.test PORT.
mkdir -p "$build/test/lib/c" "$build/test/lib/cpp"
for name in "${cases[@]}"; do
	ln -s "$build/conformance" "$build/test/lib/c/$name.test"
	ln -s "$build/conformance" "$build/test/lib/cpp/$name.test"
done

export BUILD_ROOT=$build
export PYTHONPATH="$mosq/test/lib:$mosq/test"
cd "$mosq/test/lib"

failed=()
for name in "${cases[@]}"; do
	script="$mosq/test/lib/$name.py"
	if [ -f "$here/overrides/$name.py" ]; then
		script="$here/overrides/$name.py"
	fi
	if timeout 60 python3 "$script" >"$build/$name.log" 2>&1; then
		echo "PASS $name"
	else
		echo "FAIL $name"
		sed 's/^/    /' "$build/$name.log"
		failed+=("$name")
	fi
done

echo "${#cases[@]} cases, ${#failed[@]} failed"
[ ${#failed[@]} -eq 0 ]
