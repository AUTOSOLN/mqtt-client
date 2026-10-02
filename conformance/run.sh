#!/usr/bin/env bash
# Runs libmosquitto's client test suite (mosquitto/test/lib) against this
# client. Usage: run.sh [script ...]   (default: every script the programs
# in main.go cover; a script name is the .py file without its extension)
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
	mapfile -t cases < <("$build/conformance" -scripts | sort)
fi

# The harness runs $BUILD_ROOT/test/lib/{c,cpp}/<program>.test PORT [ARGS].
mkdir -p "$build/test/lib/c" "$build/test/lib/cpp"
while read -r prog; do
	ln -s "$build/conformance" "$build/test/lib/c/$prog.test"
	ln -s "$build/conformance" "$build/test/lib/cpp/$prog.test"
done < <("$build/conformance" -list)

export BUILD_ROOT=$build
export PYTHONPATH="$mosq/test/lib:$mosq/test"
# Most scripts find the programs through BUILD_ROOT; a few start
# c/<program>.test relative to the working directory.
cd "$build/test/lib"

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
