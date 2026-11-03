#!/usr/bin/env bash
# Builds the runtime and tests it end to end:
#   - command-line handling (no args, missing file, uncaught exception)
#   - test.js: talks to its echo server over TCP and checks that the process
#     exits on its own after the server closes (about 30 seconds).
set -euo pipefail

cd "$(dirname "$0")"

PORT=8080
tmp=$(mktemp -d)
pid=""

cleanup() {
	exec 3<&- 2>/dev/null || true
	if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
		kill "$pid" 2>/dev/null || true
	fi
	rm -rf "$tmp"
}
trap cleanup EXIT

failures=0
pass() { echo "  ok   $1"; }
fail() { echo "  FAIL $1"; failures=$((failures + 1)); }

# expect_eq NAME ACTUAL EXPECTED
expect_eq() {
	if [ "$2" == "$3" ]; then pass "$1"; else fail "$1 (got: '$2', want: '$3')"; fi
}

# expect_log NAME TEXT: the process output contains TEXT.
expect_log() {
	if grep -qF -- "$2" "$tmp/out.txt"; then pass "$1"; else fail "$1 (missing log: '$2')"; fi
}

echo "Building..."
go build -o "$tmp/simple_node" .
bin="$tmp/simple_node"

echo "Command line:"
status=0
"$bin" >/dev/null 2>"$tmp/err.txt" || status=$?
expect_eq "no arguments exits with 2" "$status" "2"
if grep -q "usage:" "$tmp/err.txt"; then pass "no arguments prints usage"; else fail "no arguments prints usage"; fi

status=0
"$bin" "$tmp/does-not-exist.js" >/dev/null 2>&1 || status=$?
expect_eq "missing file exits with 1" "$status" "1"

echo 'throw new Error("boom");' >"$tmp/throws.js"
status=0
"$bin" "$tmp/throws.js" >/dev/null 2>"$tmp/err.txt" || status=$?
expect_eq "uncaught exception exits with 1" "$status" "1"
if grep -q "boom" "$tmp/err.txt"; then pass "uncaught exception is printed"; else fail "uncaught exception is printed"; fi

# The 5s timer must never run: the exception stops the loop straight away.
echo 'setTimeout(() => log("late"), 5000); setTimeout(() => { throw new Error("timer boom"); }, 10);' >"$tmp/timer-throws.js"
status=0
"$bin" "$tmp/timer-throws.js" >"$tmp/out.txt" 2>"$tmp/err.txt" || status=$?
expect_eq "exception in a callback exits with 1" "$status" "1"
if grep -q "timer boom" "$tmp/err.txt"; then pass "exception in a callback is printed"; else fail "exception in a callback is printed"; fi
if grep -q "late" "$tmp/out.txt"; then fail "loop stops after a callback exception"; else pass "loop stops after a callback exception"; fi

echo "test.js:"
"$bin" test.js >"$tmp/out.txt" 2>&1 &
pid=$!

# Wait for the server to report it is listening.
for _ in $(seq 1 50); do
	grep -q "listening on" "$tmp/out.txt" 2>/dev/null && break
	sleep 0.1
done
expect_log "server starts listening" "listening on 127.0.0.1:$PORT"

exec 3<>"/dev/tcp/127.0.0.1/$PORT"

line=""
read -r -t 2 line <&3 || true
expect_eq "welcome message" "$line" "Welcome! Type a line and I'll echo it. Type 'quit' to disconnect."

printf 'hello\n' >&3
line=""
read -r -t 2 line <&3 || true
expect_eq "echoes a line" "$line" "echo: hello"

printf 'quit\n' >&3
line=""
read -r -t 2 line <&3 || true
expect_eq "says bye on quit" "$line" "Bye!"

# After "Bye!" the server closes the connection, so the next read hits EOF (status 1).
status=0
read -r -t 2 line <&3 || status=$?
expect_eq "server closes the connection" "$status" "1"
exec 3<&-

echo "  ...waiting for the server to shut down (up to 35s)"
for _ in $(seq 1 350); do
	kill -0 "$pid" 2>/dev/null || break
	sleep 0.1
done
if kill -0 "$pid" 2>/dev/null; then
	fail "process exits after the server closes"
else
	status=0
	wait "$pid" || status=$?
	pid=""
	expect_eq "process exits with 0" "$status" "0"
fi

expect_log "log with several arguments" "log accepts 4 arguments: true"
expect_log "createServer throws on a bad address" "createServer error: createServer: invalid address format"
expect_log "client connects" "client connected"
expect_log "onData receives data" "received: hello"
expect_log "write after close throws" "write after close: socket is closed"
expect_log "setTimeout fires" "tick: 1 second passed"
expect_log "server.close runs" "closing server"

echo
if [ "$failures" -eq 0 ]; then
	echo "All tests passed."
else
	echo "$failures test(s) failed. Process output:"
	cat "$tmp/out.txt"
	exit 1
fi
