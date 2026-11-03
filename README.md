# tinynode

A small JavaScript runtime in Go, built to learn how Node.js works inside.

V8 (via [v8go](https://github.com/rogchap/v8go)) runs the JavaScript. Everything else is written from scratch: the event loop, timers, non-blocking TCP sockets, and the bindings to JavaScript. The loop talks to the kernel directly through `kqueue`.

```js
const server = net.createServer("127.0.0.1:8080", (socket) => {
    socket.write("Welcome!\n");
    socket.onData((data) => socket.write("echo: " + data));
});

setTimeout(() => server.close(), 30000);
```

```
$ go run . testdata/test.js
$ nc 127.0.0.1 8080
```

## Why

I built this project to learn how the Node.js event loop works and to get better at Go.

## What it can do

| JavaScript | What it does |
|---|---|
| `log(...args)` | Prints its arguments, like `console.log`. |
| `setTimeout(fn, ms)` | Runs `fn` once after `ms` milliseconds. |
| `net.createServer(address, onConnection)` | Listens on an IPv4 `"host:port"` and calls `onConnection(socket)` for each client. Returns a server with `close()`. Throws on a bad address. |
| `socket.write(string)` | Sends data. Returns `false` if some of it had to be queued, like Node. |
| `socket.onData(fn)` | Calls `fn(chunk)` for each chunk received. |
| `socket.close()` | Sends any queued data, then closes. Writing after `close()` throws. |
| `socket.isClosed()` | Whether the socket is closed or closing. |

On the command line it behaves like `node`:

- It exits when no timers or sockets are left.
- An uncaught exception prints its stack trace and exits with status 1.
- Running it without a script prints usage and exits with status 2.

## How it works

```
main.go                     reads the script, creates the loop and V8, runs both
internal/
  eventloop/                the event loop (the libuv part)
    eventloop.go              portable loop: register fds, run timers, dispatch callbacks
    eventloop_darwin.go       the kqueue poller and its wake-up pipe
    timer.go                  timers in a min-heap ordered by due time
    clock.go                  monotonic clock that timers are scheduled on
  netio/                    non-blocking TCP on top of the loop
    socket.go                 buffered writes, reads, graceful close
    server.go                 socket / bind / listen / accept
  bindings/                 the JavaScript API, built with V8 function templates
testdata/test.js            demo script that uses every API
test.sh                     end-to-end tests
```

Dependencies go one way: `main → bindings → netio → eventloop`. Only `bindings` knows about V8, and only `eventloop_darwin.go` knows about kqueue.

### The event loop

`Loop.Run` repeats these steps while there is work left:

1. **Timers:** run every timer that is due.
2. **Poll:** wait in `kevent` until a socket is ready or the next timer is due.
3. **I/O:** run the callback for each ready socket.
4. **Tasks:** run work queued from other goroutines with `QueueTask`.

Steps 1 and 2 match Node's *timers* and *poll* phases. The loop stops when no timers or sockets are left, or when a callback throws.

### Sockets and backpressure

Sockets never block. `write` sends what the kernel accepts and queues the rest. The queue is flushed when the socket becomes writable again, in order. `close()` waits for the queue to empty first.

### Talking to V8

Each JavaScript function wraps a Go function. Go errors are thrown as real JS `Error` objects, so scripts can catch them. Exceptions from JS callbacks come back to Go and stop the loop.

## Running it

You need macOS (for kqueue), Go 1.27+, and cgo (for v8go). v8go ships V8 prebuilt, so there's nothing else to install.

```bash
go build -o tinynode .
./tinynode testdata/test.js
```

In another terminal, connect and type lines to see them echoed. Type `quit` to disconnect:

```bash
nc 127.0.0.1 8080
```

The demo server stops after 30 seconds, and the process exits when the last client leaves.

## Tests

```bash
./test.sh
```

This builds the binary and tests exit codes, uncaught exceptions, and a real TCP session with the demo server. It takes about 30 seconds because it waits for the server to shut down.
