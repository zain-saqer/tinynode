# tinynode

A small JavaScript runtime in Go, built to learn how Node.js works inside.

The plan: V8 (via [v8go](https://github.com/rogchap/v8go)) runs the JavaScript, and everything else is written from scratch: the event loop, timers, non-blocking TCP sockets, and the bindings that expose them to JavaScript. The loop talks to the kernel directly through `kqueue`.

## Why

I'm building this to learn how the Node.js event loop works and to get better at Go.

This is a second attempt. In the first version an AI assistant wrote most of the code, so I learned less than I wanted. This time I write the code myself and use AI only to explain concepts and review my work.

## Status

Work in progress. Planned milestones:

- [x] **Event loop with timers:** a loop that runs due timers from a min-heap and exits when no work is left.
- [ ] **kqueue poller:** wait in `kevent` until a file descriptor is ready or the next timer is due.
- [ ] **Non-blocking TCP:** `socket` / `bind` / `listen` / `accept`, buffered writes with backpressure, graceful close.
- [ ] **V8 bindings:** `log`, `setTimeout`, `net.createServer`, sockets, with Go errors thrown as JS errors.
- [ ] **CLI:** `tinynode script.js` that behaves like `node` (exit codes, uncaught exceptions).
- [ ] **Tests:** unit tests per package and an end-to-end test script.

## Layout

```
main.go             (planned) reads the script, creates the loop and V8, runs both
internal/
  eventloop/        the event loop (the libuv part): timers, clock, polling
  netio/            (planned) non-blocking TCP on top of the loop
  bindings/         (planned) the JavaScript API, built with V8 function templates
```

Dependencies should go one way: `main → bindings → netio → eventloop`.

## Requirements

macOS (for kqueue; Linux with epoll is planned), Go 1.27+, and cgo (for v8go, once the bindings exist).

## Building and testing

```bash
go build ./...
go vet ./... && go test -race ./...
```
