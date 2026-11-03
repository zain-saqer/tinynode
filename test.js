// Demo of everything implemented so far:
//   log, setTimeout, net.createServer, socket.onData / write / close, server.close
//
// Run it, then connect from another terminal:
//   nc 127.0.0.1 8080
// Type lines to have them echoed back, and "quit" to disconnect.
// The server shuts down after 30 seconds, and the process exits once
// no timers or connections are left.

// log takes any number of arguments, like console.log.
log("Starting demo");
log("log accepts", 4, "arguments:", true);

// Errors from native functions are thrown as JS Errors.
try {
    net.createServer("not-an-address", () => {});
} catch (e) {
    log("createServer error:", e.message);
}

const server = net.createServer("127.0.0.1:8080", (socket) => {
    log("client connected");
    socket.write("Welcome! Type a line and I'll echo it. Type 'quit' to disconnect.\n");

    socket.onData((data) => {
        const line = data.trim();
        log("received:", line);

        if (line === "quit") {
            socket.write("Bye!\n");
            // Graceful close: anything still queued is sent before the socket closes.
            socket.close();
            try {
                socket.write("too late\n");
            } catch (e) {
                log("write after close:", e.message);
            }
            return;
        }

        // write returns false when the kernel buffer was full and some data was queued.
        const flushed = socket.write("echo: " + data);
        if (!flushed) {
            log("write queued; it will be sent when the client is ready");
        }
    });
});
log("listening on 127.0.0.1:8080 (try: nc 127.0.0.1 8080)");

setTimeout(() => log("tick: 1 second passed"), 1000);

setTimeout(() => {
    log("closing server");
    server.close();
}, 30000);
