package bindings

import (
	"bytes"
	"errors"
	"io"
	"net"
	"slices"
	"strings"
	"testing"

	v8 "rogchap.com/v8go"

	"github.com/zain-saqer/tinynode/internal/eventloop"
)

// runtime is a V8 context with the bindings installed, like main sets up,
// with stdout and stderr captured.
type runtime struct {
	ctx    *v8.Context
	loop   *eventloop.Loop
	stdout bytes.Buffer
	stderr bytes.Buffer
}

func newRuntime(t *testing.T) *runtime {
	t.Helper()
	loop, err := eventloop.New()
	if err != nil {
		t.Fatal(err)
	}
	iso := v8.NewIsolate()
	ctx := v8.NewContext(iso)
	t.Cleanup(func() {
		ctx.Close()
		iso.Dispose()
		_ = loop.Close()
	})
	rt := &runtime{ctx: ctx, loop: loop}
	if err := Install(iso, ctx, loop, &rt.stdout, &rt.stderr); err != nil {
		t.Fatal(err)
	}
	return rt
}

// eval runs JS and returns its value. It fails the test on an exception.
func (rt *runtime) eval(t *testing.T, script string) *v8.Value {
	t.Helper()
	v, err := rt.ctx.RunScript(script, "test.js")
	if err != nil {
		t.Fatal(err)
	}
	return v
}

// run runs a script's top level and then the loop, like `tinynode file.js`.
// Run is called on the test's goroutine because V8 must not be used from two
// goroutines at once; a hang is caught by go test's own timeout.
func (rt *runtime) run(t *testing.T, script string) error {
	t.Helper()
	rt.eval(t, script)
	return rt.loop.Run()
}

func TestLogJoinsArgumentsWithSpaces(t *testing.T) {
	rt := newRuntime(t)
	rt.eval(t, `log("a", 1, true); log()`)
	if got, want := rt.stdout.String(), "[Console] a 1 true\n[Console] \n"; got != want {
		t.Errorf("log printed %q, want %q", got, want)
	}
	if rt.stderr.Len() != 0 {
		t.Errorf("stderr = %q, want nothing", rt.stderr.String())
	}
}

// caught runs a statement that should throw and returns "Name: message".
func (rt *runtime) caught(t *testing.T, statement string) string {
	t.Helper()
	return rt.eval(t, `(() => {
		try { `+statement+`; } catch (e) { return e.name + ": " + e.message; }
		return "did not throw";
	})()`).String()
}

func TestBadArgumentsThrowTypeError(t *testing.T) {
	rt := newRuntime(t)
	for statement, want := range map[string]string{
		`setTimeout()`:                       `TypeError: The "callback" argument must be of type function`,
		`setTimeout("x", 1)`:                 `TypeError: The "callback" argument must be of type function`,
		`net.createServer()`:                 `TypeError: The "address" argument must be of type string`,
		`net.createServer(1, () => {})`:      `TypeError: The "address" argument must be of type string`,
		`net.createServer("127.0.0.1:0")`:    `TypeError: The "onConnection" argument must be of type function`,
		`net.createServer("127.0.0.1:0", 1)`: `TypeError: The "onConnection" argument must be of type function`,
	} {
		if got := rt.caught(t, statement); got != want {
			t.Errorf("%s threw %q, want %q", statement, got, want)
		}
	}
	if rt.stderr.Len() != 0 {
		t.Errorf("stderr = %q, want nothing", rt.stderr.String())
	}
}

func TestSetTimeoutDelayAndArgumentsLikeNode(t *testing.T) {
	rt := newRuntime(t)
	err := rt.run(t, `
		var order = [];
		setTimeout(() => order.push("string 30"), "30");
		setTimeout(() => order.push("10"), 10);
		setTimeout(() => order.push("no delay"));
		setTimeout(() => order.push("abc"), "abc");
		setTimeout(() => order.push("negative"), -5);
		setTimeout((a, b) => order.push("args " + (a + b)), 1, 2, 3);
	`)
	if err != nil {
		t.Fatal(err)
	}
	// The four 1ms timers may run in any order among themselves.
	first := strings.Split(rt.eval(t, `order.slice(0, 4).sort().join()`).String(), ",")
	if want := []string{"abc", "args 5", "negative", "no delay"}; !slices.Equal(first, want) {
		t.Errorf("1ms timers = %v, want %v", first, want)
	}
	if got := rt.eval(t, `order.slice(4).join()`).String(); got != "10,string 30" {
		t.Errorf("later timers = %s, want 10,string 30", got)
	}
}

func TestSetTimeoutRunsCallbacksInDelayOrder(t *testing.T) {
	rt := newRuntime(t)
	err := rt.run(t, `
		var order = [];
		setTimeout(() => order.push("slow"), 30);
		setTimeout(() => order.push("fast"), 5);
		order.push("sync");
	`)
	if err != nil {
		t.Fatal(err)
	}
	if got := rt.eval(t, `order.join()`).String(); got != "sync,fast,slow" {
		t.Errorf("order = %s, want sync,fast,slow", got)
	}
}

func TestExceptionInTimerStopsTheLoop(t *testing.T) {
	rt := newRuntime(t)
	err := rt.run(t, `
		var late = false;
		setTimeout(() => { throw new Error("boom"); }, 1);
		setTimeout(() => { late = true; }, 50);
	`)
	var jsErr *v8.JSError
	if !errors.As(err, &jsErr) || !strings.Contains(jsErr.Message, "boom") {
		t.Fatalf("err = %v, want a JSError with boom", err)
	}
	if rt.eval(t, `late`).Boolean() {
		t.Error("a later timer ran after the exception")
	}
}

func TestCreateServerThrowsAnErrorOnBadAddress(t *testing.T) {
	rt := newRuntime(t)
	got := rt.eval(t, `
		let result;
		try { net.createServer("nonsense", () => {}); }
		catch (e) { result = (e instanceof Error) + " " + e.message; }
		result;
	`).String()
	if !strings.HasPrefix(got, "true createServer:") {
		t.Errorf("caught %q, want an Error starting with createServer:", got)
	}
}

func TestServerEchoesAndCloses(t *testing.T) {
	rt := newRuntime(t)
	rt.eval(t, `
		var received, writeAfterClose, closedAfterClose, addressAfterClose;
		const server = net.createServer("127.0.0.1:0", (socket) => {
			socket.write("hi\n");
			socket.onData((data) => {
				received = data;
				socket.write("echo: " + data);
				socket.close();
				closedAfterClose = socket.isClosed();
				try { socket.write("late"); } catch (e) { writeAfterClose = e.message; }
				server.close();
				try { server.address(); } catch (e) { addressAfterClose = e.message; }
			});
		});
		var addr = server.address();
	`)
	addr := rt.eval(t, `addr`).String()

	// A Go client on its own goroutine; it never touches V8.
	reply := make(chan string, 1)
	go func() {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			reply <- err.Error()
			return
		}
		defer conn.Close()
		conn.Write([]byte("ping"))
		all, _ := io.ReadAll(conn) // ends when the server closes the connection
		reply <- string(all)
	}()

	if err := rt.loop.Run(); err != nil {
		t.Fatal(err)
	}
	if got := <-reply; got != "hi\necho: ping" {
		t.Errorf("client got %q, want %q", got, "hi\necho: ping")
	}
	for expr, want := range map[string]string{
		"received":          "ping",
		"closedAfterClose":  "true",
		"writeAfterClose":   "socket is closed",
		"addressAfterClose": "server.address: socket is closed",
	} {
		if got := rt.eval(t, expr).String(); got != want {
			t.Errorf("%s = %q, want %q", expr, got, want)
		}
	}
}
