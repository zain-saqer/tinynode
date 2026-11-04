package netio

import (
	"errors"
	"io"
	"net"
	"testing"

	"golang.org/x/sys/unix"

	"github.com/zain-saqer/tinynode/internal/eventloop"
)

func newTestLoop(t *testing.T) *eventloop.Loop {
	t.Helper()
	loop, err := eventloop.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = loop.Close() })
	return loop
}

// listen starts a server on a free port and returns it with the address to dial.
func listen(t *testing.T, loop *eventloop.Loop, onConnection func(*Socket) error) (*Server, string) {
	t.Helper()
	srv, err := ListenTCP(loop, "127.0.0.1:0", onConnection)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = srv.Close() })

	addr, err := srv.Addr()
	if err != nil {
		t.Fatal(err)
	}
	return srv, addr
}

func TestParseSockAddrInet4(t *testing.T) {
	for addr, want := range map[string][4]byte{
		"127.0.0.1:8080": {127, 0, 0, 1},
		"localhost:8080": {127, 0, 0, 1},
		":8080":          {0, 0, 0, 0},
	} {
		sa, err := parseSockAddrInet4(addr)
		if err != nil {
			t.Errorf("parseSockAddrInet4(%q): %v", addr, err)
			continue
		}
		if sa.Port != 8080 || sa.Addr != want {
			t.Errorf("parseSockAddrInet4(%q) = %v:%d, want %v:8080", addr, sa.Addr, sa.Port, want)
		}
	}

	for _, bad := range []string{
		"127.0.0.1",      // no port
		"127.0.0.1:http", // port is not a number
		"127.0.0.1:70000",
		"example.com:80", // would need a DNS lookup
		"[::1]:80",       // IPv6
	} {
		if _, err := parseSockAddrInet4(bad); err == nil {
			t.Errorf("parseSockAddrInet4(%q) succeeded, want an error", bad)
		}
	}
}

func TestListenTCPRejectsBadAddress(t *testing.T) {
	if _, err := ListenTCP(newTestLoop(t), "nonsense", nil); err == nil {
		t.Fatal("want an error")
	}
}

func TestListenTCPFailsWhenAddressIsInUse(t *testing.T) {
	loop := newTestLoop(t)
	_, addr := listen(t, loop, nil)
	if _, err := ListenTCP(loop, addr, nil); !errors.Is(err, unix.EADDRINUSE) {
		t.Errorf("err = %v, want EADDRINUSE", err)
	}
}

func TestServerTalksToAClient(t *testing.T) {
	loop := newTestLoop(t)
	var srv *Server
	var received string
	srv, addr := listen(t, loop, func(s *Socket) error {
		s.OnData = func(b []byte) error {
			received = string(b)
			// Nothing else to wait for once both close, so Run returns.
			_ = s.Close()
			return srv.Close()
		}
		return s.Write([]byte("welcome"))
	})

	// A plain Go client, on its own goroutine, talks to the loop's server.
	reply := make(chan string, 1)
	go func() {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			reply <- err.Error()
			return
		}
		defer conn.Close()
		buf := make([]byte, 7)
		if _, err := io.ReadFull(conn, buf); err != nil {
			reply <- err.Error()
			return
		}
		conn.Write([]byte("ping"))
		rest, _ := io.ReadAll(conn) // ends when the server closes the connection
		reply <- string(buf) + string(rest)
	}()

	if err := runWithTimeout(t, loop); err != nil {
		t.Fatal(err)
	}
	if got := <-reply; got != "welcome" {
		t.Errorf("client got %q, want %q", got, "welcome")
	}
	if received != "ping" {
		t.Errorf("server got %q, want %q", received, "ping")
	}
}

func TestOnConnectionErrorStopsTheLoop(t *testing.T) {
	loop := newTestLoop(t)
	boom := errors.New("boom")
	_, addr := listen(t, loop, func(*Socket) error { return boom })

	go func() {
		if conn, err := net.Dial("tcp", addr); err == nil {
			defer conn.Close()
			io.ReadAll(conn)
		}
	}()

	if err := runWithTimeout(t, loop); !errors.Is(err, boom) {
		t.Errorf("err = %v, want boom", err)
	}
}

func TestAddrReportsTheChosenPort(t *testing.T) {
	_, addr := listen(t, newTestLoop(t), nil)
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatal(err)
	}
	if host != "127.0.0.1" || port == "0" {
		t.Errorf("Addr = %q, want 127.0.0.1 with a real port", addr)
	}
}

func TestCloseStopsListening(t *testing.T) {
	loop := newTestLoop(t)
	srv, addr := listen(t, loop, nil)
	if err := srv.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := srv.Addr(); !errors.Is(err, ErrSocketClosed) {
		t.Errorf("Addr after Close = %v, want ErrSocketClosed", err)
	}

	// Nothing is registered any more, so Run returns straight away.
	if err := runWithTimeout(t, loop); err != nil {
		t.Fatal(err)
	}
	if conn, err := net.Dial("tcp", addr); err == nil {
		conn.Close()
		t.Error("dial succeeded after Close")
	}
}
