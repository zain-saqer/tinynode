package netio

import (
	"bytes"
	"errors"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/zain-saqer/tinynode/internal/eventloop"
)

// newTestSocket returns a Socket on one end of a connected socketpair and the
// raw fd of the other end, which plays the remote peer. The Socket's fd is
// non-blocking, like an accepted connection; the peer's is blocking.
func newTestSocket(t *testing.T) (*eventloop.Loop, *Socket, int) {
	t.Helper()
	loop, err := eventloop.New()
	if err != nil {
		t.Fatal(err)
	}
	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := unix.SetNonblock(fds[0], true); err != nil {
		t.Fatal(err)
	}
	s := newSocket(loop, fds[0])
	t.Cleanup(func() {
		_ = s.Destroy()
		_ = unix.Close(fds[1])
		_ = loop.Close()
	})
	return loop, s, fds[1]
}

// runWithTimeout fails the test instead of hanging it when Run never returns.
func runWithTimeout(t *testing.T, loop *eventloop.Loop) error {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- loop.Run() }()
	select {
	case err := <-done:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return")
		return nil
	}
}

// readAll reads fd until EOF in the background and sends everything it got.
func readAll(fd int) <-chan []byte {
	out := make(chan []byte, 1)
	go func() {
		var got []byte
		buf := make([]byte, 64*1024)
		for {
			n, err := unix.Read(fd, buf)
			if n <= 0 || err != nil {
				break
			}
			got = append(got, buf[:n]...)
		}
		out <- got
	}()
	return out
}

// pattern returns n bytes that differ from position to position, so reordered
// or dropped chunks show up in a comparison.
func pattern(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i % 251)
	}
	return b
}

func TestWriteSendsSmallDataRightAway(t *testing.T) {
	_, s, peer := newTestSocket(t)

	if err := s.Write([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	if s.Buffered() != 0 {
		t.Errorf("Buffered = %d, want 0", s.Buffered())
	}
	buf := make([]byte, 16)
	n, _ := unix.Read(peer, buf)
	if string(buf[:n]) != "hello" {
		t.Errorf("peer read %q, want %q", buf[:n], "hello")
	}
}

func TestWriteQueuesWhatTheKernelRejectsAndCloseWaitsForIt(t *testing.T) {
	loop, s, peer := newTestSocket(t)
	first, second := pattern(1<<20), []byte("tail")

	if err := s.Write(first); err != nil {
		t.Fatal(err)
	}
	if s.Buffered() == 0 {
		t.Fatal("1MB should not fit in the socket buffer; nothing was queued")
	}
	if err := s.Write(second); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if s.Closed {
		t.Fatal("Close closed the socket before the queue drained")
	}
	if err := s.Write([]byte("late")); !errors.Is(err, ErrSocketClosed) {
		t.Errorf("Write after Close = %v, want ErrSocketClosed", err)
	}

	got := readAll(peer)
	if err := runWithTimeout(t, loop); err != nil {
		t.Fatal(err)
	}
	if !s.Closed {
		t.Error("socket not closed after draining")
	}
	if want := append(first, second...); !bytes.Equal(<-got, want) {
		t.Error("peer did not receive every byte in order")
	}
}

func TestWriteAfterDestroyFails(t *testing.T) {
	_, s, _ := newTestSocket(t)
	if err := s.Destroy(); err != nil {
		t.Fatal(err)
	}
	if err := s.Write([]byte("x")); !errors.Is(err, ErrSocketClosed) {
		t.Errorf("err = %v, want ErrSocketClosed", err)
	}
}

func TestReadingPassesDataAndClosesOnPeerHangup(t *testing.T) {
	loop, s, peer := newTestSocket(t)
	var got []byte
	s.OnData = func(b []byte) error {
		got = append(got, b...)
		return nil
	}
	if err := s.startReading(); err != nil {
		t.Fatal(err)
	}
	unix.Write(peer, []byte("hi"))
	unix.Close(peer) // the loop reads "hi", then EOF

	if err := runWithTimeout(t, loop); err != nil {
		t.Fatal(err)
	}
	if string(got) != "hi" {
		t.Errorf("OnData got %q, want %q", got, "hi")
	}
	if !s.Closed {
		t.Error("socket not closed after the peer hung up")
	}
}

func TestOnDataErrorStopsTheLoop(t *testing.T) {
	loop, s, peer := newTestSocket(t)
	boom := errors.New("boom")
	s.OnData = func([]byte) error { return boom }
	if err := s.startReading(); err != nil {
		t.Fatal(err)
	}
	unix.Write(peer, []byte("x"))

	if err := runWithTimeout(t, loop); !errors.Is(err, boom) {
		t.Errorf("err = %v, want boom", err)
	}
}
