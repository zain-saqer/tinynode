package eventloop

import (
	"testing"

	"golang.org/x/sys/unix"
)

// Tests in this file look inside the kqueue backend. Backend-neutral tests of
// the poller contract are in poller_test.go.

// Closing a closed poller does not close the reused fd.
func TestCloseTwiceDoesNotCloseReusedFd(t *testing.T) {
	p, err := newKqueuePoller()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := p.close(); err != nil {
			t.Error(err)
		}
	})
	oldKq := p.kq
	if err := p.close(); err != nil {
		t.Fatal(err)
	}
	r, w := newPipe(t)
	if oldKq != r && oldKq != w {
		t.Fatal("expected the closed poller fd to be equal to one of the pipe ends fds")
	}
	if err := p.close(); err != nil {
		t.Fatal(err)
	}
	if _, err := unix.Write(w, []byte("w")); err != nil {
		t.Fatal(err)
	}
	n, err := unix.Read(r, []byte{0})
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("expected 1 byte, got %d", n)
	}
}
