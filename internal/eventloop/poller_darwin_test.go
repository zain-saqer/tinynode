package eventloop

import (
	"errors"
	"testing"

	"golang.org/x/sys/unix"
)

// Tests in this file look inside the kqueue backend. Backend-neutral tests of
// the poller contract are in poller_test.go.

func newTestKqueuePoller(t *testing.T) *kqueuePoller {
	t.Helper()
	p, err := newKqueuePoller()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := p.close(); err != nil {
			t.Error(err)
		}
	})
	return p
}

// Closing a closed poller does not close the reused fd.
func TestCloseTwiceDoesNotCloseReusedFd(t *testing.T) {
	p := newTestKqueuePoller(t)
	oldKq := p.kq
	if err := p.close(); err != nil {
		t.Fatal(err)
	}
	r, w := newPipe(t)
	if oldKq != r && oldKq != w {
		t.Fatalf("pipe fds = %d, %d, want one of them to reuse the closed kqueue fd %d", r, w, oldKq)
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
		t.Fatalf("read %d bytes, want 1", n)
	}
}

// wait resets its batch state (p.i and p.n) on every return: a normal
// return, a failed kevent call and an error from ready. The cases that run a
// batch use two events, so p.i and p.n would be non-zero without the reset.
func TestWaitResetsBatchStateOnReturn(t *testing.T) {
	tests := []struct {
		name  string
		setup func(t *testing.T) *kqueuePoller
	}{
		{
			name: "normal",
			setup: func(t *testing.T) *kqueuePoller {
				p := newTestKqueuePoller(t)
				r, w := newPipe(t)
				if err := p.add(r, Readable); err != nil {
					t.Fatal(err)
				}
				if err := p.add(w, Writable); err != nil {
					t.Fatal(err)
				}
				if _, err := unix.Write(w, []byte{0}); err != nil {
					t.Fatal(err)
				}
				if err := p.wait(0, emptyReady); err != nil {
					t.Fatal(err)
				}
				return p
			},
		},
		{
			name: "EBADF",
			setup: func(t *testing.T) *kqueuePoller {
				p, err := newKqueuePoller()
				if err != nil {
					t.Fatal(err)
				}
				// Close the kqueue fd directly so that kevent fails with EBADF.
				// Don't call p.close() later: its kq field still holds the
				// closed fd's number, and closing it could close an fd that
				// reused that number.
				if err := unix.Close(p.kq); err != nil {
					t.Fatal(err)
				}
				if err := p.wait(0, emptyReady); !errors.Is(err, unix.EBADF) {
					t.Fatalf("p.wait() = %v, want %v", err, unix.EBADF)
				}
				return p
			},
		},
		{
			name: "error from ready",
			setup: func(t *testing.T) *kqueuePoller {
				p := newTestKqueuePoller(t)
				r, w := newPipe(t)
				if err := p.add(r, Readable); err != nil {
					t.Fatal(err)
				}
				if err := p.add(w, Writable); err != nil {
					t.Fatal(err)
				}
				if _, err := unix.Write(w, []byte("test")); err != nil {
					t.Fatal(err)
				}
				calls := 0
				readyError := errors.New("error from ready")
				if err := p.wait(0, func(fd int, interest Interest) error {
					calls++
					if calls == 2 {
						return readyError
					}
					return nil
				}); !errors.Is(err, readyError) {
					t.Fatalf("p.wait() = %v, want %v", err, readyError)
				}
				return p
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := tt.setup(t)
			if p.i != 0 {
				t.Errorf("p.i = %d, want 0", p.i)
			}
			if p.n != 0 {
				t.Errorf("p.n = %d, want 0", p.n)
			}
		})
	}
}

// invalidate marks only fd's read and write events dead, not the wake event,
// which also has ident 0. The test sets the batch by hand: a current event at
// index 0, then fd 0's read and write events and the wake event. invalidate(0)
// must mark the read and write events dead and leave the wake event and the
// current event alone. invalidate only touches events, i and n, so no kqueue
// is needed.
func TestInvalidateKeepsWakeEvent(t *testing.T) {
	events := []unix.Kevent_t{
		{Ident: 1, Filter: unix.EVFILT_WRITE}, // current event
		{Ident: 0, Filter: unix.EVFILT_READ},
		{Ident: 0, Filter: unix.EVFILT_WRITE},
		{Ident: wakeIdent, Filter: unix.EVFILT_USER},
	}
	p := &kqueuePoller{events: events, i: 0, n: len(events)}
	p.invalidate(0)
	want := []struct {
		name   string
		filter int16
	}{
		{"current event", unix.EVFILT_WRITE},
		{"fd 0 read event", deadFilter},
		{"fd 0 write event", deadFilter},
		{"wake event", unix.EVFILT_USER},
	}
	for j, w := range want {
		if got := p.events[j].Filter; got != w.filter {
			t.Errorf("%s filter = %d, want %d", w.name, got, w.filter)
		}
	}
}
