package eventloop

import (
	"errors"
	"fmt"
	"time"

	"golang.org/x/sys/unix"
)

const pollerName = "kqueue/macOS"

// poller waits for fd readiness with kqueue.
type poller struct {
	kq          int
	wakeReadFd  int
	wakeWriteFd int
	events      []unix.Kevent_t
}

func newPoller() (*poller, error) {
	kq, err := unix.Kqueue()
	if err != nil {
		return nil, fmt.Errorf("kqueue error: %v", err)
	}

	// Create a pipe for thread notifications (macOS alternative to eventfd)
	var fds [2]int
	if err := unix.Pipe(fds[:]); err != nil {
		unix.Close(kq)
		return nil, fmt.Errorf("pipe error: %v", err)
	}
	p := &poller{
		kq:          kq,
		wakeReadFd:  fds[0],
		wakeWriteFd: fds[1],
		events:      make([]unix.Kevent_t, 64),
	}

	// Register the pipe's read end with kqueue
	err = unix.SetNonblock(p.wakeReadFd, true)
	if err == nil {
		err = p.add(p.wakeReadFd, Readable)
	}
	if err != nil {
		unix.Close(kq)
		unix.Close(p.wakeReadFd)
		unix.Close(p.wakeWriteFd)
		return nil, err
	}
	return p, nil
}

// close releases the kqueue and the wake pipe.
func (p *poller) close() error {
	return errors.Join(
		unix.Close(p.kq),
		unix.Close(p.wakeReadFd),
		unix.Close(p.wakeWriteFd),
	)
}

func kqueueFilter(interest Interest) int16 {
	switch interest {
	case Readable:
		return unix.EVFILT_READ
	case Writable:
		return unix.EVFILT_WRITE
	}
	panic(fmt.Sprintf("eventloop: unknown interest %d", interest))
}

func (p *poller) add(fd int, interest Interest) error {
	ev := unix.Kevent_t{
		Ident:  uint64(fd),
		Filter: kqueueFilter(interest),
		Flags:  unix.EV_ADD | unix.EV_ENABLE,
	}
	_, err := unix.Kevent(p.kq, []unix.Kevent_t{ev}, nil, nil)
	return err
}

func (p *poller) remove(fd int, interest Interest) error {
	ev := unix.Kevent_t{
		Ident:  uint64(fd),
		Filter: kqueueFilter(interest),
		Flags:  unix.EV_DELETE,
	}
	_, err := unix.Kevent(p.kq, []unix.Kevent_t{ev}, nil, nil)
	if err != nil && !errors.Is(err, unix.ENOENT) {
		return err
	}
	return nil
}

// wake interrupts a wait in progress, from any goroutine.
func (p *poller) wake() {
	_, _ = unix.Write(p.wakeWriteFd, []byte{1})
}

// wait blocks until an fd is ready, wake is called, or timeout passes; a
// negative timeout waits forever. It calls ready for each ready fd and stops
// at the first error.
func (p *poller) wait(timeout time.Duration, ready func(fd int, interest Interest) error) error {
	var ts *unix.Timespec
	if timeout >= 0 {
		t := unix.NsecToTimespec(timeout.Nanoseconds())
		ts = &t
	}

	n, err := unix.Kevent(p.kq, nil, p.events, ts)
	if err != nil {
		if errors.Is(err, unix.EINTR) {
			return nil
		}
		return fmt.Errorf("kevent: %w", err)
	}

	for i := range n {
		fd := int(p.events[i].Ident)
		if fd == p.wakeReadFd {
			var buf [8]byte
			_, _ = unix.Read(fd, buf[:]) // Clear the pipe notification
			continue
		}

		var interest Interest
		switch p.events[i].Filter {
		case unix.EVFILT_READ:
			interest = Readable
		case unix.EVFILT_WRITE:
			interest = Writable
		default:
			continue
		}
		if err := ready(fd, interest); err != nil {
			return err
		}
	}
	return nil
}
