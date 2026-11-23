package eventloop

import (
	"errors"
	"fmt"
	"os"
	"time"

	"golang.org/x/sys/unix"
)

// kqueuePoller is the poller for macOS and the BSDs, built on kqueue(2).
type kqueuePoller struct {
	kq     int
	events []unix.Kevent_t // buffer that kevent fills with ready events
	// While wait runs, events[:n] is the current batch and events[i] is the
	// event being reported. Both are 0 when wait is not running.
	i int
	n int
}

const wakeIdent = uint64(0)

// deadFilter marks an event in the current batch as invalidated, so wait
// skips it. 0 is not a real kqueue filter (the EVFILT_* values are negative),
// so it cannot clash with an event from the kernel.
const deadFilter = 0

func newPoller() (poller, error) {
	return newKqueuePoller()
}

func newKqueuePoller() (*kqueuePoller, error) {
	kq, err := unix.Kqueue()
	if err != nil {
		return nil, fmt.Errorf("poller new: %w", os.NewSyscallError("kqueue", err))
	}
	p := &kqueuePoller{
		kq:     kq,
		events: make([]unix.Kevent_t, 1024),
	}
	// add wakeup event for waking up the poller from other goroutines
	ev := unix.Kevent_t{
		Ident:  wakeIdent,
		Filter: unix.EVFILT_USER,
		Flags:  unix.EV_ADD | unix.EV_CLEAR,
	}
	_, err = unix.Kevent(p.kq, []unix.Kevent_t{ev}, nil, nil)
	if err != nil {
		unix.Close(p.kq)
		return nil, fmt.Errorf("poller new: %w", os.NewSyscallError("kevent", err))
	}
	return p, nil
}

func (p *kqueuePoller) close() error {
	if p.kq == -1 {
		return nil
	}
	kq := p.kq
	p.kq = -1
	if err := unix.Close(kq); err != nil {
		return fmt.Errorf("poller close: %w", os.NewSyscallError("close", err))
	}
	return nil
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

func (p *kqueuePoller) add(fd int, interest Interest) error {
	if p.kq == -1 {
		return fmt.Errorf("poller add fd %d %v: %w", fd, interest, ErrClosed)
	}
	ev := unix.Kevent_t{
		Ident:  uint64(fd),
		Filter: kqueueFilter(interest),
		Flags:  unix.EV_ADD | unix.EV_ENABLE,
	}
	_, err := unix.Kevent(p.kq, []unix.Kevent_t{ev}, nil, nil)
	if err != nil {
		return fmt.Errorf("poller add fd %d %v: %w", fd, interest, os.NewSyscallError("kevent", err))
	}
	return nil
}

func (p *kqueuePoller) remove(fd int, interest Interest) error {
	if p.kq == -1 {
		return fmt.Errorf("poller remove fd %d %v: %w", fd, interest, ErrClosed)
	}
	ev := unix.Kevent_t{
		Ident:  uint64(fd),
		Filter: kqueueFilter(interest),
		Flags:  unix.EV_DELETE,
	}
	_, err := unix.Kevent(p.kq, []unix.Kevent_t{ev}, nil, nil)
	if err != nil && !errors.Is(err, unix.ENOENT) {
		return fmt.Errorf("poller remove fd %d %v: %w", fd, interest, os.NewSyscallError("kevent", err))
	}
	return nil
}

func (p *kqueuePoller) wake() error {
	if p.kq == -1 {
		return fmt.Errorf("poller wake: %w", ErrClosed)
	}
	ev := unix.Kevent_t{
		Ident:  wakeIdent,
		Filter: unix.EVFILT_USER,
		Fflags: unix.NOTE_TRIGGER,
	}
	_, err := unix.Kevent(p.kq, []unix.Kevent_t{ev}, nil, nil)
	if err != nil {
		return fmt.Errorf("poller wake: %w", os.NewSyscallError("kevent", err))
	}
	return nil
}

// invalidate marks the events for fd after the current one as dead. kqueue
// reports read and write as separate events, so the current event never needs
// marking: it is already being reported.
func (p *kqueuePoller) invalidate(fd int) {
	if p.n == 0 {
		return
	}
	for j := p.i + 1; j < p.n; j++ {
		ev := &p.events[j]
		// kqueue identifies an event by ident and filter. Match only fd's read
		// and write events: the wake event also has ident 0, like stdin.
		if ev.Ident == uint64(fd) && (ev.Filter == unix.EVFILT_READ || ev.Filter == unix.EVFILT_WRITE) {
			ev.Filter = deadFilter
		}
	}
}

func (p *kqueuePoller) wait(timeout time.Duration, ready func(fd int, interest Interest) error) error {
	if ready == nil {
		panic("eventloop: ready function is nil")
	}
	if p.kq == -1 {
		return fmt.Errorf("poller wait: %w", ErrClosed)
	}
	var ts *unix.Timespec
	if timeout >= 0 {
		t := unix.NsecToTimespec(timeout.Nanoseconds())
		ts = &t
	}
	var err error
	defer func() {
		p.i = 0
		p.n = 0
	}()
	p.n, err = unix.Kevent(p.kq, nil, p.events, ts)
	if err != nil {
		if errors.Is(err, unix.EINTR) {
			return nil
		}
		return fmt.Errorf("poller wait: %w", os.NewSyscallError("kevent", err))
	}
	for p.i = range p.n {
		event := &p.events[p.i]
		var interest Interest
		switch event.Filter {
		case unix.EVFILT_READ:
			interest = Readable
		case unix.EVFILT_WRITE:
			interest = Writable
		// wake has no fd to report; it only interrupts kevent
		case unix.EVFILT_USER:
			continue
		case deadFilter:
			continue
		default:
			continue
		}
		if err := ready(int(event.Ident), interest); err != nil {
			return err
		}
	}
	return nil
}
