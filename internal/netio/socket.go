// Package netio implements non-blocking TCP sockets on top of the event loop.
package netio

import (
	"errors"

	"golang.org/x/sys/unix"

	"github.com/zain-saqer/tinynode/internal/eventloop"
)

var ErrSocketClosed = errors.New("socket is closed")

type Socket struct {
	Fd                int
	Closed            bool
	ClosedWhenDrained bool
	outBuf            []byte
	loop              *eventloop.Loop
	//todo: buffer data in case OnData cb is nil
	OnData func([]byte) error
	//todo: add OnClose and OnError callbacks
}

func newSocket(loop *eventloop.Loop, fd int) *Socket {
	return &Socket{Fd: fd, loop: loop}
}

func (s *Socket) watch(interest eventloop.Interest, cb eventloop.Callback) error {
	if s.Closed {
		return ErrSocketClosed
	}
	return s.loop.Register(s.Fd, interest, cb)
}

func (s *Socket) unwatch(interest eventloop.Interest) error {
	if s.Closed {
		return nil
	}
	return s.loop.RemoveInterest(s.Fd, interest)
}

// Destroy closes the socket right away, dropping anything still queued.
func (s *Socket) Destroy() error {
	if s.Closed {
		return nil
	}
	s.loop.Unregister(s.Fd)
	err := unix.Close(s.Fd)
	s.Closed = true
	s.outBuf = nil
	return err
}

// Close closes the socket once all queued bytes have been sent.
func (s *Socket) Close() error {
	if s.Closed {
		return nil
	}
	if len(s.outBuf) == 0 {
		return s.Destroy()
	}
	s.ClosedWhenDrained = true
	return nil
}

// Buffered returns how many written bytes are still queued for the kernel.
func (s *Socket) Buffered() int {
	return len(s.outBuf)
}

// Write sends what the kernel accepts now and queues the rest.
func (s *Socket) Write(data []byte) error {
	if s.Closed {
		return ErrSocketClosed
	}
	if s.ClosedWhenDrained {
		return ErrSocketClosed
	}
	if len(s.outBuf) > 0 {
		// Keep ordering: earlier queued bytes must go first.
		s.outBuf = append(s.outBuf, data...)
		return nil
	}
	n, err := unix.Write(s.Fd, data)
	if err != nil && !errors.Is(err, unix.EAGAIN) {
		return err
	}
	if n < 0 {
		n = 0
	}
	if n < len(data) {
		s.outBuf = append(s.outBuf, data[n:]...)
		err := s.watch(eventloop.Writable, func(fd int, interest eventloop.Interest) error {
			err := s.flush()
			if err != nil || (s.ClosedWhenDrained && len(s.outBuf) == 0) {
				_ = s.Destroy()
			}
			return nil
		})
		if err != nil {
			// Can't wait for writability; the stream would be missing bytes, so drop the connection.
			s.outBuf = nil
			_ = s.Destroy()
			return err
		}
	}
	return nil
}

// flush writes queued bytes. It returns nil when the buffer is drained or the
// socket would block; any other error means the connection is broken.
func (s *Socket) flush() error {
	for len(s.outBuf) > 0 {
		n, err := unix.Write(s.Fd, s.outBuf)
		if err != nil {
			if errors.Is(err, unix.EINTR) {
				continue
			}
			if errors.Is(err, unix.EAGAIN) {
				return nil // still full; wait for the next write event
			}
			return err
		}
		s.outBuf = s.outBuf[n:]
	}
	// Drained: stop watching for writability.
	s.outBuf = nil
	return s.unwatch(eventloop.Writable)
}

// startReading passes incoming data to OnData and destroys the socket when the peer hangs up.
func (s *Socket) startReading() error {
	err := s.watch(eventloop.Readable, func(fd int, interest eventloop.Interest) error {
		var buf [4096]byte
		n, err := unix.Read(fd, buf[:])
		if err != nil {
			if errors.Is(err, unix.EAGAIN) || errors.Is(err, unix.EWOULDBLOCK) || errors.Is(err, unix.EINTR) {
				return nil
			}
			_ = s.Destroy()
			return nil
		}
		if n == 0 {
			// A zero-byte read means the peer closed the connection.
			_ = s.Destroy()
			return nil
		}
		if s.OnData != nil {
			return s.OnData(buf[:n])
		}
		return nil
	})
	if err != nil {
		_ = s.Destroy()
	}
	return err
}
