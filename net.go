package main

import (
	"errors"

	"golang.org/x/sys/unix"
)

type Socket struct {
	Fd                int
	Closed            bool
	ClosedWhenDrained bool
	outBuf            []byte
	eventLoop         *EventLoop
	//todo: buffer data in case OnData cb is nil
	OnData func([]byte) error
	//todo: add OnClose and OnError callbacks
}

func NewSocket(el *EventLoop, fd int) *Socket {
	return &Socket{Fd: fd, eventLoop: el}
}

func (s *Socket) Destroy() error {
	if s.Closed {
		return nil
	}
	err := unix.Close(s.Fd)
	s.Closed = true
	if err != nil {
		return err
	}
	return nil
}

func (s *Socket) Close() error {
	if s.Closed {
		return nil
	}
	if len(s.outBuf) == 0 {
		return s.eventLoop.Unregister(s)
	}
	s.ClosedWhenDrained = true
	return nil
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
		el := s.eventLoop
		s.outBuf = append(s.outBuf, data[n:]...)
		err := el.Register(s, unix.EVFILT_WRITE, func(fd int, filter int16) error {
			err := s.flush()
			if err != nil || (s.ClosedWhenDrained && len(s.outBuf) == 0) {
				_ = el.Unregister(s)
			}
			return nil
		})
		if err != nil {
			// Can't wait for writability; the stream would be missing bytes, so drop the connection.
			s.outBuf = nil
			_ = el.Unregister(s)
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
				return nil // still full; wait for the next EVFILT_WRITE
			}
			return err
		}
		s.outBuf = s.outBuf[n:]
	}
	// Drained: stop watching for writability.
	s.outBuf = nil
	return s.eventLoop.RemoveFilter(s, unix.EVFILT_WRITE)
}

type Server struct {
	Closed bool
	Socket *Socket
}
