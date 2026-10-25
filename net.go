package main

import (
	"golang.org/x/sys/unix"
)

type Socket struct {
	Fd     int
	Closed bool
}

func (s *Socket) Close() error {
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

type Server struct {
	Closed bool
	Socket *Socket
}
