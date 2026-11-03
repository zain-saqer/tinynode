package netio

import (
	"fmt"
	"net"
	"strconv"

	"golang.org/x/sys/unix"

	"github.com/zain-saqer/tinynode/internal/eventloop"
)

type Server struct {
	Closed bool
	Socket *Socket
}

// Close stops listening. Connections already accepted stay open.
func (s *Server) Close() error {
	return s.Socket.Destroy()
}

func parseSockAddrInet4(addrStr string) (*unix.SockaddrInet4, error) {
	// 1. Separate host (IP) and port
	host, portStr, err := net.SplitHostPort(addrStr)
	if err != nil {
		return nil, fmt.Errorf("invalid address format: %w", err)
	}

	// 2. Parse and validate the port number
	port, err := strconv.Atoi(portStr)
	if err != nil || port < 0 || port > 65535 {
		return nil, fmt.Errorf("invalid port number: %s", portStr)
	}

	// 3. Parse and ensure it's a valid IPv4 address
	ip := net.ParseIP(host)
	if ip == nil {
		return nil, fmt.Errorf("invalid IP address: %s", host)
	}

	ip4 := ip.To4()
	if ip4 == nil {
		return nil, fmt.Errorf("not an IPv4 address: %s", host)
	}

	// 4. Construct SockaddrInet4
	sa := &unix.SockaddrInet4{
		Port: port,
	}
	copy(sa.Addr[:], ip4)

	return sa, nil
}

// ListenTCP listens on an IPv4 "host:port" address and calls onConnection with
// each accepted socket, already reading. An error from onConnection stops the loop.
func ListenTCP(loop *eventloop.Loop, address string, onConnection func(socket *Socket) error) (server *Server, err error) {
	fd, err := unix.Socket(unix.AF_INET, unix.SOCK_STREAM, 0)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			_ = unix.Close(fd)
		}
	}()
	server = &Server{Socket: newSocket(loop, fd)}

	if err := unix.SetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_REUSEADDR, 1); err != nil {
		return nil, err
	}

	addr, err := parseSockAddrInet4(address)
	if err != nil {
		return nil, err
	}

	if err := unix.Bind(fd, addr); err != nil {
		return nil, err
	}

	if err := unix.Listen(fd, 128); err != nil {
		return nil, err
	}

	err = server.Socket.watch(eventloop.Readable, func(serverFd int, interest eventloop.Interest) error {
		nfd, _, err := unix.Accept(serverFd)
		if err != nil {
			return nil
		}
		if err := unix.SetNonblock(nfd, true); err != nil {
			// A blocking fd would stall the whole loop on write, so refuse it.
			fmt.Printf("accept: set non-blocking: %v\n", err)
			_ = unix.Close(nfd)
			return nil
		}
		clientSocket := newSocket(loop, nfd)
		// Failing to set up one connection drops it; only callback errors stop the process.
		if err := clientSocket.startReading(); err != nil {
			fmt.Printf("failed to watch client socket: %v\n", err)
			return nil
		}
		return onConnection(clientSocket)
	})
	if err != nil {
		return nil, err
	}

	return server, nil
}
