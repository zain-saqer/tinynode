package main

import (
	"errors"
	"fmt"
	"net"
	"strconv"

	"golang.org/x/sys/unix"
	v8 "rogchap.com/v8go"
)

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

func ListenTCP(el *EventLoop, address string, onConnection func(socket *Socket)) (server *Server, err error) {
	fd, err := unix.Socket(unix.AF_INET, unix.SOCK_STREAM, 0)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			_ = unix.Close(fd)
		}
	}()
	server = &Server{Socket: NewSocket(el, fd)}

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

	err = el.Register(server.Socket, unix.EVFILT_READ, func(serverFd int, filter int16) {
		nfd, _, err := unix.Accept(serverFd)
		if err != nil {
			return
		}
		if err := unix.SetNonblock(nfd, true); err != nil {
			// A blocking fd would stall the whole loop on write, so refuse it.
			fmt.Printf("accept: set non-blocking: %v\n", err)
			_ = unix.Close(nfd)
			return
		}
		clientSocket := NewSocket(el, nfd)
		onConnection(clientSocket)
	})
	if err != nil {
		return nil, err
	}

	return server, nil
}

func registerClientReadHandler(el *EventLoop, socket *Socket) error {
	err := el.Register(socket, unix.EVFILT_READ, func(fd int, filter int16) {
		var buf [4096]byte
		n, err := unix.Read(fd, buf[:])
		if err != nil {
			if errors.Is(err, unix.EAGAIN) || errors.Is(err, unix.EWOULDBLOCK) || errors.Is(err, unix.EINTR) {
				return
			}
			el.Unregister(socket)
			return
		}
		if n == 0 {
			// A zero-byte read means the peer closed the connection.
			el.Unregister(socket)
			return
		}
		if socket.OnData != nil {
			socket.OnData(buf[:n])
		}
	})
	if err != nil {
		_ = el.Unregister(socket)
	}
	return err
}

func createServerJsFunc(iso *v8.Isolate, ctx *v8.Context, eventLoop *EventLoop) *v8.Function {
	createServerFunc := v8.NewFunctionTemplate(iso, func(info *v8.FunctionCallbackInfo) *v8.Value {
		args := info.Args()
		if len(args) < 2 {
			fmt.Println("Invalid arguments for createServer")
			return nil
		}
		if !args[0].IsString() {
			fmt.Println("Invalid arguments for createServer")
			return nil
		}
		if !args[1].IsFunction() {
			fmt.Println("Invalid arguments for createServer")
			return nil
		}
		callback, err := args[1].AsFunction()
		if err != nil {
			fmt.Println("Invalid arguments for createServer")
			return nil
		}

		address := args[0].String()
		server, err := ListenTCP(eventLoop, address, func(socket *Socket) {
			if err := registerClientReadHandler(eventLoop, socket); err != nil {
				fmt.Printf("failed to watch client socket: %v\n", err)
				return
			}
			jsSocket, err := newSocketObject(iso, ctx, eventLoop, socket)
			if err != nil {
				fmt.Printf("failed to create socket object: %v\n", err)
				eventLoop.Unregister(socket)
				return
			}
			if _, err := callback.Call(ctx.Global(), jsSocket); err != nil {
				fmt.Printf("createServer callback failed: %v\n", err)
			}
		})
		if err != nil {
			return throwJsError(iso, ctx, fmt.Sprintf("createServer: %v", err))
		}
		jsServer, err := newServerObject(iso, ctx, eventLoop, server)
		if err != nil {
			return throwJsError(iso, ctx, fmt.Sprintf("failed to create server object: %v", err))
		}
		return jsServer.Value
	})
	return createServerFunc.GetFunction(ctx)
}

func newServerObject(iso *v8.Isolate, ctx *v8.Context, eventLoop *EventLoop, server *Server) (*v8.Object, error) {
	closeFn := v8.NewFunctionTemplate(iso, func(info *v8.FunctionCallbackInfo) *v8.Value {
		err := eventLoop.Unregister(server.Socket)
		if err != nil {
			return throwJsError(iso, ctx, fmt.Sprintf("socket.close: %v", err))
		}
		return nil
	})

	socketTemplate := v8.NewObjectTemplate(iso)
	if err := socketTemplate.Set("close", closeFn); err != nil {
		return nil, err
	}
	return socketTemplate.NewInstance(ctx)
}

// newSocketObject builds the JS `socket` object handed to the createServer callback.
func newSocketObject(iso *v8.Isolate, ctx *v8.Context, eventLoop *EventLoop, clientSocket *Socket) (*v8.Object, error) {
	writeFn := v8.NewFunctionTemplate(iso, func(info *v8.FunctionCallbackInfo) *v8.Value {
		if clientSocket.Closed {
			return throwJsError(iso, ctx, "socket is closed")
		}
		args := info.Args()
		if len(args) < 1 || !args[0].IsString() {
			return throwJsError(iso, ctx, "socket.write: invalid arguments")
		}
		if err := clientSocket.Write([]byte(args[0].String())); err != nil {
			return throwJsError(iso, ctx, fmt.Sprintf("socket.write: %v", err))
		}
		// Like Node: true if everything was handed to the kernel, false if some is queued.
		flushed, _ := v8.NewValue(iso, len(clientSocket.outBuf) == 0)
		return flushed
	})

	// socket.onData((data) => {...})
	onDataFn := v8.NewFunctionTemplate(iso, func(info *v8.FunctionCallbackInfo) *v8.Value {
		args := info.Args()
		if len(args) < 1 || !args[0].IsFunction() {
			return throwJsError(iso, ctx, "socket.onData: invalid arguments")
		}
		dataCallback, err := args[0].AsFunction()
		if err != nil {
			return throwJsError(iso, ctx, fmt.Sprintf("socket.onData: %v", err))
		}
		clientSocket.OnData = func(data []byte) {
			chunk, err := v8.NewValue(iso, string(data))
			if err != nil {
				return
			}
			_, err = dataCallback.Call(ctx.Global(), chunk)
			if err != nil {
				fmt.Println("Failed to call onData callback", err)
				return
			}
		}
		return nil
	})

	// socket.close()
	closeFn := v8.NewFunctionTemplate(iso, func(info *v8.FunctionCallbackInfo) *v8.Value {
		err := clientSocket.Close()
		if err != nil {
			return throwJsError(iso, ctx, fmt.Sprintf("socket.close: %v", err))
		}
		return nil
	})
	// socket.isClosed()
	isClosedFn := v8.NewFunctionTemplate(iso, func(info *v8.FunctionCallbackInfo) *v8.Value {
		v, _ := v8.NewValue(iso, clientSocket.Closed || clientSocket.ClosedWhenDrained)
		return v
	})
	socketTemplate := v8.NewObjectTemplate(iso)
	if err := socketTemplate.Set("write", writeFn); err != nil {
		return nil, err
	}
	if err := socketTemplate.Set("onData", onDataFn); err != nil {
		return nil, err
	}
	if err := socketTemplate.Set("close", closeFn); err != nil {
		return nil, err
	}
	if err := socketTemplate.Set("isClosed", isClosedFn); err != nil {
		return nil, err
	}
	return socketTemplate.NewInstance(ctx)
}

func InstallNetModule(iso *v8.Isolate, ctx *v8.Context, eventLoop *EventLoop) (*v8.Object, error) {
	networkObject := v8.NewObjectTemplate(ctx.Isolate())
	obj, err := networkObject.NewInstance(ctx)
	if err != nil {
		return nil, err
	}
	err = obj.Set("createServer", createServerJsFunc(iso, ctx, eventLoop))
	if err != nil {
		return nil, err
	}
	err = ctx.Global().Set("net", obj)
	if err != nil {
		return nil, err
	}
	return obj, nil
}
