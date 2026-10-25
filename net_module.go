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

func ListenTCP(el *EventLoop, address string, onConnection func(clientFd int)) error {
	fd, err := unix.Socket(unix.AF_INET, unix.SOCK_STREAM, 0)
	if err != nil {
		return err
	}
	defer unix.Close(fd)

	if err := unix.SetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_REUSEADDR, 1); err != nil {
		return err
	}

	addr, err := parseSockAddrInet4(address)
	if err != nil {
		return err
	}

	if err := unix.Bind(fd, addr); err != nil {
		return err
	}

	if err := unix.Listen(fd, 128); err != nil {
		return err
	}

	return el.Register(fd, unix.EVFILT_READ, func(serverFd int, filter int16) {
		nfd, _, err := unix.Accept(serverFd)
		if err != nil {
			return
		}
		onConnection(nfd)
	})
}

func registerClientReadHandler(el *EventLoop, clientFd int, callback func(data []byte)) error {
	err := el.Register(clientFd, unix.EVFILT_READ, func(fd int, filter int16) {
		var buf [4096]byte
		n, err := unix.Read(fd, buf[:])
		if err != nil {
			if errors.Is(err, unix.EAGAIN) || errors.Is(err, unix.EWOULDBLOCK) || errors.Is(err, unix.EINTR) {
				return
			}
			el.Unregister(fd)
			return
		}
		if n == 0 {
			// A zero-byte read means the peer closed the connection.
			el.Unregister(fd)
			return
		}
		if callback != nil {
			callback(buf[:n])
		}
	})
	if err != nil {
		_ = unix.Close(clientFd)
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
		err = ListenTCP(eventLoop, address, func(clientFd int) {
			socket, err := newSocketObject(iso, ctx, eventLoop, clientFd)
			if err != nil {
				fmt.Printf("failed to create socket object: %v\n", err)
				eventLoop.Unregister(clientFd)
				return
			}
			if _, err := callback.Call(ctx.Global(), socket); err != nil {
				fmt.Printf("createServer callback failed: %v\n", err)
			}
		})
		if err != nil {
			fmt.Printf("createServer failed: %v\n", err)
		}
		return nil
	})
	return createServerFunc.GetFunction(ctx)
}

// newSocketObject builds the JS `socket` object handed to the createServer callback.
func newSocketObject(iso *v8.Isolate, ctx *v8.Context, eventLoop *EventLoop, clientFd int) (*v8.Object, error) {
	// socket.write(data)
	writeFn := v8.NewFunctionTemplate(iso, func(info *v8.FunctionCallbackInfo) *v8.Value {
		args := info.Args()
		if len(args) < 1 {
			return nil
		}
		_, _ = unix.Write(clientFd, []byte(args[0].String()))
		return nil
	})

	// socket.onData((data) => {...})
	onDataFn := v8.NewFunctionTemplate(iso, func(info *v8.FunctionCallbackInfo) *v8.Value {
		args := info.Args()
		if len(args) < 1 || !args[0].IsFunction() {
			fmt.Println("Invalid arguments for socket.onData")
			return nil
		}
		dataCallback, err := args[0].AsFunction()
		if err != nil {
			fmt.Println("Invalid arguments for socket.onData", err)
			return nil
		}
		err = registerClientReadHandler(eventLoop, clientFd, func(data []byte) {
			chunk, err := v8.NewValue(iso, string(data))
			if err != nil {
				return
			}
			_, err = dataCallback.Call(ctx.Global(), chunk)
			if err != nil {
				fmt.Println("Failed to call onData callback", err)
				return
			}
		})
		if err != nil {
			fmt.Println("Failed to register client read handler", err)
			return nil
		}
		return nil
	})

	// socket.close()
	closeFn := v8.NewFunctionTemplate(iso, func(info *v8.FunctionCallbackInfo) *v8.Value {
		eventLoop.Unregister(clientFd)
		return nil
	})

	socketTemplate := v8.NewObjectTemplate(iso)
	if err := socketTemplate.Set("fd", int32(clientFd)); err != nil {
		return nil, err
	}
	if err := socketTemplate.Set("write", writeFn); err != nil {
		return nil, err
	}
	if err := socketTemplate.Set("onData", onDataFn); err != nil {
		return nil, err
	}
	if err := socketTemplate.Set("close", closeFn); err != nil {
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
