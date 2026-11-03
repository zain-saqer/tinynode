package bindings

import (
	"fmt"

	v8 "rogchap.com/v8go"

	"github.com/zainsaqer/tinynode/internal/eventloop"
	"github.com/zainsaqer/tinynode/internal/netio"
)

// installNet defines the global `net` object.
func installNet(iso *v8.Isolate, ctx *v8.Context, loop *eventloop.Loop) error {
	obj, err := v8.NewObjectTemplate(iso).NewInstance(ctx)
	if err != nil {
		return err
	}
	if err := obj.Set("createServer", newCreateServerFunc(iso, ctx, loop)); err != nil {
		return err
	}
	return ctx.Global().Set("net", obj)
}

// newCreateServerFunc builds net.createServer(address, (socket) => {...}).
func newCreateServerFunc(iso *v8.Isolate, ctx *v8.Context, loop *eventloop.Loop) *v8.Function {
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
		server, err := netio.ListenTCP(loop, address, func(socket *netio.Socket) error {
			jsSocket, err := newSocketObject(iso, ctx, socket)
			if err != nil {
				fmt.Printf("failed to create socket object: %v\n", err)
				_ = socket.Destroy()
				return nil
			}
			_, err = callback.Call(ctx.Global(), jsSocket)
			return err
		})
		if err != nil {
			return throwJsError(iso, ctx, fmt.Sprintf("createServer: %v", err))
		}
		jsServer, err := newServerObject(iso, ctx, server)
		if err != nil {
			return throwJsError(iso, ctx, fmt.Sprintf("failed to create server object: %v", err))
		}
		return jsServer.Value
	})
	return createServerFunc.GetFunction(ctx)
}

func newServerObject(iso *v8.Isolate, ctx *v8.Context, server *netio.Server) (*v8.Object, error) {
	closeFn := v8.NewFunctionTemplate(iso, func(info *v8.FunctionCallbackInfo) *v8.Value {
		if err := server.Close(); err != nil {
			return throwJsError(iso, ctx, fmt.Sprintf("socket.close: %v", err))
		}
		return nil
	})

	serverTemplate := v8.NewObjectTemplate(iso)
	if err := serverTemplate.Set("close", closeFn); err != nil {
		return nil, err
	}
	return serverTemplate.NewInstance(ctx)
}

// newSocketObject builds the JS `socket` object handed to the createServer callback.
func newSocketObject(iso *v8.Isolate, ctx *v8.Context, clientSocket *netio.Socket) (*v8.Object, error) {
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
		flushed, _ := v8.NewValue(iso, clientSocket.Buffered() == 0)
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
		clientSocket.OnData = func(data []byte) error {
			chunk, err := v8.NewValue(iso, string(data))
			if err != nil {
				return err
			}
			_, err = dataCallback.Call(ctx.Global(), chunk)
			return err
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
