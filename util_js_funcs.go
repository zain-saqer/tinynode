package main

import (
	"fmt"

	v8 "rogchap.com/v8go"
)

func createLogJsFn(iso *v8.Isolate, ctx *v8.Context, eloop *EventLoop) *v8.Function {
	// Simple console logger
	logFunc := v8.NewFunctionTemplate(iso, func(info *v8.FunctionCallbackInfo) *v8.Value {
		fmt.Printf("[Console] %s\n", info.Args()[0].String())
		return nil
	})
	return logFunc.GetFunction(ctx)
}
