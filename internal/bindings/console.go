package bindings

import (
	"fmt"
	"io"
	"strings"

	v8 "rogchap.com/v8go"
)

// newLogFunc builds log(...args), which prints to w.
func newLogFunc(iso *v8.Isolate, ctx *v8.Context, w io.Writer) *v8.Function {
	// Simple console logger
	logFunc := v8.NewFunctionTemplate(iso, func(info *v8.FunctionCallbackInfo) *v8.Value {
		// Like console.log: print every argument separated by spaces; no args prints an empty line.
		args := info.Args()
		parts := make([]string, len(args))
		for i, arg := range args {
			parts[i] = arg.String()
		}
		fmt.Fprintf(w, "[Console] %s\n", strings.Join(parts, " "))
		return nil
	})
	return logFunc.GetFunction(ctx)
}
