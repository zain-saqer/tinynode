// Package bindings exposes the runtime's native APIs (log, setTimeout, net)
// to JavaScript running in V8.
package bindings

import (
	"io"

	v8 "rogchap.com/v8go"

	"github.com/zain-saqer/tinynode/internal/eventloop"
)

// Install defines the runtime's globals in ctx. Their callbacks run on loop.
// log prints to stdout; the runtime's own warnings go to stderr.
func Install(iso *v8.Isolate, ctx *v8.Context, loop *eventloop.Loop, stdout, stderr io.Writer) error {
	global := ctx.Global()
	if err := global.Set("log", newLogFunc(iso, ctx, stdout)); err != nil {
		return err
	}
	if err := global.Set("setTimeout", newSetTimeoutFunc(iso, ctx, loop)); err != nil {
		return err
	}
	return installNet(iso, ctx, loop, stderr)
}
