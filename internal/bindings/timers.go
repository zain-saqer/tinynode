package bindings

import (
	"math"
	"time"

	v8 "rogchap.com/v8go"

	"github.com/zain-saqer/tinynode/internal/eventloop"
)

// maxTimeoutMs is the longest delay node accepts (2^31-1 ms, about 24.8 days).
const maxTimeoutMs = math.MaxInt32

// newSetTimeoutFunc builds setTimeout(callback, delay, ...args). Like node, it
// throws only when callback is not a function; a missing or invalid delay
// means 1ms, and extra arguments are passed to callback.
func newSetTimeoutFunc(iso *v8.Isolate, ctx *v8.Context, loop *eventloop.Loop) *v8.Function {
	setTimeoutFunc := v8.NewFunctionTemplate(iso, func(info *v8.FunctionCallbackInfo) *v8.Value {
		args := info.Args()
		if len(args) == 0 || !args[0].IsFunction() {
			return throwJsTypeError(iso, ctx, `The "callback" argument must be of type function`)
		}
		callback, _ := args[0].AsFunction()

		timeout := time.Millisecond
		if len(args) > 1 {
			timeout = delay(args[1])
		}

		var callbackArgs []v8.Valuer
		if len(args) > 2 {
			for _, arg := range args[2:] {
				callbackArgs = append(callbackArgs, arg)
			}
		}

		now := loop.Now()
		loop.AddTimer(&eventloop.Timer{
			CreatedAt: now,
			Timeout:   timeout,
			RunAt:     now + timeout,
			Repeat:    false,
			Callback: func() error {
				_, err := callback.Call(ctx.Global(), callbackArgs...)
				return err
			},
		})
		return nil
	})
	return setTimeoutFunc.GetFunction(ctx)
}

// delay converts a JS delay in milliseconds the way node does: it is coerced to
// a number, and anything that is not between 1 and maxTimeoutMs becomes 1.
func delay(v *v8.Value) time.Duration {
	ms := v.Number() // Number(v): "20" is 20, "abc" and undefined are NaN
	if !(ms >= 1 && ms <= maxTimeoutMs) {
		ms = 1
	}
	return time.Duration(ms * float64(time.Millisecond))
}
