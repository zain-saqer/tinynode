package main

import v8 "rogchap.com/v8go"

// throwJsError throws a JS Error with msg from inside a function callback.
func throwJsError(iso *v8.Isolate, ctx *v8.Context, msg string) *v8.Value {
	msgVal, _ := v8.NewValue(iso, msg)
	if ctor, err := ctx.Global().Get("Error"); err == nil && ctor.IsFunction() {
		fn, _ := ctor.AsFunction()
		if errObj, err := fn.Call(ctx.Global(), msgVal); err == nil {
			return iso.ThrowException(errObj)
		}
	}
	return iso.ThrowException(msgVal)
}
