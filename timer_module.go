package main

import (
	"log"
	"time"

	v8 "rogchap.com/v8go"
)

type TimeoutCallback struct {
	Id        int
	CreatedAt time.Duration
	Timeout   time.Duration
	Repeat    bool
	RunAt     time.Duration
	Callback  func() error
}

func NewSetTimeoutJsFunc(iso *v8.Isolate, ctx *v8.Context, eventLoop *EventLoop) *v8.Function {
	setTimeoutFunc := v8.NewFunctionTemplate(iso, func(info *v8.FunctionCallbackInfo) *v8.Value {
		if len(info.Args()) != 2 {
			log.Println("Timeout function expects 2 arguments")
			return nil
		}
		if !info.Args()[0].IsFunction() {
			log.Println("Timeout function expects a function as the first argument")
			return nil
		}
		if !info.Args()[1].IsNumber() {
			log.Println("Timeout function expects a number as the second argument")
			return nil
		}
		callback, _ := info.Args()[0].AsFunction()

		// JS passes milliseconds; convert once here.
		timeout := time.Duration(info.Args()[1].Integer()) * time.Millisecond
		now := GetProcessElapsedTime()

		eventLoop.AddTimeoutCallback(&TimeoutCallback{
			CreatedAt: now,
			Timeout:   timeout,
			RunAt:     now + timeout,
			Repeat:    false,
			Callback: func() error {
				_, err := callback.Call(ctx.Global())
				return err
			},
		})
		return nil
	})
	return setTimeoutFunc.GetFunction(ctx)
}
