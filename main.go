package main

import (
	"fmt"
	"log"
	"os"

	v8 "rogchap.com/v8go"
)

func main() {
	eventLoop, err := NewEventLoop()
	if err != nil {
		log.Fatal(err.Error())
	}
	iso := v8.NewIsolate()
	defer iso.Dispose()

	ctx := v8.NewContext(iso)
	defer ctx.Close()

	global := ctx.Global()

	logFunc := createLogJsFn(iso, ctx, eventLoop)
	_ = global.Set("log", logFunc)

	// Timeout
	setTimeoutFunc := NewSetTimeoutJsFunc(iso, ctx, eventLoop)
	_ = global.Set("setTimeout", setTimeoutFunc)

	// Net
	if _, err := InstallNetModule(iso, ctx, eventLoop); err != nil {
		log.Fatal(err.Error())
	}

	// 3. Execute the multi-await async IIFE
	jsCode, err := os.ReadFile(`main.js`)
	if err != nil {
		log.Fatal(err.Error())
	}

	_, err = ctx.RunScript(string(jsCode), "main.js")
	if err != nil {
		log.Fatalf("RunScript failed: %v", err)
	}

	eventLoop.Run()

	fmt.Println("Done")
}
