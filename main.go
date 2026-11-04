package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	v8 "rogchap.com/v8go"

	"github.com/zain-saqer/tinynode/internal/bindings"
	"github.com/zain-saqer/tinynode/internal/eventloop"
)

func main() {
	os.Exit(run(os.Args[1:]))
}

// run executes a script like `node <file>` and returns the process exit code.
func run(args []string) int {
	if len(args) < 1 {
		fmt.Fprintf(os.Stderr, "usage: %s <script.js>\n", filepath.Base(os.Args[0]))
		return 2
	}
	scriptPath := args[0]

	jsCode, err := os.ReadFile(scriptPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		return 1
	}

	eventLoop, err := eventloop.New()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		return 1
	}
	defer eventLoop.Close()

	iso := v8.NewIsolate()
	defer iso.Dispose()

	ctx := v8.NewContext(iso)
	defer ctx.Close()

	if err := bindings.Install(iso, ctx, eventLoop, os.Stdout, os.Stderr); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		return 1
	}

	// Run the script's top level, then keep going while it has timers or sockets.
	if _, err := ctx.RunScript(string(jsCode), scriptPath); err != nil {
		printJsError(err)
		return 1
	}

	// An exception thrown in a callback (setTimeout, onData, ...) is uncaught: stop like node does.
	if err := eventLoop.Run(); err != nil {
		printJsError(err)
		return 1
	}
	return 0
}

// printJsError prints an uncaught exception the way node does: the stack trace if there is one.
func printJsError(err error) {
	var jsErr *v8.JSError
	if errors.As(err, &jsErr) && jsErr.StackTrace != "" {
		fmt.Fprintln(os.Stderr, jsErr.StackTrace)
		return
	}
	fmt.Fprintln(os.Stderr, err)
}
