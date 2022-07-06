package keyval

import (
	"fmt"
	"runtime"
)

//
func errUndefinedEndpoint() error {
	var name string

	if pc, _, _, ok := runtime.Caller(1); ok {
		name = runtime.FuncForPC(pc).Name()
	}

	return fmt.Errorf("[%s] undefined storage endpoint", name)
}

//
func errInvalidEndpoint(url string) error {
	var name string

	if pc, _, _, ok := runtime.Caller(1); ok {
		name = runtime.FuncForPC(pc).Name()
	}

	return fmt.Errorf("[%s] invalid storage endpoint, missing storage name: %s", name, url)
}

//
func errUnsupportedEndpoint(url string) error {
	var name string

	if pc, _, _, ok := runtime.Caller(1); ok {
		name = runtime.FuncForPC(pc).Name()
	}

	return fmt.Errorf("[%s] unsupported storage schema: %s", name, url)
}
