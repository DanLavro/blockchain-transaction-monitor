package utils

import (
	"fmt"
	"runtime"
	"time"
)

type ErrorWithContext struct {
	Err       error
	Context   string
	Timestamp time.Time
	File      string
	Line      int
}

func (e *ErrorWithContext) Error() string {
	return fmt.Sprintf("[%s:%d] %s: %v", e.File, e.Line, e.Context, e.Err)
}

func (e *ErrorWithContext) Unwrap() error {
	return e.Err
}

func WrapError(err error, context string) error {
	if err == nil {
		return nil
	}

	_, file, line, ok := runtime.Caller(1)
	if !ok {
		file = "unknown"
		line = 0
	}

	return &ErrorWithContext{
		Err:       err,
		Context:   context,
		Timestamp: time.Now(),
		File:      file,
		Line:      line,
	}
}

type RetryConfig struct {
	MaxAttempts  int
	InitialDelay time.Duration
	MaxDelay     time.Duration
	Multiplier   float64
}

func DefaultRetryConfig() RetryConfig {
	return RetryConfig{
		MaxAttempts:  3,
		InitialDelay: 1 * time.Second,
		MaxDelay:     30 * time.Second,
		Multiplier:   2.0,
	}
}

func Retry(config RetryConfig, fn func() error) error {
	var lastErr error
	delay := config.InitialDelay

	for attempt := 1; attempt <= config.MaxAttempts; attempt++ {
		if err := fn(); err != nil {
			lastErr = err

			if attempt == config.MaxAttempts {
				return WrapError(lastErr, fmt.Sprintf("failed after %d attempts", config.MaxAttempts))
			}

			time.Sleep(delay)
			delay = time.Duration(float64(delay) * config.Multiplier)
			if delay > config.MaxDelay {
				delay = config.MaxDelay
			}
			continue
		}

		return nil
	}

	return lastErr
}

func SafeGo(fn func(), onPanic func(interface{})) {
	go func() {
		defer func() {
			if r := recover(); r != nil {
				if onPanic != nil {
					onPanic(r)
				}
			}
		}()
		fn()
	}()
}

func Debounce(delay time.Duration, fn func()) func() {
	var timer *time.Timer
	return func() {
		if timer != nil {
			timer.Stop()
		}
		timer = time.AfterFunc(delay, fn)
	}
}

func Throttle(interval time.Duration, fn func()) func() {
	var lastCall time.Time
	return func() {
		if time.Since(lastCall) >= interval {
			lastCall = time.Now()
			fn()
		}
	}
}
