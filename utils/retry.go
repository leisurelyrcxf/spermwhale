package utils

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/leisurelyrcxf/spermwhale/utils/trace"

	"github.com/golang/glog"

	"github.com/leisurelyrcxf/spermwhale/errors"
)

var (
	RetryAnyError = func(_ error) bool {
		return true
	}
)

// WithRetry retries f() up to max number of times.
func WithRetry(interval, max int, f func(ctx context.Context) error) error {
	return WithContextRetry(context.Background(), interval, max, f)
}

func WithContextRetry(ctx context.Context, interval, max int, f func(ctx context.Context) error) error {
	return WithContextRetryEx(ctx, time.Second*time.Duration(interval), time.Second*time.Duration(interval*max), f, RetryAnyError)
}

func WithContextRetryEx(ctx context.Context, interval, timeout time.Duration, f func(ctx context.Context) error, isRetryable func(error) bool) error {
	return withContextRetryRaw(ctx, interval, timeout, false, f, isRetryable)
}

func WithSafeContextRetryEx(ctx context.Context, interval, timeout time.Duration, f func(ctx context.Context) error, isRetryable func(error) bool) error {
	return withContextRetryRaw(ctx, interval, timeout, true, f, isRetryable)
}

func withContextRetryRaw(ctx context.Context, interval, timeout time.Duration, safeCtx bool, f func(ctx context.Context) error, isRetryable func(error) bool) error {
	var (
		start  = time.Now()
		caller = getWithContextCaller()
	)

	timeoutCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	fCtx := timeoutCtx
	if safeCtx {
		fCtx = ctx
	}

	for {
		err := f(fCtx)
		if err == nil || !isRetryable(err) {
			if err != nil {
				if glog.V(1) {
					glog.Errorf("[withContextRetryRaw][%s] returned non-retryable error: '%v'", caller, err)
				}
			}
			return err
		}
		select {
		case <-timeoutCtx.Done():
			cost := time.Since(start)
			if glog.V(1) {
				glog.Errorf("[withContextRetryRaw][%s] failed after retrying for %s, last error: '%v'", caller, cost, err)
			}
			return errors.Annotatef(err, "failed after retrying for %s", cost)
		case <-time.After(interval):
			if glog.V(8) {
				glog.Warningf("[withContextRetryRaw][%s] run failed with error '%v', retrying after %s...", caller, err, interval)
			}
		}
	}
}

func getWithContextCaller() string {
	s := trace.TraceN(2, 6)
	for _, r := range s {
		if strings.Contains(r.Name, "utils.With") || strings.Contains(r.Name, "utils.with") {
			continue
		}
		r.Name = strings.ReplaceAll(r.Name, "github.com/leisurelyrcxf/spermwhale", "")
		if idx := strings.Index(r.File, "spermwhale/"); idx != -1 {
			r.File = r.File[idx+len("spermwhale/"):]
		}
		return fmt.Sprintf("%s(%s)", r.Name, r.File)
	}
	return "unknown"
}
