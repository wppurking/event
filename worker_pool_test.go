package work

import (
	"bytes"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/assembla/cony"
	"github.com/json-iterator/go"
	"github.com/stretchr/testify/assert"
)

type tstCtx struct {
	a int
	bytes.Buffer
}

func (c *tstCtx) record(s string) {
	_, _ = c.WriteString(s)
}

var tstCtxType = reflect.TypeOf(tstCtx{})

func TestWorkerPoolHandlerValidations(t *testing.T) {
	var cases = []struct {
		fn   interface{}
		good bool
	}{
		{func(j *Message) error { return nil }, true},
		{func(c *tstCtx, j *Message) error { return nil }, true},
		{func(c *tstCtx, j *Message) {}, false},
		{func(c *tstCtx, j *Message) string { return "" }, false},
		{func(c *tstCtx, j *Message) (error, string) { return nil, "" }, false},
		{func(c *tstCtx) error { return nil }, false},
		{func(c tstCtx, j *Message) error { return nil }, false},
		{func() error { return nil }, false},
		{func(c *tstCtx, j *Message, wat string) error { return nil }, false},
	}

	for i, testCase := range cases {
		r := isValidHandlerType(tstCtxType, reflect.ValueOf(testCase.fn))
		if testCase.good != r {
			t.Errorf("idx %d: should return %v but returned %v", i, testCase.good, r)
		}
	}
}

func TestWorkerPoolMiddlewareValidations(t *testing.T) {
	var cases = []struct {
		fn   interface{}
		good bool
	}{
		{func(j *Message, n NextMiddlewareFunc) error { return nil }, true},
		{func(c *tstCtx, j *Message, n NextMiddlewareFunc) error { return nil }, true},
		{func(c *tstCtx, j *Message) error { return nil }, false},
		{func(c *tstCtx, j *Message, n NextMiddlewareFunc) {}, false},
		{func(c *tstCtx, j *Message, n NextMiddlewareFunc) string { return "" }, false},
		{func(c *tstCtx, j *Message, n NextMiddlewareFunc) (error, string) { return nil, "" }, false},
		{func(c *tstCtx, n NextMiddlewareFunc) error { return nil }, false},
		{func(c tstCtx, j *Message, n NextMiddlewareFunc) error { return nil }, false},
		{func() error { return nil }, false},
		{func(c *tstCtx, j *Message, wat string) error { return nil }, false},
		{func(c *tstCtx, j *Message, n NextMiddlewareFunc, wat string) error { return nil }, false},
	}

	for i, testCase := range cases {
		r := isValidMiddlewareType(tstCtxType, reflect.ValueOf(testCase.fn))
		if testCase.good != r {
			t.Errorf("idx %d: should return %v but returned %v", i, testCase.good, r)
		}
	}
}

func TestWorkerPoolStartStop(t *testing.T) {
	ns := "work"
	enq := NewEnqueuer(ns, cony.URL(""))
	wp := NewWorkerPool(TestContext{}, 10, ns, enq, cony.URL(""))
	wp.Start()
	wp.Start()
	wp.Stop()
	wp.Stop()
	wp.Start()
	wp.Stop()
}

func TestWorkerPoolValidations(t *testing.T) {
	ns := "work"
	enq := NewEnqueuer(ns, cony.URL(""))
	wp := NewWorkerPool(TestContext{}, 10, ns, enq, cony.URL(""))

	func() {
		defer func() {
			if panicErr := recover(); panicErr != nil {
				assert.Regexp(t, "Your middleware function can have one of these signatures", fmt.Sprintf("%v", panicErr))
			} else {
				t.Errorf("expected a panic when using bad middleware")
			}
		}()

		wp.Middleware(TestWorkerPoolValidations)
	}()

	func() {
		defer func() {
			if panicErr := recover(); panicErr != nil {
				assert.Regexp(t, "Your handler function can have one of these signatures", fmt.Sprintf("%v", panicErr))
			} else {
				t.Errorf("expected a panic when using a bad handler")
			}
		}()

		wp.Job("wat", TestWorkerPoolValidations)
	}()
}

func TestWorkersPoolRunSingleThreaded(t *testing.T) {
}

func TestWorkerPoolPauseSingleThreadedJobs(t *testing.T) {
}

// Test Helpers
func (t *TestContext) SleepyJob(job *Message) error {
	msg := Q{}
	jsoniter.Unmarshal(job.Body, &msg)
	sleepTime := time.Duration(msg["sleep"].(int))
	time.Sleep(sleepTime * time.Millisecond)
	return nil
}

/*
func setupTestWorkerPool(pool *redis.Pool, namespace, jobName string, concurrency int, jobOpts JobOptions) *WorkerPool {
	deleteQueue(pool, namespace, jobName)
	deleteRetryAndDead(pool, namespace)
	deletePausedAndLockedKeys(namespace, jobName, pool)

	wp := NewWorkerPool(TestContext{}, uint(concurrency), namespace, pool)
	wp.JobWithOptions(jobName, jobOpts, (*TestContext).SleepyJob)
	// reset the backoff times to help with testing
	sleepBackoffsInMilliseconds = []int64{10, 10, 10, 10, 10}
	return wp
}
*/
