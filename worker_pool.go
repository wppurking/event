package work

import (
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"

	"github.com/assembla/cony"
	"github.com/streadway/amqp"
)

// WorkerPool represents a pool of workers. It forms the primary API of gocraft/work. WorkerPools provide the public API of gocraft/work. You can attach jobs and middlware to them. You can start and stop them. Based on their concurrency setting, they'll spin up N worker goroutines.
type WorkerPool struct {
	workerPoolID string
	concurrency  uint
	namespace    string // eg, "myapp-work"
	cli          *cony.Client
	defaultExc   cony.Exchange
	scheduleExc  cony.Exchange
	enqueuer     *Enqueuer // workPool 内部的消息发送器

	contextType   reflect.Type
	consumerTypes map[string]*consumerType
	consumers     map[string]*consumer
	middleware    []*middlewareHandler
	started       bool

	opts    []cony.ClientOpt // 记录下 cony.Client 需要的参数
	workers []*worker
	//heartbeater      *workerPoolHeartbeater
}

// You may provide your own backoff function for retrying failed jobs or use the builtin one.
// Returns the number of seconds to wait until the next attempt.
//
// The builtin backoff calculator provides an exponentially increasing wait function.
type BackoffCalculator func(msg *Message) int64

// NewWorkerPool creates a new worker pool. ctx should be a struct literal whose type will be used for middleware and handlers.
// concurrency specifies how many workers to spin up - each worker can process jobs concurrently.
// 期望 cli 与 enqueuer 是两个 connection, 避免并发时候的竞争
func NewWorkerPool(ctx interface{}, concurrency uint, namespace string, enqueuer *Enqueuer, opts ...cony.ClientOpt) *WorkerPool {
	if enqueuer == nil {
		panic("NewWorkerPool needs a non-nil *Enqueuer")
	}
	// 默认的 cony 配置, url, backoff
	defaultOpts := buildDefaultOpt()
	if len(opts) == 0 {
		panic("请输入正确的参数")
	}
	defaultOpts = append(defaultOpts, opts...)

	ctxType := reflect.TypeOf(ctx)
	validateContextType(ctxType)
	wp := &WorkerPool{
		workerPoolID:  makeIdentifier(),
		concurrency:   concurrency,
		namespace:     namespace,
		enqueuer:      enqueuer,
		opts:          defaultOpts,
		contextType:   ctxType,
		consumerTypes: make(map[string]*consumerType),
		consumers:     make(map[string]*consumer),
	}
	wp.cli = cony.NewClient(wp.opts...)
	wp.defaultExc = cony.Exchange{Name: withNS(wp.namespace, "work"), AutoDelete: false, Durable: true, Kind: "topic"}
	wp.scheduleExc = cony.Exchange{Name: withNS(wp.namespace, "work.schedule"), AutoDelete: false, Durable: true, Kind: "topic"}
	builtinQueue(wp.namespace, wp.defaultExc, wp.scheduleExc, wp.cli)

	for i := uint(0); i < wp.concurrency; i++ {
		w := newWorker(wp.namespace, wp.workerPoolID, wp.contextType,
			nil, enqueuer,
			wp.consumerTypes, wp.consumers)
		wp.workers = append(wp.workers, w)
	}
	return wp
}

// Middleware appends the specified function to the middleware chain. The fn can take one of these forms:
// (*ContextType).func(*Message, NextMiddlewareFunc) error, (ContextType matches the type of ctx specified when creating a pool)
// func(*Message, NextMiddlewareFunc) error, for the generic middleware format.
func (wp *WorkerPool) Middleware(fn interface{}) *WorkerPool {
	vfn := reflect.ValueOf(fn)
	validateMiddlewareType(wp.contextType, vfn)

	mw := &middlewareHandler{
		DynamicMiddleware: vfn,
	}

	if gmh, ok := fn.(func(*Message, NextMiddlewareFunc) error); ok {
		mw.IsGeneric = true
		mw.GenericMiddlewareHandler = gmh
	}

	wp.middleware = append(wp.middleware, mw)

	for _, w := range wp.workers {
		w.updateMiddlewareAndJobTypes(wp.middleware, wp.consumerTypes, wp.consumers)
	}

	return wp
}

// Message registers the job name to the specified handler fn. For instance, when workers pull jobs from the name queue they'll be processed by the specified handler function.
// fn can take one of these forms:
// (*ContextType).func(*Message) error, (ContextType matches the type of ctx specified when creating a pool)
// func(*Message) error, for the generic handler format.
func (wp *WorkerPool) Consumer(name string, fn interface{}) *WorkerPool {
	return wp.ConsumerWithOptions(name, ConsumerOptions{}, fn)
}

// ConsumerWithOptions adds a handler for 'name' jobs as per the Message function, but permits you specify additional options
// such as a job's priority, retry count, and whether to send dead jobs to the dead job queue or trash them.
// name: 大小写不敏感
func (wp *WorkerPool) ConsumerWithOptions(name string, jobOpts ConsumerOptions, fn interface{}) *WorkerPool {
	jobOpts = applyDefaultsAndValidate(jobOpts)

	n := strings.ToLower(name)
	vfn := reflect.ValueOf(fn)
	validateHandlerType(wp.contextType, vfn)
	jt := &consumerType{
		Name:            n,
		DynamicHandler:  vfn,
		ConsumerOptions: jobOpts,
	}
	if gh, ok := fn.(func(*Message) error); ok {
		jt.IsGeneric = true
		jt.GenericHandler = gh
	}

	wp.consumerTypes[n] = jt
	wp.consumers[n] = newConsumer(wp.namespace, jt, wp.defaultExc)

	for _, w := range wp.workers {
		w.updateMiddlewareAndJobTypes(wp.middleware, wp.consumerTypes, wp.consumers)
	}

	return wp
}

// Start starts the workers and associated processes.
func (wp *WorkerPool) Start() {
	if wp.started {
		return
	}
	wp.started = true

	// TODO: we should cleanup stale keys on startup from previously registered jobs
	for _, cn := range wp.consumers {
		cn.start(wp.cli)
	}
	go wp.observerDeclears()
	for _, w := range wp.workers {
		go w.start()
	}
}

// Stop stops the workers and associated processes.
func (wp *WorkerPool) Stop() {
	if !wp.started {
		return
	}
	wp.started = false

	wg := sync.WaitGroup{}
	for _, w := range wp.workers {
		wg.Add(1)
		go func(w *worker) {
			w.stop()
			wg.Done()
		}(w)
	}
	wp.cli.Close()
	wg.Wait()
	// 重新设置 Client, 等待重新连接
	wp.cli = cony.NewClient(wp.opts...)
}

// Drain drains all jobs in the queue before returning. Note that if jobs are added faster than we can process them, this function wouldn't return.
func (wp *WorkerPool) Drain() {
	wg := sync.WaitGroup{}
	for _, w := range wp.workers {
		wg.Add(1)
		go func(w *worker) {
			w.drain()
			wg.Done()
		}(w)
	}
	wg.Wait()
}

func (wp *WorkerPool) workerIDs() []string {
	wids := make([]string, 0, len(wp.workers))
	for _, w := range wp.workers {
		wids = append(wids, w.workerID)
	}
	sort.Strings(wids)
	return wids
}

// observerDeclears 启动并监控 cony 的 rabbitmq 连接
// 当 Client.Close 的时候, 如果是 nil, 那么则退出, 否则进行重连重试
func (wp *WorkerPool) observerDeclears() {
	// 开始监控, 在 RabbitMQ 连接断开的时候重连
	for wp.cli.Loop() {
		select {
		case err := <-wp.cli.Errors():
			e, ok := err.(*amqp.Error)
			if ok && e == nil {
				fmt.Printf("停止客户端, 退出 loop: %v\n", e)
				return
			} else {
				fmt.Println("err:", err)
			}
		}
	}
}

// validateContextType will panic if context is invalid
func validateContextType(ctxType reflect.Type) {
	if ctxType.Kind() != reflect.Struct {
		panic("work: Context needs to be a struct type")
	}
}

func validateHandlerType(ctxType reflect.Type, vfn reflect.Value) {
	if !isValidHandlerType(ctxType, vfn) {
		panic(instructiveMessage(vfn, "a handler", "handler", "job *work.Message", ctxType))
	}
}

func validateMiddlewareType(ctxType reflect.Type, vfn reflect.Value) {
	if !isValidMiddlewareType(ctxType, vfn) {
		panic(instructiveMessage(vfn, "middleware", "middleware", "job *work.Message, next NextMiddlewareFunc", ctxType))
	}
}

// Since it's easy to pass the wrong method as a middleware/handler, and since the user can't rely on static type checking since we use reflection,
// lets be super helpful about what they did and what they need to do.
// Arguments:
//  - vfn is the failed method
//  - addingType is for "You are adding {addingType} to a worker pool...". Eg, "middleware" or "a handler"
//  - yourType is for "Your {yourType} function can have...". Eg, "middleware" or "handler" or "error handler"
//  - args is like "rw web.ResponseWriter, req *web.Request, next web.NextMiddlewareFunc"
//    - NOTE: args can be calculated if you pass in each type. BUT, it doesn't have example argument name, so it has less copy/paste value.
func instructiveMessage(vfn reflect.Value, addingType string, yourType string, args string, ctxType reflect.Type) string {
	// Get context type without package.
	ctxString := ctxType.String()
	splitted := strings.Split(ctxString, ".")
	if len(splitted) <= 1 {
		ctxString = splitted[0]
	} else {
		ctxString = splitted[1]
	}

	str := "\n" + strings.Repeat("*", 120) + "\n"
	str += "* You are adding " + addingType + " to a worker pool with context type '" + ctxString + "'\n"
	str += "*\n*\n"
	str += "* Your " + yourType + " function can have one of these signatures:\n"
	str += "*\n"
	str += "* // If you don't need context:\n"
	str += "* func YourFunctionName(" + args + ") error\n"
	str += "*\n"
	str += "* // If you want your " + yourType + " to accept a context:\n"
	str += "* func (c *" + ctxString + ") YourFunctionName(" + args + ") error  // or,\n"
	str += "* func YourFunctionName(c *" + ctxString + ", " + args + ") error\n"
	str += "*\n"
	str += "* Unfortunately, your function has this signature: " + vfn.Type().String() + "\n"
	str += "*\n"
	str += strings.Repeat("*", 120) + "\n"

	return str
}

func isValidHandlerType(ctxType reflect.Type, vfn reflect.Value) bool {
	fnType := vfn.Type()

	if fnType.Kind() != reflect.Func {
		return false
	}

	numIn := fnType.NumIn()
	numOut := fnType.NumOut()

	if numOut != 1 {
		return false
	}

	outType := fnType.Out(0)
	var e *error

	if outType != reflect.TypeOf(e).Elem() {
		return false
	}

	var j *Message
	if numIn == 1 {
		if fnType.In(0) != reflect.TypeOf(j) {
			return false
		}
	} else if numIn == 2 {
		if fnType.In(0) != reflect.PtrTo(ctxType) {
			return false
		}
		if fnType.In(1) != reflect.TypeOf(j) {
			return false
		}
	} else {
		return false
	}

	return true
}

func isValidMiddlewareType(ctxType reflect.Type, vfn reflect.Value) bool {
	fnType := vfn.Type()

	if fnType.Kind() != reflect.Func {
		return false
	}

	numIn := fnType.NumIn()
	numOut := fnType.NumOut()

	if numOut != 1 {
		return false
	}

	outType := fnType.Out(0)
	var e *error

	if outType != reflect.TypeOf(e).Elem() {
		return false
	}

	var j *Message
	var nfn NextMiddlewareFunc
	if numIn == 2 {
		if fnType.In(0) != reflect.TypeOf(j) {
			return false
		}
		if fnType.In(1) != reflect.TypeOf(nfn) {
			return false
		}
	} else if numIn == 3 {
		if fnType.In(0) != reflect.PtrTo(ctxType) {
			return false
		}
		if fnType.In(1) != reflect.TypeOf(j) {
			return false
		}
		if fnType.In(2) != reflect.TypeOf(nfn) {
			return false
		}
	} else {
		return false
	}

	return true
}

func applyDefaultsAndValidate(consumerOpts ConsumerOptions) ConsumerOptions {
	if consumerOpts.Priority == 0 {
		consumerOpts.Priority = 1
	}

	if consumerOpts.MaxFails == 0 {
		consumerOpts.MaxFails = 4
	}

	if consumerOpts.Priority > 100000 {
		panic("work: ConsumerOptions.Priority must be between 1 and 100000")
	}

	return consumerOpts
}
