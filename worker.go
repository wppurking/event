package work

import (
	"fmt"
	"math/rand"
	"reflect"
	"time"
)

type worker struct {
	workerID      string
	enqueuer      *Enqueuer
	poolID        string
	namespace     string
	consumerTypes map[string]*consumerType
	consumers     map[string]*consumer
	middleware    []*middlewareHandler
	contextType   reflect.Type

	stopChan         chan struct{}
	doneStoppingChan chan struct{}

	drainChan        chan struct{}
	doneDrainingChan chan struct{}
}

func newWorker(namespace string, poolID string, contextType reflect.Type,
	middleware []*middlewareHandler, enqueuer *Enqueuer,
	consumerTypes map[string]*consumerType, consumers map[string]*consumer) *worker {
	workerID := makeIdentifier()

	w := &worker{
		workerID:    workerID,
		enqueuer:    enqueuer,
		poolID:      poolID,
		namespace:   namespace,
		contextType: contextType,

		stopChan:         make(chan struct{}),
		doneStoppingChan: make(chan struct{}),

		drainChan:        make(chan struct{}),
		doneDrainingChan: make(chan struct{}),
	}

	w.updateMiddlewareAndConsumerTypes(middleware, consumerTypes, consumers)

	return w
}

// note: can't be called while the thing is started
func (w *worker) updateMiddlewareAndConsumerTypes(middleware []*middlewareHandler, consumerTypes map[string]*consumerType, consumers map[string]*consumer) {
	w.middleware = middleware
	w.consumerTypes = consumerTypes
	w.consumers = consumers
}

func (w *worker) start() {
	go w.loop()
}

func (w *worker) stop() {
	w.stopChan <- struct{}{}
	<-w.doneStoppingChan
}

func (w *worker) drain() {
	w.drainChan <- struct{}{}
	<-w.doneDrainingChan
}

//var sleepBackoffsInMilliseconds = []int64{0, 10, 100, 1000, 5000}
var sleepBackoffsInMilliseconds = []int64{0, 10, 100, 1000, 3000}

func (w *worker) loop() {
	var drained bool
	var consequtiveNoMsgs int64

	// Begin immediately. We'll change the duration on each tick with a timer.Reset()
	timer := time.NewTimer(0)
	defer timer.Stop()

	// 下面这一段对于 fetchMsg 的轮训代码很棒:
	// 既考虑了错处时的处理.
	// 也考虑了成功处理后的获取.
	// 也考虑到没有获取到任务的处理.
	for {
		select {
		case <-w.stopChan:
			w.doneStoppingChan <- struct{}{}
			return
		case <-w.drainChan:
			drained = true
			timer.Reset(0)
		case <-timer.C:
			msg, err := w.fetchMsg()
			if err != nil {
				logError("worker.fetch", err)
				timer.Reset(10 * time.Millisecond)
			} else if msg != nil {
				w.processMsg(msg)
				consequtiveNoMsgs = 0
				timer.Reset(0)
			} else {
				if drained {
					w.doneDrainingChan <- struct{}{}
					drained = false
				}
				consequtiveNoMsgs++
				idx := consequtiveNoMsgs
				if idx >= int64(len(sleepBackoffsInMilliseconds)) {
					idx = int64(len(sleepBackoffsInMilliseconds)) - 1
				}
				timer.Reset(time.Duration(sleepBackoffsInMilliseconds[idx]) * time.Millisecond)
			}
		}
	}
}

func (w *worker) fetchMsg() (*Message, error) {
	// resort queues
	// NOTE: we could optimize this to only resort every second, or something.
	for n, c := range w.consumers {
		if ct, ok := w.consumerTypes[n]; ok && ct.MaxConcurrency > 0 && ct.Runs() >= ct.MaxConcurrency {
			continue
		}
		msg, err := c.Peek()
		if err != nil {
			return nil, err
		}
		if msg != nil {
			return msg, nil
		}
	}
	return nil, nil
}

func (w *worker) processMsg(msg *Message) {
	if ct, ok := w.consumerTypes[msg.Name]; ok {
		ct.incr()
		// TODO 需要增加任务执行的 mertic
		_, runErr := handleMessage(msg, w.contextType, w.middleware, ct)
		if runErr != nil {
			w.addToRetryOrDead(ct, msg, runErr)
		}
		ct.decr()
		msg.Ack()
	} else {
		// NOTE: since we don't have a consumerType, we don't know max retries
		runErr := fmt.Errorf("stray msg: no handler")
		logError("process_msg.stray", runErr)
		w.addToDead(msg, runErr)
	}
}

func (w *worker) addToRetryOrDead(ct *consumerType, msg *Message, runErr error) {
	failsRemaining := int64(ct.MaxFails) - msg.Fails()
	if failsRemaining > 0 {
		w.addToRetry(msg, runErr)
	} else {
		if !ct.SkipDead {
			w.addToDead(msg, runErr)
		}
	}
}

func (w *worker) addToRetry(msg *Message, runErr error) {

	var backoff BackoffCalculator

	// Choose the backoff provider
	ct, ok := w.consumerTypes[msg.Name]
	if ok {
		backoff = ct.Backoff
	}

	if backoff == nil {
		backoff = defaultBackoffCalculator
	}
	err := w.enqueuer.EnqueueInMessage(msg, backoff(msg))
	if err != nil {
		logError("worker.add_to_retry", err)
	}
}

func (w *worker) addToDead(msg *Message, runErr error) {
	// TODO: 需要考虑如何解决死信队列的重新激活问题
	msg.Name = fmt.Sprintf("%s.%s", deadQueue, msg.Name)
	msg.nonPersistent = true
	err := w.enqueuer.EnqueueMessage(msg)
	if err != nil {
		logError("worker.add_to_dead.serialize", err)
	}
}

// Default algorithm returns an fastly increasing backoff counter which grows in an unbounded fashion
func defaultBackoffCalculator(msg *Message) int64 {
	fails := msg.Fails()
	return (fails * fails * fails * fails) + 15 + (rand.Int63n(30) * (fails + 1))
}
