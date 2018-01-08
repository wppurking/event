package work

import (
	"fmt"
	"math/rand"
	"reflect"
	"time"

	"github.com/garyburd/redigo/redis"
)

const fetchKeysPerJobType = 6

type worker struct {
	workerID    string
	poolID      string
	namespace   string
	jobTypes    map[string]*jobType
	consumers   map[string]*consumer
	middleware  []*middlewareHandler
	contextType reflect.Type

	redisFetchScript *redis.Script

	stopChan         chan struct{}
	doneStoppingChan chan struct{}

	drainChan        chan struct{}
	doneDrainingChan chan struct{}
}

func newWorker(namespace string, poolID string, contextType reflect.Type, middleware []*middlewareHandler, jobTypes map[string]*jobType, consumers map[string]*consumer) *worker {
	workerID := makeIdentifier()

	w := &worker{
		workerID:    workerID,
		poolID:      poolID,
		namespace:   namespace,
		contextType: contextType,

		stopChan:         make(chan struct{}),
		doneStoppingChan: make(chan struct{}),

		drainChan:        make(chan struct{}),
		doneDrainingChan: make(chan struct{}),
	}

	w.updateMiddlewareAndJobTypes(middleware, jobTypes, consumers)

	return w
}

// note: can't be called while the thing is started
func (w *worker) updateMiddlewareAndJobTypes(middleware []*middlewareHandler, jobTypes map[string]*jobType, consumers map[string]*consumer) {
	w.middleware = middleware
	w.jobTypes = jobTypes
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

var sleepBackoffsInMilliseconds = []int64{0, 10, 100, 1000, 5000}

func (w *worker) loop() {
	var drained bool
	var consequtiveNoJobs int64

	// Begin immediately. We'll change the duration on each tick with a timer.Reset()
	timer := time.NewTimer(0)
	defer timer.Stop()

	for {
		select {
		case <-w.stopChan:
			w.doneStoppingChan <- struct{}{}
			return
		case <-w.drainChan:
			drained = true
			timer.Reset(0)
		case <-timer.C:
			job, err := w.fetchJob()
			if err != nil {
				logError("worker.fetch", err)
				timer.Reset(10 * time.Millisecond)
			} else if job != nil {
				w.processJob(job)
				consequtiveNoJobs = 0
				timer.Reset(0)
			} else {
				if drained {
					w.doneDrainingChan <- struct{}{}
					drained = false
				}
				consequtiveNoJobs++
				idx := consequtiveNoJobs
				if idx >= int64(len(sleepBackoffsInMilliseconds)) {
					idx = int64(len(sleepBackoffsInMilliseconds)) - 1
				}
				timer.Reset(time.Duration(sleepBackoffsInMilliseconds[idx]) * time.Millisecond)
			}
		}
	}
}

func (w *worker) fetchJob() (*Job, error) {
	// resort queues
	// NOTE: we could optimize this to only resort every second, or something.

	// TODO: 控制任务的并发
	for _, c := range w.consumers {
		job, err := c.Peek()
		if err != nil {
			return nil, err
		}
		if job != nil {
			return job, nil
		}
	}
	return nil, nil
}

func (w *worker) processJob(job *Job) {
	if job.Unique {
		w.deleteUniqueJob(job)
	}
	if jt, ok := w.jobTypes[job.Name]; ok {
		// TODO 需要增加任务执行的 mertic
		_, runErr := runJob(job, w.contextType, w.middleware, jt)
		if runErr != nil {
			job.failed(runErr)
			w.addToRetryOrDead(jt, job, runErr)
		} else {
			job.msg.Ack(false)
			w.removeJobFromInProgress(job)
		}
	} else {
		// NOTE: since we don't have a jobType, we don't know max retries
		runErr := fmt.Errorf("stray job: no handler")
		logError("process_job.stray", runErr)
		job.failed(runErr)
		w.addToDead(job, runErr)
	}
}

func (w *worker) deleteUniqueJob(job *Job) {
	// TODO 这个暂时无法实现
}

func (w *worker) removeJobFromInProgress(job *Job) {
	// TODO: 删除任务的执行状态
}

func (w *worker) addToRetryOrDead(jt *jobType, job *Job, runErr error) {
	failsRemaining := int64(jt.MaxFails) - job.Fails
	if failsRemaining > 0 {
		w.addToRetry(job, runErr)
	} else {
		if !jt.SkipDead {
			w.addToDead(job, runErr)
		}
	}
}

func (w *worker) addToRetry(job *Job, runErr error) {
	rawJSON, err := job.serialize()
	if err != nil {
		logError("worker.add_to_retry", err)
		return
	}

	var backoff BackoffCalculator

	// Choose the backoff provider
	jt, ok := w.jobTypes[job.Name]
	if ok {
		backoff = jt.Backoff
	}

	if backoff == nil {
		backoff = defaultBackoffCalculator
	}
	fmt.Println(rawJSON)
	// TODO: 需要进行 backoff 的重试机制
}

func (w *worker) addToDead(job *Job, runErr error) {
	rawJSON, err := job.serialize()

	if err != nil {
		logError("worker.add_to_dead.serialize", err)
		return
	}
	fmt.Println(rawJSON)
	// TODO: 需要将队列添加到死队列中
}

// Default algorithm returns an fastly increasing backoff counter which grows in an unbounded fashion
func defaultBackoffCalculator(job *Job) int64 {
	fails := job.Fails
	return (fails * fails * fails * fails) + 15 + (rand.Int63n(30) * (fails + 1))
}
