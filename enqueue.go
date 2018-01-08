package work

import (
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/assembla/cony"
	"github.com/streadway/amqp"
)

// Enqueuer can enqueue jobs.
type Enqueuer struct {
	Namespace   string // eg, "myapp-work"
	cli         *cony.Client
	defaultExc  cony.Exchange
	scheduleExc cony.Exchange

	pub     *cony.Publisher
	schePub *cony.Publisher

	opts []cony.ClientOpt // 记录下 cony.Client 需要的参数
	mtx  sync.RWMutex
}

// NewEnqueuer creates a new enqueuer with the specified Redis namespace and Redis pool.
func NewEnqueuer(namespace string, opts ...cony.ClientOpt) *Enqueuer {

	defaultOpts := buildDefaultOpt()
	if len(opts) == 0 {
		panic("cony.Client 的参数错误")
	}
	defaultOpts = append(defaultOpts, opts...)

	e := &Enqueuer{
		Namespace: namespace,
		cli:       cony.NewClient(defaultOpts...),
	}
	e.newDeclears()
	go e.loop()
	return e
}

func (e *Enqueuer) newDeclears() {
	e.defaultExc = cony.Exchange{Name: withNS(e.Namespace, "work"), AutoDelete: false, Durable: true, Kind: "topic"}
	e.scheduleExc = cony.Exchange{Name: withNS(e.Namespace, "work.schedule"), AutoDelete: false, Durable: true, Kind: "topic"}
	e.pub = cony.NewPublisher(e.defaultExc.Name, "")
	e.schePub = cony.NewPublisher(e.scheduleExc.Name, "")

	e.cli.Declare([]cony.Declaration{
		cony.DeclareExchange(e.defaultExc),
		cony.DeclareExchange(e.scheduleExc),
	})
	e.cli.Publish(e.pub)
	e.cli.Publish(e.schePub)
	builtinQueue(e.Namespace, e.defaultExc, e.scheduleExc, e.cli)
}

// 开始保护 rabbitmq 的连接
func (e *Enqueuer) loop() {
	for e.cli.Loop() {
		select {
		case err := <-e.cli.Errors():
			e, ok := err.(*amqp.Error)
			if ok && e == nil {
				fmt.Printf("停止客户端, 退出 loop: %v\n", e)
				return
			} else {
				fmt.Println("enqueuer:", err)
			}
		}

	}
}

// Enqueue will enqueue the specified job name and arguments. The args param can be nil if no args ar needed.
// Example: e.Enqueue("send_email", work.Q{"addr": "test@example.com"})
func (e *Enqueuer) Enqueue(routingKey string, args map[string]interface{}) (*Job, error) {
	job := &Job{
		Name:       routingKey,
		ID:         makeIdentifier(),
		EnqueuedAt: nowEpochSeconds(),
		Args:       args,
	}
	err := e.EnqueueJob(job)
	if err != nil {
		return nil, err
	}
	return job, nil
}

// EnqueueIn enqueues a job in the scheduled job queue for execution in secondsFromNow seconds.
func (e *Enqueuer) EnqueueIn(routingKey string, secondsFromNow int64, args map[string]interface{}) (*ScheduledJob, error) {
	job := &Job{
		Name:       routingKey,
		ID:         makeIdentifier(),
		EnqueuedAt: nowEpochSeconds(),
		Args:       args,
	}
	return e.EnqueueInJob(job, secondsFromNow)
}

func (e *Enqueuer) EnqueueJob(job *Job) error {
	rawJSON, err := job.serialize()
	if err != nil {
		return err
	}

	e.pub.PublishWithRoutingKey(amqp.Publishing{
		Body:         rawJSON,
		DeliveryMode: 2,
		ContentType:  "application/json",
		Timestamp:    time.Now(),
	}, job.Name)

	return nil
}

func (e *Enqueuer) EnqueueInJob(job *Job, secondsFromNow int64) (*ScheduledJob, error) {
	rawJSON, err := job.serialize()
	if err != nil {
		return nil, err
	}

	scheduledJob := &ScheduledJob{
		RunAt: nowEpochSeconds() + secondsFromNow,
		Job:   job,
	}
	e.schePub.PublishWithRoutingKey(amqp.Publishing{
		Body:         rawJSON,
		DeliveryMode: 2,
		ContentType:  "application/json",
		Timestamp:    time.Now(),
		Expiration:   strconv.Itoa(int(secondsFromNow * 1000)),
	}, job.Name)

	return scheduledJob, nil
}
