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

	knownJobs map[string]int64
	mtx       sync.RWMutex
}

// NewEnqueuer creates a new enqueuer with the specified Redis namespace and Redis pool.
func NewEnqueuer(namespace string, cli *cony.Client) *Enqueuer {
	if cli == nil {
		panic("NewEnqueuer needs a non-nil *cony.Client")
	}

	e := &Enqueuer{
		Namespace: namespace,
		cli:       cli,
		knownJobs: make(map[string]int64),
	}
	e.defaultExc = cony.Exchange{Name: e.withNS("work"), AutoDelete: false, Durable: true, Kind: "topic"}
	e.scheduleExc = cony.Exchange{Name: e.withNS("work.schedule"), AutoDelete: false, Durable: true, Kind: "topic"}
	e.pub = cony.NewPublisher(e.defaultExc.Name, "")
	e.schePub = cony.NewPublisher(e.scheduleExc.Name, "")
	go e.loop()
	return e
}

// TODO: 这个方法可以考虑和 workpool 合并
func (e *Enqueuer) withNS(s string) string {
	return fmt.Sprintf("%s.%s", e.Namespace, s)
}

// 开始保护 rabbitmq 的连接
func (e *Enqueuer) loop() {
	e.cli.Declare([]cony.Declaration{
		cony.DeclareExchange(e.defaultExc),
		cony.DeclareExchange(e.scheduleExc),
	})
	for e.cli.Loop() {
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

	rawJSON, err := job.serialize()
	if err != nil {
		return nil, err
	}

	e.pub.PublishWithRoutingKey(amqp.Publishing{
		Body:         rawJSON,
		DeliveryMode: 2,
		ContentType:  "application/json",
		Timestamp:    time.Now(),
	}, routingKey)

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
		Expiration:   strconv.Itoa(int(secondsFromNow)),
	}, routingKey)

	return scheduledJob, nil
}
