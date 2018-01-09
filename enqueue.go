package work

import (
	"fmt"
	"strconv"
	"sync"

	"github.com/assembla/cony"
	"github.com/json-iterator/go"
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

func (e *Enqueuer) map2Job(routingKey string, msg interface{}) (*Job, error) {
	body, err := jsoniter.Marshal(msg)
	if err != nil {
		return nil, err
	}
	// 将 msg 转换为 []byte 借用 Delivery 压入 RabbitMQ
	job := &Job{
		Name:     routingKey,
		Delivery: &amqp.Delivery{Body: body},
	}
	return job, nil
}

// Enqueue will enqueue the specified job name and arguments. The args param can be nil if no args ar needed.
// Example: e.Enqueue("send_email", work.Q{"addr": "test@example.com"})
func (e *Enqueuer) Enqueue(routingKey string, msg interface{}) (*Job, error) {
	job, err := e.map2Job(routingKey, msg)
	if err != nil {
		return nil, err
	}
	err = e.EnqueueJob(job)
	if err != nil {
		return nil, err
	}
	return job, nil
}

// EnqueueIn enqueues a job in the scheduled job queue for execution in secondsFromNow seconds.
func (e *Enqueuer) EnqueueIn(routingKey string, secondsFromNow int64, msg map[string]interface{}) (*ScheduledJob, error) {
	job, err := e.map2Job(routingKey, msg)
	if err != nil {
		return nil, err
	}
	err = e.EnqueueInJob(job, secondsFromNow)
	if err != nil {
		return nil, err
	}
	return &ScheduledJob{
		RunAt: nowEpochSeconds() + secondsFromNow,
		Job:   job,
	}, nil
}

// EnqueueJob 压入一个 job 任务
func (e *Enqueuer) EnqueueJob(job *Job) error {
	msg, err := job.encode()
	if err != nil {
		return err
	}

	return e.pub.PublishWithRoutingKey(msg.pub, msg.routingKey)
}

// EnqueueInJob 压入延时的 job
func (e *Enqueuer) EnqueueInJob(job *Job, secondsFromNow int64) error {
	msg, err := job.encode()
	if err != nil {
		return err
	}

	if secondsFromNow > 0 {
		msg.pub.Expiration = strconv.Itoa(int(secondsFromNow * 1000))
	}
	return e.schePub.PublishWithRoutingKey(msg.pub, msg.routingKey)
}
