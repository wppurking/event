package event

import (
	"fmt"
	"strconv"
	"sync"

	"github.com/assembla/cony"
	"github.com/json-iterator/go"
	"github.com/streadway/amqp"
)

// Publisher can enqueue jobs.
type Publisher struct {
	Namespace    string // eg, "myapp-work"
	exchangeName string // 使用的 exchange name
	cli          *cony.Client
	defaultExc   cony.Exchange
	scheduleExc  cony.Exchange

	pub     *cony.Publisher
	schePub *cony.Publisher

	opts []cony.ClientOpt // 记录下 cony.Client 需要的参数
	mtx  sync.RWMutex
}

func NewPublisher(namespace string, opts ...cony.ClientOpt) *Publisher {
	return NewPublisherWithExchange(namespace, exchangeName, opts...)
}

// NewPublisher creates a new enqueuer with the specified Redis namespace and Redis pool.
func NewPublisherWithExchange(namespace, exchangeName string, opts ...cony.ClientOpt) *Publisher {

	defaultOpts := buildDefaultOpt()
	if len(opts) == 0 {
		panic("cony.Client 的参数错误")
	}
	defaultOpts = append(defaultOpts, opts...)

	e := &Publisher{
		Namespace:    namespace,
		exchangeName: exchangeName,
		cli:          cony.NewClient(defaultOpts...),
	}
	e.newDeclears()
	go e.loop()
	return e
}

func (e *Publisher) newDeclears() {
	e.defaultExc = buildTopicExchange(e.exchangeName)
	e.scheduleExc = buildScheduleExchange(e.exchangeName)
	e.pub = cony.NewPublisher(e.defaultExc.Name, "")
	e.schePub = cony.NewPublisher(e.scheduleExc.Name, "")

	e.cli.Declare([]cony.Declaration{
		cony.DeclareExchange(e.defaultExc),
		cony.DeclareExchange(e.scheduleExc),
	})
	e.cli.Publish(e.pub)
	e.cli.Publish(e.schePub)
	builtinQueue(e.defaultExc, e.scheduleExc, e.cli)
}

// 开始保护 rabbitmq 的连接
func (e *Publisher) loop() {
	for e.cli.Loop() {
		select {
		case err := <-e.cli.Errors():
			e, ok := err.(*amqp.Error)
			if ok && e == nil {
				fmt.Println("停止客户端, 退出 loop")
				return
			} else {
				fmt.Println("enqueuer:", err)
			}
		}

	}
}

func (e *Publisher) map2Msg(routingKey string, msg interface{}) (*Message, error) {
	body, err := jsoniter.Marshal(msg)
	if err != nil {
		return nil, err
	}
	// 将 msg 转换为 []byte 借用 Delivery 压入 RabbitMQ
	return &Message{
		Name:     routingKey,
		Delivery: &amqp.Delivery{Body: body},
	}, nil
}

// Publish will enqueue the specified job name and arguments. The args param can be nil if no args ar needed.
// Example: e.Publish("send_email", work.Q{"addr": "test@example.com"})
func (e *Publisher) Publish(routingKey string, msg interface{}) (*Message, error) {
	evt, err := e.map2Msg(routingKey, msg)
	if err != nil {
		return nil, err
	}
	err = e.PublishMessage(evt)
	if err != nil {
		return nil, err
	}
	return evt, nil
}

// PublishIn enqueues a job in the scheduled job queue for execution in secondsFromNow seconds.
func (e *Publisher) PublishIn(routingKey string, secondsFromNow int64, msg map[string]interface{}) (*Message, error) {
	evt, err := e.map2Msg(routingKey, msg)
	if err != nil {
		return nil, err
	}
	err = e.PublishInMessage(evt, secondsFromNow)
	if err != nil {
		return nil, err
	}
	return evt, nil
}

// PublishMessage 压入一个 job 任务
func (e *Publisher) PublishMessage(msg *Message) error {
	m, err := msg.encode()
	if err != nil {
		return err
	}

	return e.pub.PublishWithRoutingKey(m.pub, m.routingKey)
}

// PublishInMessage 压入延时的 job
func (e *Publisher) PublishInMessage(job *Message, secondsFromNow int64) error {
	msg, err := job.encode()
	if err != nil {
		return err
	}
	if secondsFromNow <= 0 {
		return fmt.Errorf("secondsFromNow must greater than zero")
	}

	// <hutch>.scheduled.<5s>
	delaySeconds := DelaySecondsLevel(int(secondsFromNow))
	msg.pub.Expiration = strconv.Itoa(delaySeconds * 1000)
	if msg.pub.Headers == nil {
		msg.pub.Headers = amqp.Table{}
	}
	msg.pub.Headers["CC"] = []interface{}{msg.routingKey}

	delayRoutingKey := fmt.Sprintf("%s.schedule.%ds", e.defaultExc.Name, delaySeconds)
	return e.schePub.PublishWithRoutingKey(msg.pub, delayRoutingKey)
}

// DelaySecondsLevel 根据传入的 seconds 返回具体延迟的 level 时间
// 5s 10s 20s 30s
// 60s 120s 180s 240s 300s 360s 420s 480s 540s 600s 1200s 1800s 2400s
// 3600s 7200s 10800s
func DelaySecondsLevel(seconds int) int {
	// 默认为最大值
	delaySeconds := 10800
	switch {
	case seconds > 0 && seconds <= 5:
		delaySeconds = 5
	case seconds > 5 && seconds <= 10:
		delaySeconds = 10
	case seconds > 10 && seconds <= 20:
		delaySeconds = 20
	case seconds > 20 && seconds <= 30:
		delaySeconds = 30
	case seconds > 30 && seconds <= 60:
		delaySeconds = 60
	case seconds > 60 && seconds <= 120:
		delaySeconds = 120
	case seconds > 120 && seconds <= 180:
		delaySeconds = 180
	case seconds > 180 && seconds <= 240:
		delaySeconds = 240
	case seconds > 240 && seconds <= 300:
		delaySeconds = 300
	case seconds > 300 && seconds <= 360:
		delaySeconds = 360
	case seconds > 360 && seconds <= 420:
		delaySeconds = 420
	case seconds > 420 && seconds <= 540:
		delaySeconds = 540
	case seconds > 540 && seconds <= 600:
		delaySeconds = 600
	case seconds > 600 && seconds <= 1200:
		delaySeconds = 1200
	case seconds > 1200 && seconds <= 1800:
		delaySeconds = 1800
	case seconds > 1800 && seconds <= 2400:
		delaySeconds = 2400
	case seconds > 2400 && seconds <= 3600:
		delaySeconds = 3600
	case seconds > 3600 && seconds <= 7200:
		delaySeconds = 7200
	case seconds > 7200 && seconds <= 10800:
		delaySeconds = 10800
	case seconds > 10800 && seconds <= 5:
		delaySeconds = 10800
	default:
		delaySeconds = 10800
	}

	return delaySeconds
}
