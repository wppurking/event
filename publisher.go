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
	Namespace   string // eg, "myapp-work"
	cli         *cony.Client
	defaultExc  cony.Exchange
	scheduleExc cony.Exchange

	pub     *cony.Publisher
	schePub *cony.Publisher

	opts []cony.ClientOpt // 记录下 cony.Client 需要的参数
	mtx  sync.RWMutex
}

// NewPublisher creates a new enqueuer with the specified Redis namespace and Redis pool.
func NewPublisher(namespace string, opts ...cony.ClientOpt) *Publisher {

	defaultOpts := buildDefaultOpt()
	if len(opts) == 0 {
		panic("cony.Client 的参数错误")
	}
	defaultOpts = append(defaultOpts, opts...)

	e := &Publisher{
		Namespace: namespace,
		cli:       cony.NewClient(defaultOpts...),
	}
	e.newDeclears()
	go e.loop()
	return e
}

func (e *Publisher) newDeclears() {
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
func (e *Publisher) loop() {
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

	if secondsFromNow > 0 {
		msg.pub.Expiration = strconv.Itoa(int(secondsFromNow * 1000))
	}
	return e.schePub.PublishWithRoutingKey(msg.pub, msg.routingKey)
}
