package work

import (
	"github.com/assembla/cony"
	"github.com/streadway/amqp"
)

// consumer 负责与 RabbitMQ 中的某一个 queue + routing_key 打交道:
// * 获取 Message
// * Ack/Nack message
type consumer struct {
	ct        *consumerType
	exc       cony.Exchange
	namespace string

	// 因为 amqp 是 channel 级别安全, 但暴露出去后会有很多 goroutine 访问, 所以对于需要 ack 的 msg 使用 chan 约束
	ackDeliveies chan ackEvent
	done         chan int
	que          *cony.Queue
	c            *cony.Consumer
}

type ackEvent struct {
	msg *amqp.Delivery
	// t: 操作类型, 两种 ack/nack/reject
	t string
}

// 根据参数创建一个新的 consumer
func newConsumer(namespace string, ct *consumerType, exc cony.Exchange) *consumer {
	que := buildConyQueue(withNS(namespace, ct.RoutingKey), nil)

	return &consumer{
		namespace:    namespace,
		ct:           ct,
		exc:          exc,
		ackDeliveies: make(chan ackEvent, 500),
		done:         make(chan int, 1),
		que:          que,
	}
}

func (c *consumer) prefetch() int {
	prefetch := 20
	if c.ct != nil {
		prefetch = c.ct.Prefetch
	}
	// prefetch 必须大于 0, 不可以无限制
	if prefetch <= 0 {
		prefetch = 20
	}
	return prefetch
}

// 返回需要进行 Declare 的内容. Queue 与 binding 的 Declear
func (c *consumer) Declares() (ds []cony.Declaration) {
	ds = append(ds, cony.DeclareQueue(c.que))
	if c.ct != nil && len(c.ct.RoutingKey) > 0 {
		ds = append(ds, cony.DeclareBinding(cony.Binding{Queue: c.que, Exchange: c.exc, Key: c.ct.RoutingKey}))
	}
	return ds
}

func (c *consumer) start(cli *cony.Client) {
	if c.c == nil {
		c.c = cony.NewConsumer(c.que, cony.Qos(c.prefetch()))
	}
	cli.Declare(c.Declares())
	cli.Consume(c.c)
	go c.loopActEvent()
}

func (c *consumer) loopActEvent() {
	for {
		select {
		case ev := <-c.ackDeliveies:
			switch ev.t {
			case "ack":
				ev.msg.Ack(false)
			case "nack":
				ev.msg.Nack(false, true)
			case "reject":
				ev.msg.Reject(false)
			}
		case <-c.done:
			return
		}
	}
}

func (c *consumer) stop(cli *cony.Client) {
	c.done <- 1
	c.c.Cancel()
	c.c = nil
}
func (c *consumer) ack(ev ackEvent) {
	c.ackDeliveies <- ev
}

// Peek 一个任务
func (c *consumer) Peek() (*Message, error) {
	if c.c == nil {
		return nil, nil
	}
	select {
	case err := <-c.c.Errors():
		return nil, err
	case j := <-c.c.Deliveries():
		return decodeJob(&j, c.ack), nil
	default:
		return nil, nil
	}
}

// Pop 阻塞的获取一个任务
func (c *consumer) Pop() (*Message, error) {
	if c.c == nil {
		return nil, nil
	}
	j := <-c.c.Deliveries()
	return decodeJob(&j, c.ack), nil
}
