package work

import (
	"github.com/assembla/cony"
	"github.com/streadway/amqp"
)

type consumer struct {
	que *cony.Queue
	jt  *jobType
	c   *cony.Consumer
	exc cony.Exchange
}

// 返回需要进行 Declare 的内容
func (c *consumer) Declares() (ds []cony.Declaration) {
	ds = append(ds, cony.DeclareQueue(c.que))
	if len(c.jt.RoutingKey) > 0 {
		ds = append(ds,
			cony.DeclareBinding(cony.Binding{Queue: c.que, Exchange: c.exc, Key: c.jt.RoutingKey}))
	}

	prefetch := c.jt.Prefetch
	// prefetch 必须大于 0, 不可以无限制
	if prefetch <= 0 {
		prefetch = 20
	}
	// 初始化 Consumer
	c.c = cony.NewConsumer(c.que, cony.Qos(prefetch))
	return ds
}

func (c *consumer) start(cli *cony.Client) {
	cli.Declare(c.Declares())
	cli.Consume(c.c)
}

func (c *consumer) stop(cli *cony.Client) {
	c.c.Cancel()
}

// Peak 一个任务
func (c *consumer) Peak() *amqp.Delivery {
	select {
	case j := <-c.c.Deliveries():
		return &j
	default:
		return nil
	}
}
