package work

import (
	"github.com/assembla/cony"
)

type consumer struct {
	que *cony.Queue
	jt  *jobType
	c   *cony.Consumer
	exc cony.Exchange
}

// 返回需要进行 Declare 的内容
func (c *consumer) Declares() (ds []cony.Declaration) {
	var prefetch int

	ds = append(ds, cony.DeclareQueue(c.que))
	if c.jt == nil {
		prefetch = 20
	} else {
		prefetch = c.jt.Prefetch
		if len(c.jt.Name) > 0 {
			ds = append(ds, cony.DeclareBinding(cony.Binding{Queue: c.que, Exchange: c.exc, Key: c.jt.Name}))
		}
	}
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

// Peek 一个任务
func (c *consumer) Peek() (*Job, error) {
	select {
	case j := <-c.c.Deliveries():
		return newJob(j.Body, &j)
	default:
		return nil, nil
	}
}

// Pop 阻塞的获取一个任务
func (c *consumer) Pop() (*Job, error) {
	j := <-c.c.Deliveries()
	return newJob(j.Body, &j)
}
