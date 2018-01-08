package work

import (
	"github.com/assembla/cony"
)

type consumer struct {
	jt        *jobType
	exc       cony.Exchange
	namespace string

	que *cony.Queue
	c   *cony.Consumer
}

// 根据参数创建一个新的 consumer
func newConsumer(namespace string, jt *jobType, exc cony.Exchange) *consumer {
	prefetch := 20
	if jt != nil {
		prefetch = jt.Prefetch
	}
	// prefetch 必须大于 0, 不可以无限制
	if prefetch <= 0 {
		prefetch = 20
	}

	que := buildConyQueue(withNS(namespace, jt.Name), nil)

	return &consumer{
		namespace: namespace,
		jt:        jt,
		exc:       exc,
		que:       que,
		c:         cony.NewConsumer(que, cony.Qos(prefetch)),
	}
}

// 返回需要进行 Declare 的内容. Queue 与 binding 的 Declear
func (c *consumer) Declares() (ds []cony.Declaration) {
	ds = append(ds, cony.DeclareQueue(c.que))
	if c.jt != nil && len(c.jt.Name) > 0 {
		ds = append(ds, cony.DeclareBinding(cony.Binding{Queue: c.que, Exchange: c.exc, Key: c.jt.Name}))
	}
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
