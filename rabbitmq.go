package work

import (
	"github.com/assembla/cony"
	"github.com/streadway/amqp"
)

// 构建内置的 retry 与 dead queue
func builtinQueue(ns string, exc cony.Exchange, cli *cony.Client) {
	// retry queue: 不需要 consumer, 由 rabbitmq 的 ddl 自行处理
	retryQue := buildConyQueue(withNS(ns, retryQueue),
		amqp.Table{"x-message-ttl": "2592000000", "x-dead-letter-exchange": exc.Name})
	retryBnd := cony.Binding{Queue: retryQue, Exchange: exc, Key: "#"}

	// dead queue: 不需要 consumer, 由 rabbitmq 自行过期处理
	deadQue := buildConyQueue(withNS(ns, deadQueue), amqp.Table{"x-message-ttl": "2592000000"})

	cli.Declare([]cony.Declaration{
		cony.DeclareQueue(deadQue),
		cony.DeclareQueue(retryQue),
		cony.DeclareBinding(retryBnd),
	})
}

// 构建一个默认的持久化的 queue
func buildConyQueue(name string, table amqp.Table) *cony.Queue {
	return &cony.Queue{
		Name:       name,
		AutoDelete: false,
		Durable:    true,
		Args:       table,
	}
}
