package event

import (
	"fmt"

	"github.com/assembla/cony"
	"github.com/streadway/amqp"
)

const (
	// TODO: retryQueue 与 deadQueue 都需要与 hutch, hutch-schedule 命名同规则, 这样才可以整合使用
	retryQueue   = "dead_queue"
	deadQueue    = "schedule_queue"
	exchangeName = "hutch"
)

// 构建 buildin 重试/retry 的队列
func buildinQueueName(exName, sufix string) string {
	return fmt.Sprintf("%s_%s", exName, sufix)
}

// 构建内置的 retry 与 dead queue
func builtinQueue(ns string, exc cony.Exchange, schExc cony.Exchange, cli *cony.Client) {
	// 30 * 24 * 3600  * 1000
	oneMonth := int64(2592000000)

	// retry queue: 不需要 consumer, 由 rabbitmq 的 ddl 自行处理
	// - 最长超过 30 天重新投递
	// - 重新投递到默认的 exchange
	retryQue := buildConyQueue(
		buildinQueueName(exc.Name, retryQueue),
		amqp.Table{"x-message-ttl": oneMonth, "x-dead-letter-exchange": exc.Name},
	)
	retryBnd := cony.Binding{Queue: retryQue, Exchange: schExc, Key: "#"}

	// dead queue: 不需要 consumer, 由 rabbitmq 自行过期处理
	// - 消息超过 30 天放弃
	// - 超过 10w 条消息放弃
	deadQue := buildConyQueue(
		buildinQueueName(exc.Name, deadQueue),
		amqp.Table{"x-message-ttl": oneMonth, "x-max-length": int64(100000)},
	)
	deadBnd := cony.Binding{Queue: deadQue, Exchange: exc, Key: deadQueue + ".#"}

	cli.Declare([]cony.Declaration{
		cony.DeclareQueue(deadQue),
		cony.DeclareQueue(retryQue),
		cony.DeclareBinding(retryBnd),
		cony.DeclareBinding(deadBnd),
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

// buildDefaultOpt 构建默认的 cony.Client 配置
func buildDefaultOpt() []cony.ClientOpt {
	return []cony.ClientOpt{
		cony.URL(""),
		cony.Backoff(cony.DefaultBackoff),
	}
}

func buildScheduleExchange(name string) cony.Exchange {
	vname := name
	if len(name) == 0 {
		vname = exchangeName
	}
	return buildTopicExchange(fmt.Sprintf("%s.schedule", vname))
}

func buildTopicExchange(name string) cony.Exchange {
	if len(name) == 0 {
		return cony.Exchange{Name: exchangeName, AutoDelete: false, Durable: true, Kind: "topic"}
	}
	return cony.Exchange{Name: name, AutoDelete: false, Durable: true, Kind: "topic"}
}
