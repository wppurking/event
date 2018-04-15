package event

import (
	"fmt"
	"strings"

	"github.com/assembla/cony"
	"github.com/streadway/amqp"
)

const (
	// TODO: delayQueue 与 deadQueue 都需要与 hutch, hutch-schedule 命名同规则, 这样才可以整合使用
	deadQueue    = "dead_queue"
	delayQueue   = "delay_queue"
	exchangeName = "hutch"

	//# fixed delay levels
	//# seconds(4): 5s, 10s, 20s, 30s
	//# minutes(14): 1m, 2m, 3m, 4m, 5m, 6m, 7m, 8m, 9m, 10m, 20m, 30m, 40m, 50m
	//# hours(3): 1h, 2h, 3h
)

var (
	// 拥有的 delayQueues
	delayQueues = []string{
		"5s", "10s", "20s", "30s",
		"60s", "120s", "180s", "240s", "300s", "360s", "420s", "480s", "540s", "600s", "1200s", "1800s", "2400s", "3000s",
		"3600s", "7200s", "10800s",
	}
	// 30 * 24 * 3600  * 1000
	oneMonth = int64(2592000000)
)

// 构建 buildin 重试/retry 的队列
func buildinQueueName(names ...string) string {
	return strings.Join(names, "_")
}

// 构建内置的 retry 与 dead queue
func builtinQueue(exc cony.Exchange, delayExc cony.Exchange, cli *cony.Client) {
	var declears []cony.Declaration

	// delay queue_<5s>: 不需要 consumer, 由 rabbitmq 的 ddl 自行处理
	// - 最长超过 30 天重新投递
	// - 重新投递到默认的 exchange
	qus, binds := builtinDelayQueues(exc, delayExc)

	// dead queue: 不需要 consumer, 由 rabbitmq 自行过期处理
	// - 消息超过 30 天放弃
	// - 超过 10w 条消息放弃
	deadQue := buildConyQueue(
		buildinQueueName(exc.Name, deadQueue),
		amqp.Table{"x-message-ttl": oneMonth, "x-max-length": int64(100000)},
	)
	deadBnd := cony.Binding{Queue: deadQue, Exchange: exc, Key: deadQueue + ".#"}

	qus = append(qus, deadQue)
	binds = append(binds, deadBnd)
	for _, q := range qus {
		declears = append(declears, cony.DeclareQueue(q))
	}
	for _, b := range binds {
		declears = append(declears, cony.DeclareBinding(b))
	}

	cli.Declare(declears)
}

func builtinDelayQueues(exc cony.Exchange, delayExc cony.Exchange) ([]*cony.Queue, []cony.Binding) {
	var ques []*cony.Queue
	var binds []cony.Binding
	for _, q := range delayQueues {
		retryQue := buildConyQueue(
			// <hutch>_delay_queue_<5s>
			buildinQueueName(exc.Name, delayQueue, q),
			amqp.Table{"x-message-ttl": oneMonth, "x-dead-letter-exchange": exc.Name},
		)
		ques = append(ques, retryQue)
		binds = append(binds, cony.Binding{
			Queue:    retryQue,
			Exchange: delayExc,
			Key:      fmt.Sprintf("%s.schedule.%s", exc.Name, q), // <hutch>.schedule.<5s>
		})
	}
	return ques, binds
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
	vname := name
	if len(name) == 0 {
		vname = exchangeName
	}
	return cony.Exchange{Name: vname, AutoDelete: false, Durable: true, Kind: "topic"}
}
