package event

// 准备将传递的消息, 充分利用 RabbitMQ 中的消息, 让 message 只保留 json
import (
	"strings"
	"time"

	"github.com/streadway/amqp"
)

type message struct {
	pub        amqp.Publishing
	routingKey string
}

// 输出 rabbitmq 使用的 Publishing
func (j *Message) encode() (*message, error) {
	j.MessageId = makeIdentifier()
	j.Timestamp = time.Now()
	p := amqp.Publishing{
		MessageId:    j.MessageId,
		Headers:      j.Headers,
		Body:         j.Body,
		DeliveryMode: 2,
		ContentType:  "application/json",
		Timestamp:    j.Timestamp,
	}
	// 如果不需要持久化, 那么则只在内存中出现
	if j.nonPersistent {
		p.DeliveryMode = 1
	}
	return &message{
		pub:        p,
		routingKey: j.Name,
	}, nil
}

// 解析出一个 Message
func decodeMessage(msg *amqp.Delivery, ack func(ev ackEvent)) *Message {
	// Name 需要从两个地方获取, 一个为 Headers 中的 CC, 一个为 msg.RoutingKey
	// 因为在使用 fixed delay level 的方案中, routing_key 会被修改为 <hutch>.schedule.<5s> , 而真实的 routing_key
	// 则由 CC 中的信息携带

	name := msg.RoutingKey
	// 从 Headers 中解析出第一个 CC
	if msg.Headers != nil {
		if ccs, ok := msg.Headers["CC"]; ok {
			switch ccsv := ccs.(type) {
			case []interface{}:
				if firstCC, isStr := ccsv[0].(string); isStr {
					name = firstCC
				}
			}
		}
	}

	return &Message{
		Name:     strings.ToLower(name),
		Delivery: msg,
		ack:      ack,
		fails:    0, // 强制清零, 让后续的 job.Fails 方法 lazy 计算并缓存
	}
}
