package work

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
func (j *Job) encode() (*message, error) {
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

// 解析出一个 Job
func decodeJob(msg *amqp.Delivery, ack func(ev ackEvent)) *Job {
	return &Job{
		Name:     strings.ToLower(msg.RoutingKey),
		Delivery: msg,
		ack:      ack,
		fails:    0, // 强制清零, 让后续的 job.Fails 方法 lazy 计算并缓存
	}
}
