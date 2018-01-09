package work

// 准备将传递的消息, 充分利用 RabbitMQ 中的消息, 让 message 只保留 json
import (
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/streadway/amqp"
)

type message struct {
	pub        amqp.Publishing
	routingKey string
}

// 输出 rabbitmq 使用的 Publishing
func (j *Job) serializeMsg() (*message, error) {
	rawJSON, err := jsoniter.Marshal(j)
	if err != nil {
		return nil, err
	}
	p := amqp.Publishing{
		Headers:      amqp.Table{"name": j.Name, "fails": j.Fails},
		Body:         rawJSON,
		DeliveryMode: 2,
		ContentType:  "application/json",
		Timestamp:    time.Now(),
	}
	return &message{
		pub:        p,
		routingKey: j.Name,
	}, nil
}
