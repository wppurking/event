package work

import (
	"strings"

	"github.com/json-iterator/go"
	"github.com/streadway/amqp"
)

// Message represents a job.
type Message struct {
	*amqp.Delivery `json:"-"`

	// Inputs when making a new job
	Name          string            `json:"-"`
	fails         int64             `json:"-"`
	argError      error             `json:"-"`
	ack           func(ev ackEvent) `json:"-"` // ack 的行动
	nonPersistent bool              `json:"-"` // 是否持久化
}

// Q is a shortcut to easily specify arguments for jobs when enqueueing them.
// Example: e.Enqueue("send_email", work.Q{"addr": "test@example.com", "track": true})
type Q map[string]interface{}

func (j *Message) Ack() bool {
	if j.ack == nil {
		return false
	}
	j.ack(ackEvent{msg: j.Delivery, t: "ack"})
	return true
}

func (j *Message) Nack() bool {
	if j.ack == nil {
		return false
	}
	j.ack(ackEvent{msg: j.Delivery, t: "nack"})
	return true
}

func (j *Message) Reject() bool {
	if j.ack == nil {
		return false
	}
	j.ack(ackEvent{msg: j.Delivery, t: "reject"})
	return true
}

func (j *Message) serialize() ([]byte, error) {
	return jsoniter.Marshal(j)
}

// Fails 返回从 x-dead header 信息中记录的重试记录
func (j *Message) Fails() int64 {
	// refs: https://github.com/wppurking/hutch-schedule/blob/master/lib/hutch/error_handlers/max_retry.rb
	if j.fails > 0 {
		return j.fails
	}

	deathsMap, ok := j.Headers["x-death"]
	if !ok {
		return 0
	}

	if deaths, ok := deathsMap.([]interface{}); ok {
		// 解析出所有的 x-dead
		var xDeathArray []amqp.Table
		for _, death := range deaths {
			if dt, ok := death.(amqp.Table); ok {
				for _, rk := range dt["routing-keys"].([]interface{}) {
					// Fixme: 这里需要获取到 workPool 的 Namespace 才能做 100% 匹配, 现在只能做 routing_key 的包含匹配
					if len(j.Name) > 0 && strings.Contains(strings.ToLower(rk.(string)), j.Name) {
						xDeathArray = append(xDeathArray, dt)
					}
				}
			}
		}

		var c int64 = 0
		for _, xDeath := range xDeathArray {
			// amqp.Table 中的 count 支持下面几种 int
			if cd, ok := xDeath["count"].(int64); ok {
				c += cd
			} else if cd, ok := xDeath["count"].(int32); ok {
				c += int64(cd)
			} else if cd, ok := xDeath["count"].(int16); ok {
				c += int64(cd)
			}
		}
		j.fails = c
		return j.fails
	}
	return 0
}
