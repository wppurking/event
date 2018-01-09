package work

import (
	"strings"

	"github.com/json-iterator/go"
	"github.com/streadway/amqp"
)

// Job represents a job.
type Job struct {
	*amqp.Delivery `json:"-"`

	// Inputs when making a new job
	Name     string            `json:"-"`
	fails    int64             `json:"-"`
	argError error             `json:"-"`
	ack      func(ev ackEvent) `json:"-"` // ack 的行动
}

// Q is a shortcut to easily specify arguments for jobs when enqueueing them.
// Example: e.Enqueue("send_email", work.Q{"addr": "test@example.com", "track": true})
type Q map[string]interface{}

func (j *Job) Ack() bool {
	if j.ack == nil {
		return false
	}
	j.ack(ackEvent{msg: j.Delivery, t: "ack"})
	return true
}

func (j *Job) Nack() bool {
	if j.ack == nil {
		return false
	}
	j.ack(ackEvent{msg: j.Delivery, t: "nack"})
	return true
}

func (j *Job) Reject() bool {
	if j.ack == nil {
		return false
	}
	j.ack(ackEvent{msg: j.Delivery, t: "reject"})
	return true
}

func (j *Job) serialize() ([]byte, error) {
	return jsoniter.Marshal(j)
}

// Fails 返回从 x-dead header 信息中记录的重试记录
func (j *Job) Fails() int64 {
	// refs: https://github.com/wppurking/hutch-schedule/blob/master/lib/hutch/error_handlers/max_retry.rb
	//map[x-death:[map[exchange:work.work.schedule routing-keys:[foobar] original-expiration:64000 count:1 reason:expired queue:work._retry time:2018-01-09 00:01:43 +0800 CST]]]

	if j.fails > 0 {
		return j.fails
	}

	deathsMap, ok := j.Headers["x-death"]
	if !ok {
		return 0
	}

	if deaths, ok := deathsMap.([]map[string]interface{}); ok {
		// 解析出所有的 x-dead
		var xDeathArray []map[string]interface{}
		for _, death := range deaths {
			for _, rk := range death["routing-keys"].([]string) {
				// Fixme: 这里需要获取到 workPool 的 Namespace 才能做 100% 匹配, 现在只能做 routing_key 的包含匹配
				if len(j.Name) > 0 && strings.Contains(strings.ToLower(rk), j.Name) {
					xDeathArray = append(xDeathArray, death)
				}
			}
		}

		c := 0
		for _, xDeath := range xDeathArray {
			if cd, ok := xDeath["count"].(int); ok {
				c += cd
			}
		}
		j.fails = int64(c)
		return j.fails
	}
	return 0
}

// RetryJob represents a job in the retry queue.
type RetryJob struct {
	RetryAt int64 `json:"retry_at"`
	*Job
}

// ScheduledJob represents a job in the scheduled queue.
type ScheduledJob struct {
	RunAt int64 `json:"run_at"`
	*Job
}

// DeadJob represents a job in the dead queue.
type DeadJob struct {
	DiedAt int64 `json:"died_at"`
	*Job
}
