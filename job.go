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
	Name       string                 `json:"name,omitempty"`
	ID         string                 `json:"id"`
	EnqueuedAt int64                  `json:"t"`
	Args       map[string]interface{} `json:"args"`
	Unique     bool                   `json:"unique,omitempty"`

	fails    int64             `json:"-"`
	rawJSON  []byte            `json:"-"`
	argError error             `json:"-"`
	ack      func(ev ackEvent) `json:"-"` // ack 的行动
}

// Q is a shortcut to easily specify arguments for jobs when enqueueing them.
// Example: e.Enqueue("send_email", work.Q{"addr": "test@example.com", "track": true})
type Q map[string]interface{}

func newJob(rawJSON []byte, msg *amqp.Delivery, ack func(ev ackEvent)) (*Job, error) {
	var job Job
	err := jsoniter.Unmarshal(rawJSON, &job)
	if err != nil {
		return nil, err
	}
	job.rawJSON = rawJSON
	job.Delivery = msg
	job.ack = ack
	return &job, nil
}

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
				if strings.ToLower(rk) != j.Name {
					continue
				}
				xDeathArray = append(xDeathArray, death)
			}
		}

		var c int64 = 0
		for _, xDeath := range xDeathArray {
			if cd, ok := xDeath["count"].(int64); ok {
				c += cd
			}
		}
		j.fails = c
		return j.fails
	}
	return 0
}

func (j *Job) failed(err error) {
	// TODO: 需要这个方法吗?
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
