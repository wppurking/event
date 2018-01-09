package work

import (
	"github.com/json-iterator/go"
	"github.com/streadway/amqp"
)

// Job represents a job.
type Job struct {
	// Inputs when making a new job
	Name       string                 `json:"name,omitempty"`
	ID         string                 `json:"id"`
	EnqueuedAt int64                  `json:"t"`
	Args       map[string]interface{} `json:"args"`
	Unique     bool                   `json:"unique,omitempty"`

	rawJSON      []byte            `json:"-"`
	dequeuedFrom []byte            `json:"-"`
	inProgQueue  []byte            `json:"-"`
	argError     error             `json:"-"`
	msg          *amqp.Delivery    `json:"-"`
	ack          func(ev ackEvent) `json:"-"` // ack 的行动
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
	job.msg = msg
	job.ack = ack
	return &job, nil
}

func (j *Job) Ack() bool {
	if j.ack == nil {
		return false
	}
	j.ack(ackEvent{msg: j.msg, t: "ack"})
	return true
}

func (j *Job) Nack() bool {
	if j.ack == nil {
		return false
	}
	j.ack(ackEvent{msg: j.msg, t: "nack"})
	return true
}

func (j *Job) Reject() bool {
	if j.ack == nil {
		return false
	}
	j.ack(ackEvent{msg: j.msg, t: "reject"})
	return true
}

func (j *Job) serialize() ([]byte, error) {
	return jsoniter.Marshal(j)
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
