package work

import (
	"testing"
	"time"

	"github.com/assembla/cony"
	"github.com/json-iterator/go"
	"github.com/stretchr/testify/assert"
)

func newCli() *cony.Client {
	return cony.NewClient(cony.URL("amqp://guest:guest@localhost:5672/ajd"))
}

func clientOpts() []cony.ClientOpt {
	return []cony.ClientOpt{cony.URL("amqp://guest:guest@localhost:5672/ajd")}
}

func TestEnqueue(t *testing.T) {
	ns := "work"
	name := "wat"
	enqueuer := NewEnqueuer(ns, clientOpts()...)
	cn := newConsumer(enqueuer.Namespace, &consumerType{Name: name}, enqueuer.defaultExc)
	cn.start(enqueuer.cli)

	job, err := enqueuer.Enqueue(name, Q{"a": 1, "b": "cool"})
	assert.Nil(t, err)
	assert.Equal(t, "wat", job.Name)
	assert.True(t, len(job.MessageId) > 10)                       // Something is in it
	assert.True(t, job.Timestamp.Unix() > (time.Now().Unix()-10)) // Within 10 seconds
	assert.True(t, job.Timestamp.Unix() < (time.Now().Unix()+10)) // Within 10 seconds

	q := Q{}
	jsoniter.Unmarshal(job.Body, &q)
	assert.Equal(t, "cool", q["b"])
	assert.EqualValues(t, 1, q["a"])

	j, _ := cn.Pop()
	assert.Equal(t, true, j.Ack())
	assert.Equal(t, "wat", j.Name)
	assert.True(t, len(j.MessageId) > 10)                       // Something is in it
	assert.True(t, j.Timestamp.Unix() > (time.Now().Unix()-10)) // Within 10 seconds
	assert.True(t, j.Timestamp.Unix() < (time.Now().Unix()+10)) // Within 10 seconds

	jsoniter.Unmarshal(job.Body, &q)
	assert.Equal(t, "cool", q["b"])
	assert.EqualValues(t, 1, q["a"])

	// Now enqueue another job, make sure that we can enqueue multiple
	_, err = enqueuer.Enqueue("wat", Q{"a": 1, "b": "cool"})
	_, err = enqueuer.Enqueue("wat", Q{"a": 1, "b": "cool"})
	assert.Nil(t, err)
	// pop 两次
	j, _ = cn.Pop()
	j.Ack()
	j, _ = cn.Pop()
	j.Ack()
}

type c struct{}

func TestEnqueueIn(t *testing.T) {
	ns := "work"
	enq := NewEnqueuer(ns, clientOpts()...)
	wp := NewWorkerPool(c{}, 20, ns, enq, cony.URL(""))
	wp.Start()
	defer wp.Stop()
	enqueuer := NewEnqueuer(ns, clientOpts()...)

	rk := "wat"
	job, err := enqueuer.EnqueueIn(rk, 300, Q{"a": 1, "b": "cool"})
	assert.Nil(t, err)
	if assert.NotNil(t, job) {
		assert.Equal(t, "wat", job.Name)
		assert.True(t, len(job.MessageId) > 10)                       // Something is in it
		assert.True(t, job.Timestamp.Unix() > (time.Now().Unix()-10)) // Within 10 seconds
		assert.True(t, job.Timestamp.Unix() < (time.Now().Unix()+10)) // Within 10 seconds
		q := Q{}
		jsoniter.Unmarshal(job.Body, &q)
		assert.Equal(t, "cool", q["b"])
		assert.EqualValues(t, 1, q["a"])
	}

	// Make sure the length of the scheduled job queue is 1
	//assert.EqualValues(t, 1, zsetSize(pool, redisKeyScheduled(ns)))

	// Get the job
	/*
		score, j := jobOnZset(pool, redisKeyScheduled(ns))

		assert.True(t, score > time.Now().Unix()+290)
		assert.True(t, score <= time.Now().Unix()+300)

		assert.Equal(t, "wat", j.Name)
		assert.True(t, len(j.ID) > 10)                        // Something is in it
		assert.True(t, j.EnqueuedAt > (time.Now().Unix()-10)) // Within 10 seconds
		assert.True(t, j.EnqueuedAt < (time.Now().Unix()+10)) // Within 10 seconds
		assert.Equal(t, "cool", j.ArgString("b"))
		assert.EqualValues(t, 1, j.ArgInt64("a"))
		assert.NoError(t, j.ArgError())
	*/
}
