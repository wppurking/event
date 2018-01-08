package work

import (
	"fmt"
	"testing"
	"time"

	"github.com/assembla/cony"
	"github.com/stretchr/testify/assert"
)

func newCli() *cony.Client {
	return cony.NewClient(cony.URL("amqp://guest:guest@localhost:5672/ajd"))
}

func TestEnqueue(t *testing.T) {
	cli := newCli()
	ns := "work"
	name := "wat"
	enqueuer := NewEnqueuer(ns, cli)
	cn := newConsumer(enqueuer.Namespace, &jobType{Name: name}, enqueuer.defaultExc)
	cn.start(cli)

	job, err := enqueuer.Enqueue(name, Q{"a": 1, "b": "cool"})
	assert.Nil(t, err)
	assert.Equal(t, "wat", job.Name)
	assert.True(t, len(job.ID) > 10)                        // Something is in it
	assert.True(t, job.EnqueuedAt > (time.Now().Unix()-10)) // Within 10 seconds
	assert.True(t, job.EnqueuedAt < (time.Now().Unix()+10)) // Within 10 seconds
	assert.Equal(t, "cool", job.ArgString("b"))
	assert.EqualValues(t, 1, job.ArgInt64("a"))
	assert.NoError(t, job.ArgError())

	j, _ := cn.Pop()
	fmt.Println(j)
	err = j.msg.Ack(false)
	assert.NoError(t, err)
	assert.Equal(t, "wat", j.Name)
	assert.True(t, len(j.ID) > 10)                        // Something is in it
	assert.True(t, j.EnqueuedAt > (time.Now().Unix()-10)) // Within 10 seconds
	assert.True(t, j.EnqueuedAt < (time.Now().Unix()+10)) // Within 10 seconds
	assert.Equal(t, "cool", j.ArgString("b"))
	assert.EqualValues(t, 1, j.ArgInt64("a"))
	assert.NoError(t, j.ArgError())

	// Now enqueue another job, make sure that we can enqueue multiple
	_, err = enqueuer.Enqueue("wat", Q{"a": 1, "b": "cool"})
	_, err = enqueuer.Enqueue("wat", Q{"a": 1, "b": "cool"})
	assert.Nil(t, err)
	// pop 两次
	j, _ = cn.Pop()
	j.msg.Ack(false)
	j, _ = cn.Pop()
	j.msg.Ack(false)
}

type c struct{}

func TestEnqueueIn(t *testing.T) {
	cli := newCli()
	ns := "work"
	wp := NewWorkerPool(c{}, 20, ns, cli)
	wp.Start()
	defer wp.Stop()
	enqueuer := NewEnqueuer(ns, cli)

	rk := "wat"
	job, err := enqueuer.EnqueueIn(rk, 300, Q{"a": 1, "b": "cool"})
	assert.Nil(t, err)
	if assert.NotNil(t, job) {
		assert.Equal(t, "wat", job.Name)
		assert.True(t, len(job.ID) > 10)                        // Something is in it
		assert.True(t, job.EnqueuedAt > (time.Now().Unix()-10)) // Within 10 seconds
		assert.True(t, job.EnqueuedAt < (time.Now().Unix()+10)) // Within 10 seconds
		assert.Equal(t, "cool", job.ArgString("b"))
		assert.EqualValues(t, 1, job.ArgInt64("a"))
		assert.NoError(t, job.ArgError())
		assert.EqualValues(t, job.EnqueuedAt+300, job.RunAt)
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
