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
	job, err := enqueuer.Enqueue(name, Q{"a": 1, "b": "cool"})
	assert.Nil(t, err)
	assert.Equal(t, "wat", job.Name)
	assert.True(t, len(job.ID) > 10)                        // Something is in it
	assert.True(t, job.EnqueuedAt > (time.Now().Unix()-10)) // Within 10 seconds
	assert.True(t, job.EnqueuedAt < (time.Now().Unix()+10)) // Within 10 seconds
	assert.Equal(t, "cool", job.ArgString("b"))
	assert.EqualValues(t, 1, job.ArgInt64("a"))
	assert.NoError(t, job.ArgError())

	cn := &consumer{
		que: &cony.Queue{Name: withNS(enqueuer.Namespace, name)},
		jt:  &jobType{Name: name},
		exc: enqueuer.defaultExc,
	}
	cn.start(cli)
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
	cn.Pop()
	cn.Pop()
}

/*
func TestEnqueueIn(t *testing.T) {
	pool := newTestPool(":6379")
	ns := "work"
	cleanKeyspace(ns, pool)
	enqueuer := NewEnqueuer(ns, pool)

	// Set to expired value to make sure we update the set of known jobs
	enqueuer.knownJobs["wat"] = 4

	job, err := enqueuer.EnqueueIn("wat", 300, Q{"a": 1, "b": "cool"})
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

	// Make sure "wat" is in the known jobs
	assert.EqualValues(t, []string{"wat"}, knownJobs(pool, redisKeyKnownJobs(ns)))

	// Make sure the cache is set
	expiresAt := enqueuer.knownJobs["wat"]
	assert.True(t, expiresAt > (time.Now().Unix()+290))

	// Make sure the length of the scheduled job queue is 1
	assert.EqualValues(t, 1, zsetSize(pool, redisKeyScheduled(ns)))

	// Get the job
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
}
*/
