package main

import (
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"github.com/assembla/cony"
	"github.com/gocraft/health"
	"github.com/wppurking/work"
)

var namespace = "bench_test"
var enq *work.Enqueuer

type context struct{}

func epsilonHandler(job *work.Job) error {
	//fmt.Println("hi")
	//a := job.Args[0]
	//fmt.Printf("job: %s arg: %v\n", job.Name, a)
	atomic.AddInt64(&totcount, 1)
	return nil
}

func main() {
	stream := health.NewStream().AddSink(&health.WriterSink{os.Stdout})

	numJobs := 10
	jobNames := []string{}
	rabbitMqURL := "amqp://guest:guest@localhost:5672/ajd"
	enq = work.NewEnqueuer(namespace, cony.NewClient(cony.URL(rabbitMqURL)))

	for i := 0; i < numJobs; i++ {
		jobNames = append(jobNames, fmt.Sprintf("job%d", i))
	}

	job := stream.NewJob("enqueue_all")
	enqueueJobs(jobNames, 10000)
	job.Complete(health.Success)

	workerPool := work.NewWorkerPool(context{}, 20,
		namespace, cony.NewClient(cony.URL(rabbitMqURL)), enq)
	for _, jobName := range jobNames {
		workerPool.Job(jobName, epsilonHandler)
	}
	go monitor()

	job = stream.NewJob("run_all")
	workerPool.Start()
	workerPool.Drain()
	job.Complete(health.Success)
	select {}
}

var totcount int64

func monitor() {
	t := time.Tick(1 * time.Second)

	curT := 0
	c1 := int64(0)
	c2 := int64(0)
	prev := int64(0)

DALOOP:
	for {
		select {
		case <-t:
			curT++
			v := atomic.AddInt64(&totcount, 0)
			fmt.Printf("after %d seconds, count is %d\n", curT, v)
			if curT == 1 {
				c1 = v
			} else if curT == 3 {
				c2 = v
			}
			if v == prev {
				break DALOOP
			}
			prev = v
		}
	}
	fmt.Println("Jobs/sec: ", float64(c2-c1)/2.0)
	os.Exit(0)
}

func enqueueJobs(jobs []string, count int) {
	for _, jobName := range jobs {
		for i := 0; i < count; i++ {
			enq.Enqueue(jobName, work.Q{"i": i})
		}
	}
}
