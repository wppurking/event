package main

import (
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"github.com/assembla/cony"
	"github.com/gocraft/health"
	"github.com/wppurking/event"
)

var namespace = "bench_test"
var enq *event.Publisher

type context struct{}

func epsilonHandler(job *event.Message) error {
	//fmt.Println("hi")
	//a := job.Args[0]
	//fmt.Printf("job: %s arg: %v\n", job.Name, a)
	atomic.AddInt64(&totcount, 1)
	return nil
}

func main() {
	stream := health.NewStream().AddSink(&health.WriterSink{os.Stdout})

	numJobs := 10
	var jobNames []string
	rabbitMqURL := "amqp://guest:guest@localhost:5672/event"
	enq = event.NewPublisher(namespace, cony.URL(rabbitMqURL))

	for i := 0; i < numJobs; i++ {
		jobNames = append(jobNames, fmt.Sprintf("job%d", i))
	}

	job := stream.NewJob("enqueue_all")
	enqueueJobs(jobNames, 10000)
	job.Complete(health.Success)

	workerPool := event.NewWorkerPool(context{}, 20,
		namespace, enq, cony.URL(rabbitMqURL))
	for _, jobName := range jobNames {
		workerPool.Consumer(jobName, epsilonHandler)
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
			enq.Publish(jobName, event.Q{"i": i})
		}
	}
}
