package main

import (
	"flag"
	"fmt"
	"math/rand"
	"time"

	"github.com/assembla/cony"
	"github.com/wppurking/work"
)

var rabbitMqURL = flag.String("amqp", "amqp://guest:guest@localhost:5672/", "amqp url")
var namespace = flag.String("ns", "work", "namespace")

func epsilonHandler(job *work.Job) error {
	fmt.Println("epsilon")
	time.Sleep(time.Second)

	if rand.Intn(2) == 0 {
		return fmt.Errorf("random error")
	}
	return nil
}

type context struct{}

func main() {
	flag.Parse()
	fmt.Println("Installing some fake data")

	// Enqueue some jobs:
	go enqueues()

	enq := work.NewEnqueuer(*namespace, cony.URL(*rabbitMqURL))
	wp := work.NewWorkerPool(context{}, 5, *namespace, enq, cony.URL(*rabbitMqURL))
	wp.JobWithOptions("foobar", work.JobOptions{MaxConcurrency: 2}, epsilonHandler)
	wp.Start()

	select {}
}

func enqueues() {
	en := work.NewEnqueuer(*namespace, cony.URL(*rabbitMqURL))
	for {
		for i := 0; i < 20; i++ {
			en.Enqueue("foobar", work.Q{"i": i})
		}

		time.Sleep(1 * time.Second)
	}
}
