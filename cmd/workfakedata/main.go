package main

import (
	"flag"
	"fmt"
	"time"

	"github.com/assembla/cony"
	"github.com/wppurking/work"
)

var rabbitMqURL = flag.String("amqp", "amqp://guest:guest@localhost:5672/", "amqp url")
var namespace = flag.String("ns", "work", "namespace")

func epsilonHandler(job *work.Message) error {
	fmt.Println("epsilon")
	time.Sleep(time.Second)

	return fmt.Errorf("random error")
	/*
		if rand.Intn(2) == 0 {
			return fmt.Errorf("random error")
		}
		return nil
	*/
}

type context struct{}

func main() {
	flag.Parse()
	fmt.Println("Installing some fake data")

	enq := work.NewEnqueuer(*namespace, cony.URL(*rabbitMqURL))
	// Enqueue some jobs:
	go enqueues(enq)

	wp := work.NewWorkerPool(context{}, 5, *namespace, enq, cony.URL(*rabbitMqURL))
	wp.ConsumerWithOptions("foobar", work.ConsumerOptions{MaxFails: 3, Prefetch: 30}, epsilonHandler)
	//wp.Message("foobar", epsilonHandler)
	wp.Start()

	select {}
}

func enqueues(en *work.Enqueuer) {
	for {
		for i := 0; i < 20; i++ {
			_, err := en.Enqueue("foobar", work.Q{"i": i})
			if err != nil {
				fmt.Println(err)
			}
		}

		time.Sleep(1 * time.Second)
	}
}
