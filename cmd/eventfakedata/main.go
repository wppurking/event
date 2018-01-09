package main

import (
	"flag"
	"fmt"
	"math/rand"
	"time"

	"github.com/assembla/cony"
	"github.com/wppurking/event"
)

var rabbitMqURL = flag.String("amqp", "amqp://guest:guest@localhost:5672/", "amqp url")
var namespace = flag.String("ns", "work", "namespace")

func epsilonHandler(job *event.Message) error {
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

	enq := event.NewPublisher(*namespace, cony.URL(*rabbitMqURL))
	// Publish some jobs:
	go enqueues(enq)

	wp := event.NewWorkerPool(context{}, 5, *namespace, enq, cony.URL(*rabbitMqURL))
	wp.ConsumerWithOptions("foobar", event.ConsumerOptions{MaxFails: 3, Prefetch: 30}, epsilonHandler)
	//wp.Message("foobar", epsilonHandler)
	wp.Start()

	select {}
}

func enqueues(en *event.Publisher) {
	for {
		for i := 0; i < 20; i++ {
			_, err := en.Publish("foobar", event.Q{"i": i})
			if err != nil {
				fmt.Println(err)
			}
		}

		time.Sleep(1 * time.Second)
	}
}
