package main

import (
	"flag"
	"fmt"
	"math/rand"
	"time"

	"github.com/assembla/cony"
	"github.com/wppurking/event"
)

var rabbitMqURL = flag.String("amqp", "amqp://guest:guest@localhost:5672/event", "amqp url")
var namespace = flag.String("ns", "work", "namespace")

func (c *context) epsilonHandler(job *event.Message) error {
	fmt.Println("epsilon")
	fmt.Println(string(job.Body))
	time.Sleep(200 * time.Millisecond)

	if rand.Intn(2) == 0 {
		return fmt.Errorf("random error")
	}
	return nil
}

type context struct{}

var routingKey = "event.main"

func main() {
	flag.Parse()
	fmt.Println("Installing some fake data")

	enq := event.NewPublisher(*namespace, cony.URL(*rabbitMqURL))
	//Publish some jobs:
	go enqueues(enq)

	wp := event.NewWorkerPool(context{}, 5, *namespace, enq, cony.URL(*rabbitMqURL))
	//opts := event.ConsumerOptions{MaxFails: 3, Prefetch: 30, QueueName:"queue_name"}
	//wp.ConsumerWithOptions(routingKey, opts, epsilonHandler)
	wp.Consumer(routingKey, (*context).epsilonHandler)
	wp.Start()

	select {}
}

func enqueues(en *event.Publisher) {
	for {
		for i := 0; i < 20; i++ {
			_, err := en.Publish(routingKey, event.Q{"i": i})
			//_, err := en.PublishIn(routingKey, rand.Int63n(100), event.Q{"i": i})
			if err != nil {
				fmt.Println(err)
			}
		}

		time.Sleep(1 * time.Second)
	}
}
