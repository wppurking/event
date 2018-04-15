package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/assembla/cony"
	"github.com/wppurking/event"
)

var rabbitMqURL = flag.String("amqp", "amqp://guest:guest@localhost:5672/event", "amqp url")
var namespace = flag.String("ns", "work", "namespace")
var routingKey = flag.String("rk", "", "job name")
var jobArgs = flag.String("args", "{}", "job arguments")

func main() {
	flag.Parse()

	if *routingKey == "" {
		fmt.Println("no job specified")
		os.Exit(1)
	}

	var args map[string]interface{}
	err := json.Unmarshal([]byte(*jobArgs), &args)
	if err != nil {
		fmt.Println("invalid args:", err)
		os.Exit(1)
	}

	en := event.NewPublisher(*namespace, cony.URL(*rabbitMqURL))
	en.Publish(*routingKey, args)
	time.Sleep(100 * time.Millisecond)
}
