package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"github.com/assembla/cony"
	"github.com/wppurking/work"
)

var rabbitMqURL = flag.String("amqp", "amqp://guest:guest@localhost:5672/", "amqp url")
var namespace = flag.String("ns", "work", "namespace")
var jobName = flag.String("job", "", "job name")
var jobArgs = flag.String("args", "{}", "job arguments")

func main() {
	flag.Parse()

	if *jobName == "" {
		fmt.Println("no job specified")
		os.Exit(1)
	}

	var args map[string]interface{}
	err := json.Unmarshal([]byte(*jobArgs), &args)
	if err != nil {
		fmt.Println("invalid args:", err)
		os.Exit(1)
	}

	en := work.NewEnqueuer(*namespace, cony.NewClient(cony.URL(*rabbitMqURL)))
	en.Enqueue(*jobName, args)
}
