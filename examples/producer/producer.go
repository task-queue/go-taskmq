package main

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/task-queue/go-taskmq"
	"github.com/task-queue/go-taskmq/broker/redis"
	"gopkg.in/redis.v5"
)

var queueName *string
var sleepMilleseconds *int

func init() {
	queueName = flag.String("queue", "task", "a queue name")
	sleepMilleseconds = flag.Int("sleep", 100, "Sleep in millesonds between messages")
}

func main() {

	flag.Parse()

	client := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	r := taskmq.New(broker.NewRedis(client), nil)

	err := r.Connect()
	if err != nil {
		fmt.Fprintln(os.Stderr, "Error connection to Redis:", err)
		os.Exit(1)
	}

	fmt.Println("Start pushing messages")
	fmt.Printf("    %-23s: %dms\n", "Sleep beetween messages", *sleepMilleseconds)
	fmt.Printf("    %-23s: %s\n", "Queue name", *queueName)
	fmt.Println("\nPress Ctrl+C to abort")

	i := 0
	for true {
		i++
		r.Publish(*queueName, []byte("Ping command "+strconv.Itoa(i)))
		if *sleepMilleseconds > 0 {
			time.Sleep(time.Duration(*sleepMilleseconds) * time.Millisecond)
		}
	}
}
