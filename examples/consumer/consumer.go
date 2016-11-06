package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/task-queue/go-taskmq"
	"github.com/task-queue/go-taskmq/broker/redis"
	"gopkg.in/redis.v5"
)

var queueName *string
var sleepMilleseconds *int
var verbose bool

func init() {
	queueName = flag.String("queue", "task", "a queue name")
	sleepMilleseconds = flag.Int("sleep", 10, "Sleep in millesonds for handle message")

	flag.BoolVar(&verbose, "verbose", false, "Verbose output")
	flag.BoolVar(&verbose, "v", false, "Verbose output")
}

func main() {

	flag.Parse()

	client := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})

	config := &taskmq.Config{
		Logger: log.New(os.Stdout, "", 3),
	}

	mq := taskmq.New(broker.NewRedis(client), config)

	err := mq.Connect()
	if err != nil {
		fmt.Fprintln(os.Stderr, "Error connection to Redis:", err)
		os.Exit(1)
	}

	fmt.Println("Start consuming messages")
	fmt.Printf("    %-23s: %dms\n", "Processing time", *sleepMilleseconds)
	fmt.Printf("    %-23s: %s\n", "Queue name", *queueName)
	fmt.Printf("    %-23s: %v\n", "Verbose mode", verbose)
	fmt.Println("\nPress Ctrl+C to abort")

	mq.Consume(*queueName, func(body []byte) error {
		if verbose {
			fmt.Print("Receive body:", string(body), "... ")
		}
		if *sleepMilleseconds > 0 {
			time.Sleep(time.Duration(*sleepMilleseconds) * time.Millisecond)
		}
		if verbose {
			fmt.Println("done")
		}
		return nil
	})
}
