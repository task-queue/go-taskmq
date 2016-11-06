package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/task-queue/go-taskmq"
	"github.com/task-queue/go-taskmq/broker/redis"
	"gopkg.in/redis.v5"
)

var queueName *string
var verbose bool
var sleepMilleseconds *int

func init() {
	queueName = flag.String("queue", "task", "a queue name")
	sleepMilleseconds = flag.Int("sleep", 100, "Sleep in millesonds between messages")

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

	fmt.Println("Start pushing messages")
	fmt.Printf("    %-23s: %dms\n", "Sleep beetween messages", *sleepMilleseconds)
	fmt.Printf("    %-23s: %s\n", "Queue name", *queueName)
	fmt.Printf("    %-23s: %v\n", "Verbose mode", verbose)
	fmt.Println("\nPress Ctrl+C to abort")

	i := 0
	for true {
		i++
		mq.Publish(*queueName, []byte("Ping command "+strconv.Itoa(i)))

		if verbose {
			fmt.Printf("Publish message %d to %q... ", i, *queueName)
		}

		if *sleepMilleseconds > 0 {
			time.Sleep(time.Duration(*sleepMilleseconds) * time.Millisecond)
		}

		if verbose {
			fmt.Println("done")
		}
	}
}
