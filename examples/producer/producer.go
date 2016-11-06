package main

import (
	"flag"
	"strconv"
	"time"

	"gopkg.in/redis.v5"
	"github.com/task-queue/go-taskmq"
	"github.com/task-queue/go-taskmq/broker/redis"
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
		panic(err)
	}

	i := 0

	for true {
		i++
		r.Publish(*queueName, []byte("Ping command "+strconv.Itoa(i)))
		if *sleepMilleseconds > 0 {
			time.Sleep(time.Duration(*sleepMilleseconds) * time.Millisecond)
		}
	}
}
