package broker

import (
	"fmt"
	"strconv"
	"time"

	"github.com/task-queue/go-taskmq"
	"gopkg.in/redis.v5"
)

type Redis struct {
	client *redis.Client

	id int64

	consumeQueue   string
	unackedQueue   string
	heartbeatQueue string
}

func NewRedis(client *redis.Client) *Redis {
	return &Redis{
		client: client,
	}
}

func (c Redis) Connect() error {
	_, err := c.client.Ping().Result()
	return err
}

func (c Redis) Clone() taskmq.IBroker {
	return &Redis{client: c.client}
}

func (c Redis) Push(queue string, body []byte) error {
	cmd := c.client.LPush(queue, string(body))
	_, err := cmd.Result()
	return err
}

func (c *Redis) InitConsumer(queue string) error {
	var err error

	c.id, err = c.client.Incr("redismq::" + queue + "::consumers").Result()
	if err != nil {
		return err
	}

	c.consumeQueue = queue
	c.unackedQueue = "redismq::" + queue + "::unacked::" + strconv.FormatInt(c.id, 10)
	c.heartbeatQueue = "redismq::" + queue + "::heartbeat"

	go c.heartbeat()
	go c.observer()

	return nil
}

func (c Redis) Pop() ([]byte, error) {
	body, err := c.client.BRPopLPush(c.consumeQueue, c.unackedQueue, 0).Result()
	return []byte(body), err
}

func (c Redis) Ack() {
	c.client.RPop(c.unackedQueue)
}

func (c Redis) heartbeat() {
	id := strconv.FormatInt(c.id, 10)

	for {
		z := redis.Z{
			Score:  float64(time.Now().Unix()),
			Member: id,
		}

		c.client.ZAdd(c.heartbeatQueue, z)
		time.Sleep(3 * time.Second)
	}
}

func (c Redis) observer() {

	consumerDieSeconds := int64(15)

	for {
		z := redis.ZRangeBy{
			Max:    strconv.FormatInt(time.Now().Unix()-consumerDieSeconds, 10),
			Offset: 0,
			Count:  -1,
		}
		fmt.Println("Found fails", z)

		res, err := c.client.ZRangeByScore(c.heartbeatQueue, z).Result()
		if err != nil {
			panic(err)
		}

		fmt.Println("Res", res)
		for _, consumerID := range res {
			failQueue := "redismq::" + c.consumeQueue + "::unacked::" + consumerID
			_, err := c.client.RPopLPush(failQueue, c.consumeQueue).Result()
			if err != nil {
				fmt.Printf("\033[0;31m%v\033[0m\n", err)
				// panic(err)
			}
			fmt.Println("REMOVE CONSUMER", consumerID)
			c.client.ZRem(c.heartbeatQueue, consumerID)
		}

		time.Sleep(15 * time.Second)
	}
}
