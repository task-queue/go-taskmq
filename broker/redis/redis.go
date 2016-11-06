package broker

import (
	"strconv"
	"time"

	"github.com/task-queue/go-taskmq"
	"gopkg.in/redis.v5"
)

type Redis struct {
	client *redis.Client
	logger taskmq.ILogger

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

func (c *Redis) SetLogger(logger taskmq.ILogger) {
	c.logger = logger
}

func (c Redis) Clone() taskmq.IBroker {

	return &Redis{
		client: c.client,
		logger: c.logger,
	}
}

func (c Redis) Push(queue string, body []byte) error {
	c.debug("Push message to queue", queue)
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

func (c Redis) Ack() error {
	_, err := c.client.RPop(c.unackedQueue).Result()
	return err
}

func (c Redis) Nack() error {
	_, err := c.client.RPopLPush(c.unackedQueue, c.consumeQueue).Result()
	return err
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
		c.debug("Finding dead consumers")
		z := redis.ZRangeBy{
			Max:    strconv.FormatInt(time.Now().Unix()-consumerDieSeconds, 10),
			Offset: 0,
			Count:  -1,
		}

		res, err := c.client.ZRangeByScore(c.heartbeatQueue, z).Result()
		if err != nil {
			c.error(err)
			time.Sleep(15 * time.Second)
			continue
		}

		for _, consumerID := range res {
			c.info("Remove dead conumer", consumerID)
			failQueue := "redismq::" + c.consumeQueue + "::unacked::" + consumerID
			c.debug("Move", failQueue, "to", c.consumeQueue)
			_, err := c.client.RPopLPush(failQueue, c.consumeQueue).Result()
			if err != nil {
				// skip error because failQueue could be empty
				c.warning(err)
			}

			c.debug("Remove conumer from heartbeat", consumerID)
			_, err = c.client.ZRem(c.heartbeatQueue, consumerID).Result()
			if err != nil {
				c.error(err)
			}
		}

		time.Sleep(15 * time.Second)
	}
}

func (c Redis) debug(m ...interface{}) {
	c.logger.Println("debug", m)
}

func (c Redis) info(m ...interface{}) {
	c.logger.Println("info", m)
}

func (c Redis) warning(m ...interface{}) {
	c.logger.Println("warn", m)
}

func (c Redis) error(m ...interface{}) {
	c.logger.Println("error", m)
}
