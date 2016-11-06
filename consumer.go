package taskmq

type callbackFunction func(body []byte) error

type consumer struct {
	Queue  string
	broker IBroker
	logger ILogger
}

func newConsumer(broker IBroker, logger ILogger, queueName string) *consumer {

	c := &consumer{
		Queue:  queueName,
		broker: broker,
	}

	return c
}

func (c *consumer) Listen(fn callbackFunction) error {
	c.broker.InitConsumer(c.Queue)
	for {
		res, err := c.broker.Pop()
		if err != nil {
			c.logger.Println("warn", "Can't get message from queue", c.Queue, err)
			return err
		}

		err = fn(res)
		if err != nil {
			c.logger.Println("warn", "The message wasn't processed by consumer", err)

			if err := c.broker.Nack(); err != nil {
				c.logger.Println("error", "Can't requeue message to queue", c.Queue, err)
			}

			// return origin error
			return err
		}

		err = c.broker.Ack()
		if err != nil {
			c.logger.Println("error", "The message was processed successfully but Broker can't ack this package", err)
			return err
		}
	}
}
