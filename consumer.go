package taskmq

type callbackFunction func(body []byte) error

type consumer struct {
	Queue  string
	broker IBroker
}

func newConsumer(broker IBroker, queueName string) *consumer {

	c := &consumer{
		Queue:  queueName,
		broker: broker,
	}

	return c
}

func (c *consumer) Listen(fn callbackFunction) {
	c.broker.InitConsumer(c.Queue)
	for {
		res, err := c.broker.Pop()
		if err != nil {
			panic(err)
		}

		err = fn(res)
		if err != nil {
			panic(err)
		}

		c.broker.Ack()
	}
}
