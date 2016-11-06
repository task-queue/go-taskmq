package taskmq

type TaskMQ struct {
	config *Config
	broker IBroker
}

func New(broker IBroker, config *Config) *TaskMQ {
	return &TaskMQ{
		config: config,
		broker: broker,
	}
}

func (r *TaskMQ) Connect() error {

	return r.broker.Connect()
}

func (r *TaskMQ) Publish(queueName string, body []byte) {
	err := r.broker.Push(queueName, body)

	if err != nil {
		panic(err)
	}
}

func (r *TaskMQ) Consume(queueName string, fn callbackFunction) {
	broker := r.broker.Clone()
	c := newConsumer(broker, queueName)
	c.Listen(fn)
}
