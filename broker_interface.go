package taskmq

type IBroker interface {
	Connect() error
	Clone() IBroker

	Push(string, []byte) error

	InitConsumer(string) error
	Pop() ([]byte, error)
	Ack() error
	Nack() error

	SetLogger(ILogger)
}
