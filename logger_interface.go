package taskmq

type ILogger interface {
	Print(...interface{})
	Println(...interface{})
	Printf(string, ...interface{})
}
