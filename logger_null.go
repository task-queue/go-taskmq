package taskmq

type LoggerNull struct{}

func (l LoggerNull) Print(...interface{}) {}

func (l LoggerNull) Println(...interface{}) {}

func (l LoggerNull) Printf(m string, f ...interface{}) {}
