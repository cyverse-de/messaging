package messaging

import (
	"fmt"

	"github.com/streadway/amqp"
)

type AMQPHeaderCarrier amqp.Table

func (ahc AMQPHeaderCarrier) Get(key string) string {
	i := ahc[key]

	switch v := i.(type) {
	case int:
		return fmt.Sprintf("%d", v)
	case string:
		return v
	default:
		return ""
	}
}

func (ahc AMQPHeaderCarrier) Set(key string, value string) {
	ahc[key] = value
}

func (ahc AMQPHeaderCarrier) Keys() []string {
	keys := make([]string, 0, len(ahc))
	for k := range ahc {
		keys = append(keys, k)
	}
	return keys
}
