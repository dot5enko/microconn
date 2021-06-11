package microconn

import (
	"github.com/streadway/amqp"
	"log"
	"runtime/debug"
	"github.com/dot5enko/gobase/errors"
)

type Consumer struct {
	errors.ErrorNotifier

	deliveries      <-chan amqp.Delivery
	responseHandler DeliveryChannelHandler
}

func (c Consumer) Start() {
	var err error
	for {
		err = eventHandlerWithRecovery(c.deliveries, c.responseHandler)
		if err == nil {
			log.Printf("consumer finished ")
			return
		} else {
			c.Notify(errors.CausedError(err, "got a recovery on consumer"))
		}
	}
}

func eventHandlerWithRecovery(deliveries <-chan amqp.Delivery, rhandler DeliveryChannelHandler) (err error) {

	defer func() {
		if x := recover(); x != nil {
			debug.PrintStack()
			err = errors.BasicError("Recovered event handling routine : %v", x)
		}
	}()

	rhandler(deliveries)

	return

}
