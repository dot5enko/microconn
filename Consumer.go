package microconn

import (
	"github.com/dot5enko/gobase/errors"
	"github.com/streadway/amqp"
	"runtime/debug"
)

type Consumer struct {
	errors.ErrorNotifier

	deliveries      <-chan amqp.Delivery
	responseHandler DeliveryChannelHandler
	finishedCb      func()
}

func (c *Consumer) OnCompleted(cb func()) {
	c.finishedCb = cb
}

func (c Consumer) Start() {

	defer func() {
		recov := recover()
		if recov != nil {

			errVal, ok := recov.(error)
			if ok {
				c.ErrorNotifier.Notify(errors.CausedError(errVal, "Got error  while executing finish callback for consumer"))
			} else {
				c.ErrorNotifier.Notify(errors.BasicError("Got error  while executing finish callback for consumer: %v", recov))
			}

		}
	}()

	var err error
	for {
		err = eventHandlerWithRecovery(c.deliveries, c.responseHandler)
		if err == nil {
			c.finishedCb()
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
