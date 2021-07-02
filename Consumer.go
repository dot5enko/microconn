package microconn

import (
	"github.com/dot5enko/gobase/errors"
	"github.com/streadway/amqp"
	//"runtime/debug"
)

type Consumer struct {
	errors.ErrorNotifier

	deliveries      <-chan amqp.Delivery
	responseHandler DeliveryHandler
	finishedCb      func()
	consumerName    string
	channel         *amqp.Channel
	config          RmqConfig
}

func (c *Consumer) OnCompleted(cb func()) {
	c.finishedCb = cb
}

func (c *Consumer) ConsumeInternal() (err error) {
	c.deliveries, err = c.channel.Consume(c.consumerName, c.consumerName, c.config.AutoAck, true, false, false, nil)
	if err != nil {
		err = errors.CausedError(err, "Unable to start consuming")
	}
	return
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

	c.ConsumeInternal()

	//var err error

	for x := range c.deliveries {
		c.responseHandler(x)
	}
	c.finishedCb()

	//for {
	//err = eventHandlerWithRecovery(c.deliveries,)
	//if err == nil {
	//	c.finishedCb()
	//	return
	//} else {
	//	c.Notify(errors.CausedError(err, "got a recovery on consumer"))
	//}
	//}
}

//func eventHandlerWithRecovery(deliveries <-chan amqp.Delivery, rhandler DeliveryHandler) (err error) {
//
//	defer func() {
//		if x := recover(); x != nil {
//			debug.PrintStack()
//			err = errors.BasicError("Recovered event handling routine : %v", x)
//		}
//	}()
//
//	rhandler(deliveries)
//
//	return
//
//}
