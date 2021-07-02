package microconn

import (
	"github.com/dot5enko/gobase/errors"
	"github.com/streadway/amqp"
	"log"
	"runtime/debug"
	"time"
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

	onClose := c.channel.NotifyClose(make(chan *amqp.Error))
	c.ConsumeInternal()

	var err error

	for {
		select {
		case err = <-onClose:
			if err == nil {
				c.finishedCb()
			} else {
				// todo modify it via config
				time.Sleep(time.Second)
				err = c.ConsumeInternal()
				if err != nil {
					log.Printf("Unable to reconnect. consumer finished explicitly: %s", err.Error())
					break
				}
				// reconnection handling
			}
		case del := <-c.deliveries:
			// todo recover
			c.responseHandler(del)
		}
	}

	//for {
	//	err = eventHandlerWithRecovery(c.deliveries, c.responseHandler)
	//	if err == nil {
	//		c.finishedCb()
	//		return
	//	} else {
	//		c.Notify(errors.CausedError(err, "got a recovery on consumer"))
	//	}
	//}
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
