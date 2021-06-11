package microconn

import (
	"crypto/md5"
	"fmt"
	"github.com/google/uuid"
	"github.com/streadway/amqp"
	"log"
	"github.com/dot5enko/gobase/errors"
)

type Rmq struct {
	errors.ErrorNotifier

	amqp     *amqp.Connection
	channel  *amqp.Channel
	notifier func(err error)
	config   RmqConfig
	subid    string
}

type RmqConfig struct {
	Host     string `json:"host"`
	Port     int    `json:"port"`
	Username string `json:"username"`
	Password string `json:"password"`

	ImmediateSend bool `json:"immediate_send"`
	MandatorySend bool `json:"mandatory_send"`
	AutoAck       bool `json:"auto_ack"`
}

type DeliveryChannelHandler func(delivery <-chan amqp.Delivery)

func (receiver *Rmq) Connect(config RmqConfig) error {
	receiver.config = config

	url := fmt.Sprintf("amqp://%s:%s@%s:%d/", config.Username, config.Password, config.Host, config.Port)
	conn, err := amqp.Dial(url)

	if err != nil {
		return errors.CausedError(err, "Unable to connect to rmq")
	}

	receiver.amqp = conn
	receiver.channel, err = receiver.amqp.Channel()
	receiver.subid = fmt.Sprintf("%x", md5.Sum([]byte(uuid.New().String())))[0:8]

	if err != nil {
		return errors.CausedError(err, "Unable to create channel")
	}

	return nil
}

func (receiver *Rmq) ConsumeAs(consumerName, from string, responseHandler DeliveryChannelHandler) (err error, con Consumer) {

	_, err = receiver.channel.QueueDeclare(consumerName, true, false, false, true, nil)
	if err != nil {
		err = errors.CausedError(err, "Unable to create a queue to consume from `%s`", from)
		return
	} else {
		err = receiver.channel.QueueBind(consumerName, "", from, true, nil)
		if err != nil {
			err = errors.CausedError(err, "Unable to bind a queue")
			return
		}
	}
	var deliveries <-chan amqp.Delivery
	deliveries, err = receiver.channel.Consume(consumerName, consumerName, receiver.config.AutoAck, true, false, false, nil)
	if err != nil {
		err = errors.CausedError(err, "Unable to start consuming")
		return
	}

	con.SetNotifier(receiver.notifier)
	con.deliveries = deliveries
	con.responseHandler = responseHandler

	return
}

func (receiver *Rmq) SendTo(exchange string, routingKey string, bytes []byte) error {

	msg := amqp.Publishing{
		Body: bytes,
	}

	result := receiver.channel.Publish(exchange, routingKey,
		receiver.config.MandatorySend,
		receiver.config.ImmediateSend, msg,
	)

	if result != nil {
		log.Printf("Error sending message to exchange `%s`", exchange)
	} else {
		log.Printf("successfully sent a message to exchange `%s`: %s", exchange, routingKey)
	}

	return result
}
