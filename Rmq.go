package microconn

import (
	"crypto/md5"
	"fmt"

	"github.com/dot5enko/gobase/errors"
	"github.com/google/uuid"
	"github.com/streadway/amqp"
)

type Rmq struct {
	errors.ErrorNotifier

	amqp     *amqp.Connection
	channel  *amqp.Channel
	notifier func(err error)
	config   RmqConfig
	subid    string

	Options       RmqOptions
	closeNotifier func(err *amqp.Error)
}

var DefaultRmqOptions RmqOptions = RmqOptions{ReplyTo: ""}

func New() Rmq {
	result := Rmq{}

	result.Options = DefaultRmqOptions

	return result
}

type RmqOptions struct {
	ReplyTo string
}

type RmqConfig struct {
	Host     string `json:"host"`
	Port     int    `json:"port"`
	Username string `json:"username"`
	Password string `json:"password"`
	Vhost    string `json:"vhost"`

	ImmediateSend bool `json:"immediate_send"`
	MandatorySend bool `json:"mandatory_send"`
	AutoAck       bool `json:"auto_ack"`
	//ReconnectDelaySeconds int  `json:"reconnect_delay_sec"`
}

type DeliveryChannelHandler func(delivery <-chan amqp.Delivery)
type DeliveryHandler func(delivery amqp.Delivery)

func (receiver *Rmq) RawHandle() *amqp.Connection {
	return receiver.amqp
}

func (receiver *Rmq) RawChannel() *amqp.Channel {
	return receiver.channel
}
func (receiver *Rmq) Disconnect() error {
	return receiver.channel.Close()
}
func (receiver *Rmq) Connect(config RmqConfig) error {
	receiver.config = config

	// todo substitute with DialConfig
	url := fmt.Sprintf("amqp://%s:%s@%s:%d/%s", config.Username, config.Password, config.Host, config.Port, config.Vhost)
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

	if receiver.closeNotifier != nil {
		go func() {
			notifier := receiver.channel.NotifyClose(make(chan *amqp.Error))
			not := <-notifier

			if not != nil {
				if receiver.closeNotifier != nil {
					receiver.closeNotifier(not)
				}
			}
		}()
	}

	return nil
}
func (receiver *Rmq) OnConnClose(cb func(err *amqp.Error)) {
	receiver.closeNotifier = cb
}

func (receiver *Rmq) ConsumeDirect(consumerName, from string, responseHandler DeliveryHandler) (err error, con Consumer) {

	_, err = receiver.channel.QueueDeclare(consumerName, false, true, true, true, nil)
	if err != nil {
		err = errors.CausedError(err, "Unable to create a queue to consume from `%s`", from)
		return
	} else {
		err = receiver.channel.QueueBind(consumerName, consumerName, from, true, nil)
		if err != nil {
			err = errors.CausedError(err, "Unable to bind a queue")
			return
		}
	}

	con.consumerName = consumerName
	con.channel = receiver.channel
	con.config = receiver.config

	con.SetNotifier(receiver.notifier)
	con.responseHandler = responseHandler

	return
}

func (receiver *Rmq) ConsumeAs(consumerName, from string, responseHandler DeliveryHandler) (err error, con Consumer) {

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

	con.consumerName = consumerName
	con.channel = receiver.channel
	con.config = receiver.config

	// todo get rid of this
	con.SetNotifier(receiver.notifier)
	con.responseHandler = responseHandler

	return
}

func (receiver *Rmq) SendTo(exchange string, routingKey string, bytes []byte) error {
	return receiver.SendToWithHeaders(exchange, routingKey, bytes, nil)
}
func (receiver *Rmq) SendToWithHeaders(exchange string, routingKey string, bytes []byte, headers map[string]interface{}) error {

	msg := amqp.Publishing{
		Body:    bytes,
		Headers: headers,
	}

	// todo use more performant way of doing this:
	// make a  publishing once, and then just substitute Body bytes

	if receiver.Options.ReplyTo != "" {
		msg.ReplyTo = receiver.Options.ReplyTo
	}

	result := receiver.channel.Publish(exchange, routingKey,
		receiver.config.MandatorySend,
		receiver.config.ImmediateSend, msg,
	)

	return result
}
