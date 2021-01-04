package rmq

import (
	"fmt"
	"github.com/streadway/amqp"
	"log"
	"strconv"
	"time"
)

type ConfigStruct struct {
	Rabbitmq     RabbitmqStruct `json:"rabbitmq"`
	Database     DatabaseConfig `json:"database"`
	QueueName    string         `json:"queue_name"`
	ExchangeName string         `json:"exchange_name"`
}

type RabbitmqStruct struct {
	Host     string `json:"host"`
	Port     int    `json:"port"`
	Login    string `json:"login"`
	Password string `json:"password"`
}

type DatabaseConfig struct {
	Host     string `json:"host"`
	Port     int    `json:"port"`
	Username string `json:"username"`
	Dbname   string `json:"dbname"`
	Password string `json:"password"`
	Debug    bool
}

func (conf ConfigStruct) Test() error {
	return nil
}

type mq struct {
	me   string
	conn *amqp.Connection
	ch   *amqp.Channel

	sendChannel chan MqMessage
}

func (r mq) Me() string {
	return r.me
}

func NewMq(consumerName string) mq {
	mq := mq{
		me:          consumerName,
		sendChannel: make(chan MqMessage, 1),
	}

	return mq
}

type MqMessage struct {
	Dest    string
	Content []byte
	Raw     *amqp.Delivery
}

func (mq mq) Send(message MqMessage) {
	mq.sendChannel <- message
}

func (mq mq) sendInternal(message MqMessage) error {

	var pub amqp.Publishing
	pub.Body = message.Content
	pub.ReplyTo = mq.me

	result := mq.ch.Publish(message.Dest, "", true, false, pub)
	return result
}

func (mq *mq) Start(conf ConfigStruct) (error, chan MqMessage) {

	curStep := 1
	maxSteps := 10
	interpolationAcceleration := 0.4
	stepInterpolationCoeff := 1 + interpolationAcceleration

	var err error

	conf.ExchangeName = mq.me
	err, mq.conn, mq.ch = mq.Connect(conf)

	if err != nil {
		return fmt.Errorf("unable to connect to rabbit instance : %s", err.Error()), nil
	}

	autoAck := true

	msgs, err := mq.ch.Consume(
		conf.QueueName, // queue
		"",             // consumer
		autoAck,        // auto ack
		false,          // exclusive
		false,          // no local
		false,          // no wait
		nil,            // args
	)

	if err != nil {

		amqpErr, isAmqpError := err.(*amqp.Error)
		if !isAmqpError {
			return err, nil
		}

		for {
			if amqpErr.Code == 404 {
				curDelayBeforeRetry := stepInterpolationCoeff * float64(curStep)

				fmt.Printf("... %s retry with delay %f\n", amqpErr.Reason, curDelayBeforeRetry)
				time.Sleep(time.Second * time.Duration(curDelayBeforeRetry))
				curStep++
				if curStep >= maxSteps {
					curStep = maxSteps
				} else {
					stepInterpolationCoeff += interpolationAcceleration
				}
				mq.conn.Close()
				return amqpErr, nil
			} else {
				mq.conn.Close()
				return fmt.Errorf("unable to start consuming from q %s : %s (%d)", conf.QueueName, err.Error(), 0), nil
			}
		}
	}

	log.Printf("Start receiving order logs\n")

	result := make(chan MqMessage, 10)

	go func() {

		fmt.Printf("mq receive routine started\n")

		for d := range msgs {

			result <- MqMessage{
				Content: d.Body,
				Raw:     &d,
			}

			if !autoAck {
				ackErr := d.Ack(false)
				if ackErr != nil {
					log.Printf("Unable to ack some message: %s\n", err.Error())
				}
			}
		}
	}()

	// send routine
	go func() {
		fmt.Printf("mq send routine started\n")
		for msg := range mq.sendChannel {
			mq.sendInternal(msg)
		}
	}()

	return nil, result
}

func (mq) Connect(conf ConfigStruct) (error, *amqp.Connection, *amqp.Channel) {

	dsn := conf.Rabbitmq.Login + ":" + conf.Rabbitmq.Password + "@" + conf.Rabbitmq.Host + ":" + strconv.Itoa(conf.Rabbitmq.Port)
	dsnfull := "amqp://" + dsn + "/"
	log.Printf("Connecting to  rabbit %s\n", dsnfull)
	conn, err := amqp.Dial(dsnfull)

	if err != nil {
		log.Printf("Unable to dial to rabbit server %s : %s\n", dsnfull, err.Error())
		return err, nil, nil
	}

	ch, err := conn.Channel()
	if err != nil {
		log.Printf("Error creating rabbit channel : %s\n", err.Error())
		return err, nil, nil
	}

	err = ch.ExchangeDeclare(
		conf.ExchangeName, // name
		"direct",          // type
		false,             // durable
		false,             // auto-deleted
		false,             // internal
		false,             // no-wait
		nil,               // arguments
	)
	if err != nil {
		log.Printf("Error declaring rabbit exchange (%s): %s\n", conf.ExchangeName, err.Error())
		return err, nil, nil
	}

	q, err := ch.QueueDeclare(
		"",    // name
		false, // durable
		false, // delete when unused
		true,  // exclusive
		false, // no-wait
		nil,   // arguments
	)

	log.Printf("declared a queue: %s\n", q.Name)

	if err != nil {
		log.Printf("Error declaring rabbit queue : %s\n", err.Error())
		return err, nil, nil
	}

	err = ch.QueueBind(
		q.Name,            // queue name
		"",                // routing key
		conf.ExchangeName, // exchange
		false,
		nil)

	if err != nil {
		log.Printf("Error binding rabbit queue: %s\n", err.Error())
		return err, nil, nil
	}

	return nil, conn, ch
}
