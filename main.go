package main

import (
	"fmt"
	"github.com/dot5enko/microconn/rmq"
	"runtime"
	"time"
)

func main() {

	conf := rmq.ConfigStruct{
		Rabbitmq: rmq.RabbitmqStruct{
			Host:     "localhost",
			Port:     5672,
			Login:    "guest",
			Password: "guest",
		},
		Database:     rmq.DatabaseConfig{},
		QueueName:    "",
		ExchangeName: "test",
	}

	go func() {
		rmqVal := rmq.NewMq("bob")

		err, msgs := rmqVal.Start(conf)
		if err != nil {
			fmt.Println(err.Error())
		}

		for msg := range msgs {

			fmt.Printf("[%s] Got a message in --> %s from %s\n", rmqVal.Me(), string(msg.Content), msg.Raw.ReplyTo)
			rmqVal.Send(msg.Raw.ReplyTo,rmq.MqMessage{Content: []byte("hi, this is response")})
		}
	}()

	go func() {
		rmqVal := rmq.NewMq("alice")

		err, msgs := rmqVal.Start(conf)
		if err != nil {
			fmt.Println(err.Error())
		}

		go func() {
			for {
				rmqVal.Send("bob", rmq.MqMessage{Content: []byte("hola")})
				time.Sleep(time.Second * 5)
			}
		}()

		for msg := range msgs {
			fmt.Printf("[%s] Got a message in --> %s from %s\n", rmqVal.Me(), string(msg.Content),msg.Raw.ReplyTo)
		}
	}()

	for {
		runtime.Gosched()
		time.Sleep(time.Millisecond * 100)
	}

}
