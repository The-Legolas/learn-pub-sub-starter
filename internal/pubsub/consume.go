package pubsub

import (
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type SimpleQueueType int

const (
	SimpleQueueDurable SimpleQueueType = iota
	SimpleQueueTransient
)

func DeclareAndBind(
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
) (*amqp.Channel, amqp.Queue, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("could not open unique channel : %v", err)
	}

	isDurable := queueType == SimpleQueueDurable
	isautoDelete := queueType == SimpleQueueTransient
	isexclusive := queueType == SimpleQueueTransient

	newQueue, err := ch.QueueDeclare(
		queueName,    // name
		isDurable,    // durable
		isautoDelete, // delete when unuseds
		isexclusive,  // exclusive
		false,        //  no-wait
		nil,          // args
	)
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("could not declare new queue: %v", err)
	}

	err = ch.QueueBind(newQueue.Name, key, exchange, false, nil)
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("could not bind queue: %v", err)
	}

	return ch, newQueue, nil
}

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType, // an enum to represent "durable" or "transient"
	handler func(T),
) error {

	ch, que, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return fmt.Errorf("could not declare and bind queue: %v", err)
	}

	deliveryChannels, err := ch.Consume(que.Name, "", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("error consuming channel: %v", err)
	}

	go func() {
		for elem := range deliveryChannels {
			fmt.Println("received pause message")
			var message T
			if err := json.Unmarshal(elem.Body, &message); err != nil {
				fmt.Printf("Error unmarshalling JSON: %v", err)
			}
			handler(message)
			elem.Ack(false)
		}
	}()

	return nil

}
