package pubsub

import (
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
