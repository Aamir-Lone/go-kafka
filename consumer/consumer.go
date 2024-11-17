// Consumer code to specify topics and partitions, supporting multiple subscriptions
package main

import (
	"fmt"
	"strings"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"group.id":          "my-group",
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		panic(err)
	}
	defer c.Close()

	var topics string
	fmt.Println("Enter the topic names separated by commas:")
	fmt.Print("> ")
	fmt.Scanln(&topics)
	topicList := strings.Split(topics, ",")

	err = c.SubscribeTopics(topicList, nil)
	if err != nil {
		panic(err)
	}

	fmt.Println("Subscribed to topics:", topicList)
	fmt.Println("Listening for messages (Press Ctrl+C to stop)...")

	for {
		msg, err := c.ReadMessage(-1)
		if err != nil {
			fmt.Printf("Consumer error: %v (%v)\n", err, msg)
			continue
		}
		fmt.Printf("Received message on %s: %s\n", msg.TopicPartition, string(msg.Value))
	}
}
