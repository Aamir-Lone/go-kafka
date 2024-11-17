// Updated producer code for modularity with multiple topics and partitions
package main

import (
	"bufio"
	"fmt"
	"os"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "localhost:9092"})
	if err != nil {
		panic(err)
	}
	defer p.Close()

	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Delivery failed: %v\n", ev.TopicPartition)
				} else {
					fmt.Printf("Delivered message to %v\n", ev.TopicPartition)
				}
			}
		}
	}()

	scanner := bufio.NewScanner(os.Stdin)
	fmt.Println("Enter the topic name (type 'exit' to quit):")
	fmt.Print("> ")
	if scanner.Scan() {
		topic := scanner.Text()
		if topic == "exit" {
			return
		}

		fmt.Printf("Sending messages to topic '%s' (type 'exit' to quit):\n", topic)
		for scanner.Scan() {
			input := scanner.Text()
			if input == "exit" {
				break
			}
			message := kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
				Value:          []byte(input),
			}
			p.Produce(&message, nil)
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Printf("Error reading input: %v\n", err)
	}
}
