package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/IBM/sarama"
)

func createTopic(broker string, topicName string, partitions int32, replicationFactor int16) error {
	config := sarama.NewConfig()

	admin, err := sarama.NewClusterAdmin([]string{broker}, config)
	if err != nil {
		return fmt.Errorf("Failed to create cluster admin: %w", err)
	}
	defer admin.Close()

	topicDetail := &sarama.TopicDetail{
		NumPartitions:     partitions,
		ReplicationFactor: replicationFactor,
	}

	err = admin.CreateTopic(topicName, topicDetail, false)
	if err != nil {
		if err == sarama.ErrTopicAlreadyExists {
			fmt.Printf("Topic %q already exist\n", topicName)
			return nil
		}
		return fmt.Errorf("Failed to create topic: %w", err)
	}
	fmt.Printf("Topic %q created successfully\n", topicName)
	return nil
}

func main() {
	broker := os.Getenv("BROKER")
	if broker == "" {
		broker = "kafka:9092"
	}

	fmt.Printf("Configured to connect to Kafka at: %s\n", broker)
	time.Sleep(10 * time.Second)

	if err := createTopic(broker, "Pokemon", 1, 1); err != nil {
		log.Printf("Error creating topic: %v\n", err)
	}

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)
	<-sigchan

	fmt.Println("Shutting down...")
}
