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

func createProducer(broker string) (sarama.AsyncProducer, error) {
	config := sarama.NewConfig()

	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	config.Producer.RequiredAcks = sarama.WaitForAll

	producer, err := sarama.NewAsyncProducer([]string{broker}, config)
	if err != nil {
		log.Fatalf("Error creating producer: %v", err)
	}

	go func() {
		for msg := range producer.Successes() {
			fmt.Printf("[SUCCESS] Message sent to topic %s [partition: %d, offset: %d]\n", msg.Topic, msg.Partition, msg.Offset)
		}
	}()

	go func() {
		for err := range producer.Errors() {
			log.Printf("[ERROR] Failed to send message: %v\n", err)
		}
	}()

	messages := []string{"Staryu evolved into Starmie!"}

	go func() {
		for _, pokemon := range messages {
			msg := &sarama.ProducerMessage{
				Topic: "Pokemon",
				Value: sarama.StringEncoder(pokemon),
			}

			producer.Input() <- msg
			fmt.Printf("Pushed %s to producer channel\n", pokemon)
			time.Sleep(500 * time.Millisecond)
		}
	}()

	return producer, nil
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

	producer, err := createProducer(broker)
	if err != nil {
		log.Fatalf("Producer error: %v", err)
	}
	defer producer.AsyncClose()

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)
	<-sigchan

	fmt.Println("Shutting down...")
}
