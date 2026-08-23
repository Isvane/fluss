package main

import (
	"context"
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

type Consumer struct{}

func (c *Consumer) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

func (c *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (c *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case message, ok := <-claim.Messages():
			if !ok {
				return nil
			}

			fmt.Printf("Claimed: value = %s, timestamp = %v, topic = %s\n", string(message.Value), message.Timestamp, message.Topic)
			session.MarkMessage(message, "")
			session.Commit()
		case <-session.Context().Done():
			return nil
		}
	}
}

func startConsumer(ctx context.Context, broker string, group string, topics []string) (sarama.ConsumerGroup, error) {
	config := sarama.NewConfig()
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	client, err := sarama.NewConsumerGroup([]string{broker}, group, config)
	if err != nil {
		return nil, fmt.Errorf("error creating consumer group client: %w", err)
	}

	consumer := Consumer{}

	go func() {
		for {
			if err := client.Consume(ctx, topics, &consumer); err != nil {
				if ctx.Err() != nil {
					return
				}
				log.Printf("Error from consumer: %v\n", err)
			}
			if ctx.Err() != nil {
				return
			}
		}
	}()

	return client, nil
}

func main() {
	group := "pokemon-fans"
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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	consumerGroup, err := startConsumer(ctx, broker, group, []string{"Pokemon"})
	if err != nil {
		log.Fatalf("Consumer error: %v", err)
	}
	defer consumerGroup.Close()

	log.Println("Sarama consumer up and running...")

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)
	<-sigchan

	fmt.Println("Shutting down...")
	cancel()
}
