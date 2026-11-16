package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/IBM/sarama"
	"github.com/jackc/pgx/v5/pgxpool"
)

func initDB(ctx context.Context, connStr string) (*pgxpool.Pool, error) {
	pool, err := pgxpool.New(ctx, connStr)
	if err != nil {
		return nil, fmt.Errorf("unable to create connection pool: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("unable to ping database: %w", err)
	}

	schema := `
	CREATE TABLE IF NOT EXISTS kafka_messages (
		id BIGSERIAL PRIMARY KEY,
		topic TEXT NOT NULL,
		partition INT NOT NULL,
		kafka_offset BIGINT NOT NULL,
		msg_key TEXT,
		msg_value TEXT NOT NULL,
		headers JSONB,
		msg_timestamp TIMESTAMPTZ NOT NULL,
		created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
		UNIQUE (topic, partition, kafka_offset)
	);`

	if _, err := pool.Exec(ctx, schema); err != nil {
		pool.Close()
		return nil, fmt.Errorf("failed to initialize schema: %w", err)
	}

	slog.Info("Connected to PostgreSQL and ensured schema exists")
	return pool, nil
}

func createTopic(broker string, topicName string, partitions int32, replicationFactor int16) error {
	config := sarama.NewConfig()

	admin, err := sarama.NewClusterAdmin([]string{broker}, config)
	if err != nil {
		return fmt.Errorf("failed to create cluster admin: %w", err)
	}
	defer admin.Close()

	topicDetail := &sarama.TopicDetail{
		NumPartitions:     partitions,
		ReplicationFactor: replicationFactor,
	}

	err = admin.CreateTopic(topicName, topicDetail, false)
	if err != nil {
		if err == sarama.ErrTopicAlreadyExists {
			slog.Info("Topic already exists", slog.String("topic", topicName))
			return nil
		}
		return fmt.Errorf("failed to create topic: %w", err)
	}
	slog.Info("Topic created successfully", slog.String("topic", topicName))
	return nil
}

func createProducer(broker string, wg *sync.WaitGroup) (sarama.AsyncProducer, error) {
	config := sarama.NewConfig()

	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	config.Producer.Idempotent = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Net.MaxOpenRequests = 1

	producer, err := sarama.NewAsyncProducer([]string{broker}, config)
	if err != nil {
		return nil, fmt.Errorf("error creating producer: %w", err)
	}

	wg.Add(2)
	go func() {
		defer wg.Done()

		for msg := range producer.Successes() {
			slog.Info("Message sent successfully",
				slog.String("topic", msg.Topic),
				slog.Int("partition", int(msg.Partition)),
				slog.Int64("offset", msg.Offset),
			)
		}
	}()

	go func() {
		defer wg.Done()

		for err := range producer.Errors() {
			slog.Error("Failed to send message", slog.Any("error", err))
		}
	}()

	return producer, nil
}

type Consumer struct {
	db *pgxpool.Pool
}

func (c *Consumer) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

func (c *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (c *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	const insertQuery = `
		INSERT INTO kafka_messages (topic, partition, kafka_offset, msg_key, msg_value, headers, msg_timestamp)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (topic, partition, kafka_offset) DO NOTHING;`

	for {
		select {
		case message, ok := <-claim.Messages():
			if !ok {
				return nil
			}

			headers := make(map[string]string, len(message.Headers))
			for _, header := range message.Headers {
				headers[string(header.Key)] = string(header.Value)
			}
			headersJSON, _ := json.Marshal(headers)

			var msgKey *string
			if message.Key != nil {
				k := string(message.Key)
				msgKey = &k
			}

			_, err := c.db.Exec(
				session.Context(),
				insertQuery,
				message.Topic,
				message.Partition,
				message.Offset,
				msgKey,
				string(message.Value),
				headersJSON,
				message.Timestamp,
			)
			if err != nil {
				slog.Error("Failed to persist message to Postgres",
					slog.Any("error", err),
					slog.Int64("offset", message.Offset),
				)
				continue
			}

			slog.Info("Message persisted to DB",
				slog.String("topic", message.Topic),
				slog.Int("partition", int(message.Partition)),
				slog.Int64("offset", message.Offset),
				slog.String("key", string(message.Key)),
				slog.String("value", string(message.Value)),
			)

			session.MarkMessage(message, "")
		case <-session.Context().Done():
			return nil
		}
	}
}

func startConsumer(ctx context.Context, broker string, group string, topics []string, db *pgxpool.Pool, wg *sync.WaitGroup) (sarama.ConsumerGroup, error) {
	config := sarama.NewConfig()
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	client, err := sarama.NewConsumerGroup([]string{broker}, group, config)
	if err != nil {
		return nil, fmt.Errorf("error creating consumer group client: %w", err)
	}

	consumer := Consumer{db: db}

	wg.Add(1)
	go func() {
		defer wg.Done()

		for {
			if err := client.Consume(ctx, topics, &consumer); err != nil {
				if ctx.Err() != nil {
					return
				}
				slog.Error("Error from consumer", slog.Any("error", err))
			}
			if ctx.Err() != nil {
				return
			}
		}
	}()

	return client, nil
}

func main() {
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown-host"
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		dbURL = "postgres://fluss:fluss_secret@postgres:5432/pokemon_db?sslmode=disable"
	}

	dbPool, err := initDB(ctx, dbURL)
	if err != nil {
		slog.Error("Database initialization failed", slog.Any("error", err))
		os.Exit(1)
	}
	defer dbPool.Close()

	wg := &sync.WaitGroup{}

	group := "pokemon-fans"
	broker := os.Getenv("BROKER")
	if broker == "" {
		broker = "kafka:9092"
	}

	slog.Info("Configured to connect to Kafka", slog.String("broker", broker))

	if err := createTopic(broker, "Pokemon", 3, 1); err != nil {
		slog.Error("Error creating topic", slog.Any("error", err))
	}

	producer, err := createProducer(broker, wg)
	if err != nil {
		slog.Error("Producer error", slog.Any("error", err))
		os.Exit(1)
	}

	consumerGroup, err := startConsumer(ctx, broker, group, []string{"Pokemon"}, dbPool, wg)
	if err != nil {
		slog.Error("Consumer error", slog.Any("error", err))
		os.Exit(1)
	}
	defer consumerGroup.Close()

	slog.Info("Sarama consumer up and running...")

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		scanner := bufio.NewScanner(os.Stdin)
		fmt.Println("Enter message (or type 'quit' to exit): ")
		os.Stdout.Sync()

		for scanner.Scan() {
			input := scanner.Text()

			if input == "" {
				fmt.Print("Enter message: ")
				os.Stdout.Sync()
				continue
			}

			if input == "quit" || input == "exit" {
				sigchan <- syscall.SIGINT
				return
			}

			key := extractKey(input)

			msg := &sarama.ProducerMessage{
				Topic:     "Pokemon",
				Timestamp: time.Now(),
				Key:       key,
				Value:     sarama.StringEncoder(input),
				Headers: []sarama.RecordHeader{
					{
						Key:   []byte("producer-host"),
						Value: []byte(hostname),
					},
				},
			}

			producer.Input() <- msg
			slog.Info("Pushed to channel", slog.String("msg", input))

			fmt.Println("Enter message: ")
			os.Stdout.Sync()
		}

		if err := scanner.Err(); err != nil {
			fmt.Fprintln(os.Stderr, "Failed to read input:", err)
		}
	}()

	<-sigchan

	slog.Info("Shutting down...")

	cancel()
	producer.AsyncClose()
	wg.Wait()
}

func extractKey(input string) sarama.Encoder {
	var knownKeys = []string{"pikachu", "charizard", "bulbasaur", "squirtle"}

	lower := strings.ToLower(input)
	for _, key := range knownKeys {
		if strings.Contains(lower, key) {
			return sarama.StringEncoder(key)
		}
	}

	return nil
}
