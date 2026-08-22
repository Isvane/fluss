package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	broker := os.Getenv("BROKER")
	if broker == "" {
		broker = "localhost:9092"
	}

	fmt.Printf("Configured to connect to Kafka at: %s\n", broker)
	time.Sleep(10 * time.Second)

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)
	<-sigchan

	fmt.Println("Shutting down...")
}
