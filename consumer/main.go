package main

import (
	"context"
	"log"
	"fmt"
	"github.com/segmentio/kafka-go"
)

func main() {
    // Define the Kafka broker address and topic
    topic := "wikipedia.changes"
    groupID := "wiki"

    // Create a new Kafka reader with the specified broker and topic
    r := kafka.NewReader(kafka.ReaderConfig{
        Brokers:   []string{"127.0.0.1:9092"},
        GroupID:   groupID,
        Topic:     topic,
        MaxBytes:  10e6, // 10MB
		StartOffset: kafka.FirstOffset,
    })

    fmt.Println("Kafka consumer started...")

    // Start reading messages
    for {
        msg, err := r.ReadMessage(context.Background())
        if err != nil {
            log.Printf("Error while reading message: %v", err)
            break
        }
        fmt.Printf("Received message at offset %d: %s = %s\n", msg.Offset, string(msg.Key), string(msg.Value))
    }

    // Close the reader
    if err := r.Close(); err != nil {
        log.Fatal("Failed to close reader:", err)
    }
}