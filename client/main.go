package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	const (
		streamURL = "https://stream.wikimedia.org/v2/stream/recentchange"
		topic     = "wikipedia-changes"
	)

	brokers := []string{"localhost:9092"}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		log.Println("Shutting down...")
		cancel()
	}()
	changeChan := make(chan *RecentChange, 100)

	sse := NewWikiSseClient(streamURL, changeChan)

	go func() {
		if err := sse.Start(ctx); err != nil {
			log.Printf("Stream error: %v", err)
			cancel() // Cancel the context on error
			return
		}
		log.Println("SSE client started.")
	}()

	mapper := &RecentChangeMapper{}
	producer := NewMessageProducer[RecentChange](brokers, topic, mapper, changeChan)

	producer.Start(ctx)
}
