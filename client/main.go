package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

func main() {
	const (
		streamURL = "https://stream.wikimedia.org/v2/stream/recentchange"
		topic     = "wikipedia-changes"
	)

	// Get the port from environment variable first
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	// Set up HTTP server to respond to Cloud Run health checks first
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("Kafka client is running"))
	})

	server := &http.Server{
		Addr:    ":" + port,
		Handler: mux,
	}

	// Start context and signal handling
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Create a channel to signal when HTTP server is ready
	serverReady := make(chan struct{})

	// Start the HTTP server in a goroutine
	go func() {
		log.Printf("Starting HTTP server on port %s", port)
		// Signal that we're about to start listening
		close(serverReady)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("HTTP server error: %v", err)
			cancel() // Cancel context on server error
		}
	}()

	// Wait for server to be ready
	<-serverReady
	log.Printf("HTTP server is ready to accept connections on port %s", port)

	// Now start Kafka-related operations
	brokerEnv := os.Getenv("KAFKA_BROKERS")
	if brokerEnv == "" {
		log.Fatal("KAFKA_BROKERS environment variable is not set")
	}
	brokers := strings.Split(brokerEnv, ",")

	go func() {
		<-sigChan
		log.Println("Shutting down...")
		cancel()
	}()

	changeChan := make(chan *RecentChange, 100)
	sse := NewWikiSseClient(streamURL, changeChan)

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				if err := sse.Start(ctx); err != nil {
					log.Printf("Stream error: %v", err)
					// Don't cancel context here, just log and retry
					time.Sleep(5 * time.Second)
					continue
				}
			}
		}
	}()

	mapper := &RecentChangeMapper{}
	producer := NewMessageProducer[RecentChange](brokers, topic, mapper, changeChan)

	producer.Start(ctx)

	// Wait for shutdown signal
	<-ctx.Done()

	// Gracefully shutdown the HTTP server
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("HTTP server shutdown error: %v", err)
	}
}
