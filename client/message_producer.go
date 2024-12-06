package main

import (
	"context"
	"github.com/segmentio/kafka-go"
	"log"
	"os"
	"time"
)

type MessageProducer[T any] struct {
	writer *kafka.Writer
	logger *log.Logger
	mapper Mapper[T, kafka.Message]
	input  chan *T
}

func NewMessageProducer[T any](
	brokers []string,
	topic string,
	mapper Mapper[T, kafka.Message],
	input chan *T) *MessageProducer[T] {

	return &MessageProducer[T]{
		writer: &kafka.Writer{
			Addr:         kafka.TCP(brokers...),
			Topic:        topic,
			WriteTimeout: 10 * time.Second,
			Logger:       kafka.LoggerFunc(log.Printf),
			ErrorLogger:  kafka.LoggerFunc(log.Printf),
			Async:        true,
		},
		mapper: mapper,
		input:  input,
		logger: log.New(os.Stdout, "[KafkaProducer] ", log.LstdFlags),
	}
}

func (mp *MessageProducer[T]) Start(ctx context.Context) {
	for {
		select {
		case msg := <-mp.input:
			if err := mp.sendChange(ctx, msg); err != nil {
				mp.logger.Printf("Failed to send message to queue: %v", err)
				continue
			}
		case <-ctx.Done():
			mp.logger.Println("Shutting down producer...")
			return
		}
	}
}

func (mp *MessageProducer[T]) sendChange(ctx context.Context, msg *T) error {
	var kafkaMsg kafka.Message

	if err := mp.mapper.Map(msg, &kafkaMsg); err != nil {
		return err
	}

	return mp.writer.WriteMessages(ctx, kafkaMsg)
}
