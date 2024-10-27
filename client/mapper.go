package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/segmentio/kafka-go"
	"time"
)

type Mapper[From any, To any] interface {
	Map(source *From, destination *To) error
}

type RecentChangeMapper struct{}

func (m *RecentChangeMapper) Map(source *RecentChange, destination *kafka.Message) error {
	if source == nil || destination == nil {
		return errors.New("neither source nor destination can be nil")
	}

	valueBytes, err := json.Marshal(source)
	if err != nil {
		return fmt.Errorf("marshal error: %w", err)
	}

	destination.Key = []byte(source.Wiki)
	destination.Value = valueBytes
	destination.Time = time.Now()
	destination.Headers = []kafka.Header{
		{Key: "type", Value: []byte(source.Type)},
		{Key: "domain", Value: []byte(source.Meta.Domain)},
	}

	return nil
}
