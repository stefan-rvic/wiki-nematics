package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/segmentio/kafka-go"
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

	destination.Value = valueBytes

	return nil
}
