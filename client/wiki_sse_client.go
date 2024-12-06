package main

import (
	"context"
	"encoding/json"
	"github.com/r3labs/sse/v2"
	"log"
	"os"
)

type Length struct {
	Old *int64 `json:"old"`
	New *int64 `json:"new"`
}

type Revision struct {
	Old *int64 `json:"old"`
	New *int64 `json:"new"`
}

type Meta struct {
	URI       string `json:"uri"`
	RequestID string `json:"request_id"`
	ID        string `json:"id"`
	Dt        string `json:"dt"`
	Domain    string `json:"domain"`
	Stream    string `json:"stream"`
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
	Offset    int64  `json:"offset"`
}

type RecentChange struct {
	ID               *int64 `json:"id"`
	Type             string `json:"type"`
	Title            string `json:"title"`
	Namespace        int    `json:"namespace"`
	Comment          string `json:"comment"`
	ParsedComment    string `json:"parsedcomment"`
	Timestamp        int64  `json:"timestamp"`
	User             string `json:"user"`
	Bot              bool   `json:"bot"`
	ServerURL        string `json:"server_url"`
	ServerName       string `json:"server_name"`
	ServerScriptPath string `json:"server_script_path"`
	Wiki             string `json:"wiki"`

	Minor     bool     `json:"minor"`
	Patrolled bool     `json:"patrolled"`
	Length    Length   `json:"length"`
	Revision  Revision `json:"revision"`

	LogID            *int64  `json:"log_id"`
	LogType          *string `json:"log_type"`
	LogAction        string  `json:"log_action"`
	LogParams        any     `json:"log_params"`
	LogActionComment *string `json:"log_action_comment"`

	Meta Meta `json:"meta"`
}

type WikiSseClient struct {
	client *sse.Client
	output chan<- *RecentChange
	logger *log.Logger
}

func NewWikiSseClient(url string, output chan<- *RecentChange) *WikiSseClient {
	return &WikiSseClient{
		client: sse.NewClient(url),
		logger: log.New(os.Stdout, "[WikiSSEClient] ", log.LstdFlags),
		output: output,
	}
}

func (w *WikiSseClient) Start(ctx context.Context) error {
	return w.client.SubscribeRawWithContext(ctx, w.HandleEvent)
}

func (w *WikiSseClient) HandleEvent(msg *sse.Event) {
	if len(msg.Data) == 0 {
		return
	}

	var change RecentChange
	if err := json.Unmarshal(msg.Data, &change); err != nil {
		w.logger.Printf("Error parsing change: %v", err)
		return
	}

	if change.Meta.Domain == "canary" {
		return
	}

	w.output <- &change
}
