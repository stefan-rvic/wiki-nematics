package main

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
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

	mechanism := plain.Mechanism{
		Username: "615698213919-compute@developer.gserviceaccount.com",
		Password: "ewogICJ0eXBlIjogInNlcnZpY2VfYWNjb3VudCIsCiAgInByb2plY3RfaWQiOiAid2lraS1uZW1hdGljcyIsCiAgInByaXZhdGVfa2V5X2lkIjogImRhYjZjNjY0NDYwYzUzZGUxYzQ2OTE2NDlmOGVjMDZmYTk5YTkyYmYiLAogICJwcml2YXRlX2tleSI6ICItLS0tLUJFR0lOIFBSSVZBVEUgS0VZLS0tLS1cbk1JSUV2Z0lCQURBTkJna3Foa2lHOXcwQkFRRUZBQVNDQktnd2dnU2tBZ0VBQW9JQkFRQzZ5MW9BUUpnaHQrOUZcbkd0MGFQQkprN1hGUXRSMTVXUE5BNENHQ0FPRUFheVZwMGNXcE1sNzhWUzRTRkpyNlNIQVRNdDgxL2NsSEZaUlRcblBPVjBTMWlVc25VTWY4elZmVWF4T3k4Vmd4bWgrcWlsd2xaQ2ZnTXhVaDFySlN0ejRDUnM5T3FUMWZ3NVZXVWtcbjg4cXpCRVFvR3E2cElhb2NhcGVTTWNVdGM0QUppclpXTldVZ1l1NTJqQUx6ZEtiam9rT0d0MHVVNXl5TXVWdjFcbm5EMjZlYkp4Uk91MGRiaUsxYllPeHRObS9pTGpWeWpmaFB0bllsZGtXMjFrazkxQ2ZIdUs2VUo5d0dSMDh0Q1dcbjRTL0lORlRCTHFnVTVqYURobVB1SW5COUNGeFByNGRrdU1xZ3cyYytyTWp1OE9PRm1ZVW8yYml6M0JpVXZqRDZcbnBIUG9aUHAzQWdNQkFBRUNnZ0VBQnhxS3FWaDJNS1Mvd0Y5ZGZ6ZVU5TlQ1ZitTVFlyVURuZU9vTFRzcFUyRy9cbndpbDNISkIzZjUzTVZic0dOY25NRy83Tm5FNWRlWXJwakNJcHU3ck4wSmN2ZWwwMTIxSHdxVXdqTzBIZXdnUGlcbnBxM0svVlpiZXJteERXSnBmUFpPTnYwNEdvMHR2V1J3L0puRTRLVk40NVNOaGVNV01XUjFyOXFkWEFyT0dMbG1cbmtkclgwMHU0R0MvU1RxQWRrWDd2VHZBMGk5MHpreVhqNGgrNWE4Y1NJL0UxbENKVUR4NVp4NitzaHZwSXUzUkhcbi92S2h3UkZFUTJXdVBYN0p4MW9FY2FIVDZYdWdXV1VMbUwrV1JMbmEydnBJYm5hWGJXV0RaMTBPM0tQc3laRHZcbkN6VU9CaWNmZHhZUmpBWjZXSUliOEtRYnMzUVpTUUtOeWhiM2xUQzB1UUtCZ1FEL0hEVkdHNE9TeFZ2VE9WUkZcbjljVVh4SWZ4Sjl3Ums4OFJ6ck5ZZnVBQUNBNm0wTE1LNkNCeEJZWUE0MEhQSnQ5UkpWZVQzN0FxaHRuWnY4b0JcbmNsWHpIUFVtaytrQW1MOW9oOVpqaTZZdTg5bmhUUGFqazl6WVdjMXRXbWdjcnY0S3dLZzVvMm9RQk5EVC9KTGFcbjYra1RqbmdkV3d1LzdrNEN3QWk4YXUyK1ZRS0JnUUM3Y2lTbTdsSnRaeXh3SGlDaDlCUHdLYUo3TDFack9hNDdcbmZ4MGZGQW53Uno3eVJZVThIN1JTU0ZlUm9tNjlGSWNaMmlqekpvOUpZdFlDN3pnL3hrK1NTcnFJWWY4eE9ERmpcbkprNHRWcXFieFRQVTJFUGhSbE5haDA3dHpUQmgyWTY5cFIyd3JFcXhGSVRTM3JaREMrNFlYby9jcG5QbE9MaUxcbitkSHpIZnJKbXdLQmdRQzQzVFVSdDdCeHRFaWxXTjdqejRSaWc4MUxDT3BsWm1uZ2FwdjJIZ0t1b3lnUzVCdEtcblpRblZQUDV0T0VHaEhuY25jMXJ5VWw5emdjTHVFeGdNVWxGTVdnaWdTd0RHcU9uVGt0UGQwUDI4K29KQnpLYlJcbnhMMTluaDNLQjRCNGdLcWhHaGtObzRpaFVRd1BBZkZkYVNTK1FqaHlkVjZmVjgzNkdqUjZiVFlZL1FLQmdHblpcbk92bEkreUxzY0J1ZjU2Mk10dldYalRraXNobzZxRGpRdnhFZHI3OFBmR3d5OWRuTnpYWHBoQW1wUC85bDZDU2hcbkhSNnhWNWlKUjNEQXhYSzkrWkVTd2VMaDg4bEhnaHdMTlhwRXhuTFFHVVRJR3d6TE9hYVZZZXpIUWRyL2o2dG5cbjRpd3lIcnVBYXNEcEl1TVppWW9aWEFPdHV5UmxzMURYOGNibjF3YWhBb0dCQUorQjhuK0Z1dFZqdGY0NEdYUCtcbmdQaWFDMW5RV2FFMVVxTEdkK3E0eWljN1F4N1ppYVh3K2Fia0VwOFI1YlRIdFN0bDhyTFlCbUFDOENPcWcySHNcbnNCeDZFYW1LaHo2VmdJNWhDZWhyTHd1d0YrWkxENnlFWnNPRks5ZHRuVlBBcXZROEpReHpjUHRBNlAzZkQ0YTZcbmtJNUFydUVTSml6UnhaL3RZWndnREVUUVxuLS0tLS1FTkQgUFJJVkFURSBLRVktLS0tLVxuIiwKICAiY2xpZW50X2VtYWlsIjogIjYxNTY5ODIxMzkxOS1jb21wdXRlQGRldmVsb3Blci5nc2VydmljZWFjY291bnQuY29tIiwKICAiY2xpZW50X2lkIjogIjExNjEyMjc1Njk2NzQ3NTAzODYzNCIsCiAgImF1dGhfdXJpIjogImh0dHBzOi8vYWNjb3VudHMuZ29vZ2xlLmNvbS9vL29hdXRoMi9hdXRoIiwKICAidG9rZW5fdXJpIjogImh0dHBzOi8vb2F1dGgyLmdvb2dsZWFwaXMuY29tL3Rva2VuIiwKICAiYXV0aF9wcm92aWRlcl94NTA5X2NlcnRfdXJsIjogImh0dHBzOi8vd3d3Lmdvb2dsZWFwaXMuY29tL29hdXRoMi92MS9jZXJ0cyIsCiAgImNsaWVudF94NTA5X2NlcnRfdXJsIjogImh0dHBzOi8vd3d3Lmdvb2dsZWFwaXMuY29tL3JvYm90L3YxL21ldGFkYXRhL3g1MDkvNjE1Njk4MjEzOTE5LWNvbXB1dGUlNDBkZXZlbG9wZXIuZ3NlcnZpY2VhY2NvdW50LmNvbSIsCiAgInVuaXZlcnNlX2RvbWFpbiI6ICJnb29nbGVhcGlzLmNvbSIKfQo=",
	}

	sharedTransport := &kafka.Transport{
		SASL: mechanism,
	}

	return &MessageProducer[T]{
		writer: &kafka.Writer{
			Addr:         kafka.TCP(brokers...),
			Topic:        topic,
			WriteTimeout: 10 * time.Second,
			Logger:       kafka.LoggerFunc(log.Printf),
			ErrorLogger:  kafka.LoggerFunc(log.Printf),
			Async:        true,
			Balancer:     &kafka.Hash{},
			Transport:    sharedTransport,
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
