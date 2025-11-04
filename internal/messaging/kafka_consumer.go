package messaging

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog"
	"github.com/segmentio/kafka-go"
	"github.com/shopspring/decimal"

	"github.com/cypherlabdev/data-normalizer-service/internal/models"
)

// RawOddsData represents the raw odds from odds-adapter
type RawOddsData struct {
	ID          uuid.UUID       `json:"id"`
	Provider    string          `json:"provider"`
	EventID     string          `json:"event_id"`
	EventName   string          `json:"event_name"`
	Sport       string          `json:"sport"`
	Competition string          `json:"competition"`
	Market      string          `json:"market"`
	Selection   string          `json:"selection"`
	BackPrice   decimal.Decimal `json:"back_price"`
	LayPrice    decimal.Decimal `json:"lay_price"`
	BackSize    decimal.Decimal `json:"back_size"`
	LaySize     decimal.Decimal `json:"lay_size"`
	Timestamp   time.Time       `json:"timestamp"`
}

// KafkaRawOddsMessage represents the message from odds-adapter
type KafkaRawOddsMessage struct {
	OddsData  []RawOddsData `json:"odds_data"`
	Provider  string        `json:"provider"`
	Timestamp time.Time     `json:"timestamp"`
	BatchID   string        `json:"batch_id"`
}

// KafkaConsumer consumes raw odds from Kafka
type KafkaConsumer struct {
	reader   kafkaReader
	producer Producer
	logger   zerolog.Logger
}

// kafkaReader interface for Kafka reader abstraction
type kafkaReader interface {
	FetchMessage(ctx context.Context) (kafka.Message, error)
	CommitMessages(ctx context.Context, msgs ...kafka.Message) error
	Close() error
	Config() kafka.ReaderConfig
}

// NewKafkaConsumer creates a new Kafka consumer
func NewKafkaConsumer(
	brokers []string,
	topic string,
	groupID string,
	producer Producer,
	logger zerolog.Logger,
) *KafkaConsumer {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		Topic:          topic,
		GroupID:        groupID,
		MinBytes:       1e3,
		MaxBytes:       10e6,
		CommitInterval: 1000,
	})

	return &KafkaConsumer{
		reader:   reader,
		producer: producer,
		logger:   logger.With().Str("component", "kafka_consumer").Logger(),
	}
}

// Start begins consuming messages
func (c *KafkaConsumer) Start(ctx context.Context) error {
	c.logger.Info().
		Str("topic", c.reader.Config().Topic).
		Str("group_id", c.reader.Config().GroupID).
		Msg("started consuming from Kafka")

	for {
		select {
		case <-ctx.Done():
			c.logger.Info().Msg("stopping Kafka consumer")
			return c.reader.Close()

		default:
			msg, err := c.reader.FetchMessage(ctx)
			if err != nil {
				if err == context.Canceled {
					return nil
				}
				c.logger.Error().Err(err).Msg("failed to fetch message")
				continue
			}

			if err := c.processMessage(ctx, msg); err != nil {
				c.logger.Error().
					Err(err).
					Int64("offset", msg.Offset).
					Msg("failed to process message")
				continue
			}

			if err := c.reader.CommitMessages(ctx, msg); err != nil {
				c.logger.Error().Err(err).Msg("failed to commit message")
			}
		}
	}
}

// processMessage processes a single Kafka message
func (c *KafkaConsumer) processMessage(ctx context.Context, msg kafka.Message) error {
	var kafkaMsg KafkaRawOddsMessage
	if err := json.Unmarshal(msg.Value, &kafkaMsg); err != nil {
		return fmt.Errorf("failed to unmarshal message: %w", err)
	}

	c.logger.Debug().
		Int("odds_count", len(kafkaMsg.OddsData)).
		Str("provider", kafkaMsg.Provider).
		Str("batch_id", kafkaMsg.BatchID).
		Msg("processing raw odds batch")

	// Normalize odds (convert to standard format)
	normalizedOdds := make([]models.NormalizedOdds, 0, len(kafkaMsg.OddsData))
	for _, raw := range kafkaMsg.OddsData {
		normalized := models.NormalizedOdds{
			ID:           raw.ID,
			EventID:      raw.EventID,
			EventName:    raw.EventName,
			Sport:        raw.Sport,
			Competition:  raw.Competition,
			Market:       raw.Market,
			Selection:    raw.Selection,
			BackPrice:    raw.BackPrice,
			LayPrice:     raw.LayPrice,
			BackSize:     raw.BackSize,
			LaySize:      raw.LaySize,
			Timestamp:    raw.Timestamp,
			NormalizedAt: time.Now().UTC(),
		}
		normalizedOdds = append(normalizedOdds, normalized)
	}

	// Publish normalized odds to Kafka
	if err := c.producer.PublishBatch(ctx, normalizedOdds); err != nil {
		return fmt.Errorf("failed to publish normalized odds: %w", err)
	}

	c.logger.Info().
		Int("count", len(normalizedOdds)).
		Str("batch_id", kafkaMsg.BatchID).
		Msg("normalized and published odds")

	return nil
}

// Close closes the Kafka reader
func (c *KafkaConsumer) Close() error {
	return c.reader.Close()
}
