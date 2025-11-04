package messaging

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog"
	"github.com/segmentio/kafka-go"

	"github.com/cypherlabdev/data-normalizer-service/internal/models"
)

// KafkaNormalizedOddsMessage represents the message sent to odds-optimizer
type KafkaNormalizedOddsMessage struct {
	OddsData  []models.NormalizedOdds `json:"odds_data"`
	Timestamp time.Time               `json:"timestamp"`
	BatchID   string                  `json:"batch_id"`
}

// KafkaProducer publishes normalized odds to Kafka
type KafkaProducer struct {
	writer kafkaWriter
	logger zerolog.Logger
}

// kafkaWriter interface for Kafka writer abstraction
type kafkaWriter interface {
	WriteMessages(ctx context.Context, msgs ...kafka.Message) error
	Close() error
}

// NewKafkaProducer creates a new Kafka producer
func NewKafkaProducer(
	brokers []string,
	topic string,
	logger zerolog.Logger,
) *KafkaProducer {
	writer := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: kafka.RequireAll,
		MaxAttempts:  3,
		BatchSize:    100,
		BatchTimeout: 5 * time.Second,
		Compression:  kafka.Snappy,
	}

	return &KafkaProducer{
		writer: writer,
		logger: logger.With().Str("component", "kafka_producer").Logger(),
	}
}

// PublishBatch publishes a batch of normalized odds
func (p *KafkaProducer) PublishBatch(ctx context.Context, odds []models.NormalizedOdds) error {
	if len(odds) == 0 {
		return nil
	}

	message := &KafkaNormalizedOddsMessage{
		OddsData:  odds,
		Timestamp: time.Now().UTC(),
		BatchID:   uuid.New().String(),
	}

	// Serialize to JSON
	payload, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %w", err)
	}

	// Create Kafka message
	kafkaMsg := kafka.Message{
		Key:   []byte(message.BatchID),
		Value: payload,
		Headers: []kafka.Header{
			{Key: "batch_id", Value: []byte(message.BatchID)},
			{Key: "timestamp", Value: []byte(message.Timestamp.Format(time.RFC3339))},
			{Key: "count", Value: []byte(fmt.Sprintf("%d", len(odds)))},
		},
	}

	// Write to Kafka
	if err := p.writer.WriteMessages(ctx, kafkaMsg); err != nil {
		return fmt.Errorf("failed to write to Kafka: %w", err)
	}

	p.logger.Info().
		Int("count", len(odds)).
		Str("batch_id", message.BatchID).
		Msg("published normalized odds batch")

	return nil
}

// Close closes the Kafka writer
func (p *KafkaProducer) Close() error {
	return p.writer.Close()
}
