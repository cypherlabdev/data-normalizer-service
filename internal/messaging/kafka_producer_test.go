package messaging

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog"
	"github.com/segmentio/kafka-go"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cypherlabdev/data-normalizer-service/internal/models"
)

// mockKafkaWriter is a mock implementation of kafka.Writer for testing
type mockKafkaWriter struct {
	messages      []kafka.Message
	shouldError   bool
	errorMessage  string
	closed        bool
	writeMessages func(ctx context.Context, msgs ...kafka.Message) error
}

func (m *mockKafkaWriter) WriteMessages(ctx context.Context, msgs ...kafka.Message) error {
	if m.writeMessages != nil {
		return m.writeMessages(ctx, msgs...)
	}
	if m.shouldError {
		return assert.AnError
	}
	m.messages = append(m.messages, msgs...)
	return nil
}

func (m *mockKafkaWriter) Close() error {
	m.closed = true
	return nil
}

// createTestProducer creates a producer with a mock writer for testing
func createTestProducer(mockWriter *mockKafkaWriter) *KafkaProducer {
	logger := zerolog.Nop()
	producer := &KafkaProducer{
		writer: mockWriter,
		logger: logger.With().Str("component", "kafka_producer").Logger(),
	}
	return producer
}

// TestNewKafkaProducer tests the creation of a new Kafka producer
func TestNewKafkaProducer(t *testing.T) {
	brokers := []string{"localhost:9092"}
	topic := "test-topic"
	logger := zerolog.Nop()

	// Execute
	producer := NewKafkaProducer(brokers, topic, logger)

	// Assert
	assert.NotNil(t, producer)
	assert.NotNil(t, producer.writer)
	assert.NotNil(t, producer.logger)
}

// TestPublishBatch_Success tests successful batch publishing
func TestPublishBatch_Success(t *testing.T) {
	mockWriter := &mockKafkaWriter{}
	producer := createTestProducer(mockWriter)
	ctx := context.Background()

	odds := []models.NormalizedOdds{
		{
			ID:           uuid.New(),
			EventID:      "event-1",
			EventName:    "Test Event",
			Sport:        "Football",
			Competition:  "Premier League",
			Market:       "Match Winner",
			Selection:    "Home",
			BackPrice:    decimal.NewFromFloat(2.5),
			LayPrice:     decimal.NewFromFloat(2.6),
			BackSize:     decimal.NewFromFloat(1000),
			LaySize:      decimal.NewFromFloat(1500),
			Timestamp:    time.Now(),
			NormalizedAt: time.Now(),
		},
	}

	// Execute
	err := producer.PublishBatch(ctx, odds)

	// Assert
	assert.NoError(t, err)
	assert.Equal(t, 1, len(mockWriter.messages))

	// Verify message structure
	msg := mockWriter.messages[0]
	assert.NotEmpty(t, msg.Key)
	assert.NotEmpty(t, msg.Value)

	// Verify headers
	assert.Equal(t, 3, len(msg.Headers))
	headerKeys := []string{"batch_id", "timestamp", "count"}
	for i, header := range msg.Headers {
		assert.Equal(t, headerKeys[i], header.Key)
		assert.NotEmpty(t, header.Value)
	}

	// Verify message content
	var kafkaMsg KafkaNormalizedOddsMessage
	err = json.Unmarshal(msg.Value, &kafkaMsg)
	require.NoError(t, err)
	assert.Equal(t, 1, len(kafkaMsg.OddsData))
	assert.NotEmpty(t, kafkaMsg.BatchID)
	assert.NotZero(t, kafkaMsg.Timestamp)
	assert.Equal(t, "event-1", kafkaMsg.OddsData[0].EventID)
}

// TestPublishBatch_MultipleOdds tests publishing multiple odds
func TestPublishBatch_MultipleOdds(t *testing.T) {
	mockWriter := &mockKafkaWriter{}
	producer := createTestProducer(mockWriter)
	ctx := context.Background()

	odds := []models.NormalizedOdds{
		{
			ID:           uuid.New(),
			EventID:      "event-1",
			EventName:    "Test Event 1",
			Sport:        "Football",
			Competition:  "Premier League",
			Market:       "Match Winner",
			Selection:    "Home",
			BackPrice:    decimal.NewFromFloat(2.5),
			LayPrice:     decimal.NewFromFloat(2.6),
			BackSize:     decimal.NewFromFloat(1000),
			LaySize:      decimal.NewFromFloat(1500),
			Timestamp:    time.Now(),
			NormalizedAt: time.Now(),
		},
		{
			ID:           uuid.New(),
			EventID:      "event-2",
			EventName:    "Test Event 2",
			Sport:        "Tennis",
			Competition:  "Wimbledon",
			Market:       "Match Winner",
			Selection:    "Player A",
			BackPrice:    decimal.NewFromFloat(1.8),
			LayPrice:     decimal.NewFromFloat(1.9),
			BackSize:     decimal.NewFromFloat(2000),
			LaySize:      decimal.NewFromFloat(2500),
			Timestamp:    time.Now(),
			NormalizedAt: time.Now(),
		},
	}

	// Execute
	err := producer.PublishBatch(ctx, odds)

	// Assert
	assert.NoError(t, err)
	assert.Equal(t, 1, len(mockWriter.messages))

	// Verify message content
	var kafkaMsg KafkaNormalizedOddsMessage
	err = json.Unmarshal(mockWriter.messages[0].Value, &kafkaMsg)
	require.NoError(t, err)
	assert.Equal(t, 2, len(kafkaMsg.OddsData))
}

// TestPublishBatch_EmptyBatch tests publishing an empty batch
func TestPublishBatch_EmptyBatch(t *testing.T) {
	mockWriter := &mockKafkaWriter{}
	producer := createTestProducer(mockWriter)
	ctx := context.Background()

	odds := []models.NormalizedOdds{}

	// Execute
	err := producer.PublishBatch(ctx, odds)

	// Assert - should not error and should not send any message
	assert.NoError(t, err)
	assert.Equal(t, 0, len(mockWriter.messages))
}

// TestPublishBatch_NilBatch tests publishing a nil batch
func TestPublishBatch_NilBatch(t *testing.T) {
	mockWriter := &mockKafkaWriter{}
	producer := createTestProducer(mockWriter)
	ctx := context.Background()

	// Execute
	err := producer.PublishBatch(ctx, nil)

	// Assert - should not error and should not send any message
	assert.NoError(t, err)
	assert.Equal(t, 0, len(mockWriter.messages))
}

// TestPublishBatch_KafkaError tests handling Kafka write errors
func TestPublishBatch_KafkaError(t *testing.T) {
	mockWriter := &mockKafkaWriter{shouldError: true}
	producer := createTestProducer(mockWriter)
	ctx := context.Background()

	odds := []models.NormalizedOdds{
		{
			ID:           uuid.New(),
			EventID:      "event-1",
			EventName:    "Test Event",
			Sport:        "Football",
			Competition:  "Premier League",
			Market:       "Match Winner",
			Selection:    "Home",
			BackPrice:    decimal.NewFromFloat(2.5),
			LayPrice:     decimal.NewFromFloat(2.6),
			BackSize:     decimal.NewFromFloat(1000),
			LaySize:      decimal.NewFromFloat(1500),
			Timestamp:    time.Now(),
			NormalizedAt: time.Now(),
		},
	}

	// Execute
	err := producer.PublishBatch(ctx, odds)

	// Assert
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to write to Kafka")
}

// TestPublishBatch_ContextCancellation tests handling context cancellation
func TestPublishBatch_ContextCancellation(t *testing.T) {
	mockWriter := &mockKafkaWriter{
		writeMessages: func(ctx context.Context, msgs ...kafka.Message) error {
			return ctx.Err()
		},
	}
	producer := createTestProducer(mockWriter)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	odds := []models.NormalizedOdds{
		{
			ID:           uuid.New(),
			EventID:      "event-1",
			EventName:    "Test Event",
			Sport:        "Football",
			Competition:  "Premier League",
			Market:       "Match Winner",
			Selection:    "Home",
			BackPrice:    decimal.NewFromFloat(2.5),
			LayPrice:     decimal.NewFromFloat(2.6),
			BackSize:     decimal.NewFromFloat(1000),
			LaySize:      decimal.NewFromFloat(1500),
			Timestamp:    time.Now(),
			NormalizedAt: time.Now(),
		},
	}

	// Execute
	err := producer.PublishBatch(ctx, odds)

	// Assert
	assert.Error(t, err)
}

// TestPublishBatch_BatchIDUniqueness tests that each batch gets a unique ID
func TestPublishBatch_BatchIDUniqueness(t *testing.T) {
	mockWriter := &mockKafkaWriter{}
	producer := createTestProducer(mockWriter)
	ctx := context.Background()

	odds := []models.NormalizedOdds{
		{
			ID:           uuid.New(),
			EventID:      "event-1",
			EventName:    "Test Event",
			Sport:        "Football",
			Competition:  "Premier League",
			Market:       "Match Winner",
			Selection:    "Home",
			BackPrice:    decimal.NewFromFloat(2.5),
			LayPrice:     decimal.NewFromFloat(2.6),
			BackSize:     decimal.NewFromFloat(1000),
			LaySize:      decimal.NewFromFloat(1500),
			Timestamp:    time.Now(),
			NormalizedAt: time.Now(),
		},
	}

	// Publish twice
	err := producer.PublishBatch(ctx, odds)
	require.NoError(t, err)
	err = producer.PublishBatch(ctx, odds)
	require.NoError(t, err)

	// Assert - should have different batch IDs
	assert.Equal(t, 2, len(mockWriter.messages))

	var msg1 KafkaNormalizedOddsMessage
	err = json.Unmarshal(mockWriter.messages[0].Value, &msg1)
	require.NoError(t, err)

	var msg2 KafkaNormalizedOddsMessage
	err = json.Unmarshal(mockWriter.messages[1].Value, &msg2)
	require.NoError(t, err)

	assert.NotEqual(t, msg1.BatchID, msg2.BatchID)
}

// TestPublishBatch_MessageKey tests that message key is set to batch ID
func TestPublishBatch_MessageKey(t *testing.T) {
	mockWriter := &mockKafkaWriter{}
	producer := createTestProducer(mockWriter)
	ctx := context.Background()

	odds := []models.NormalizedOdds{
		{
			ID:           uuid.New(),
			EventID:      "event-1",
			EventName:    "Test Event",
			Sport:        "Football",
			Competition:  "Premier League",
			Market:       "Match Winner",
			Selection:    "Home",
			BackPrice:    decimal.NewFromFloat(2.5),
			LayPrice:     decimal.NewFromFloat(2.6),
			BackSize:     decimal.NewFromFloat(1000),
			LaySize:      decimal.NewFromFloat(1500),
			Timestamp:    time.Now(),
			NormalizedAt: time.Now(),
		},
	}

	// Execute
	err := producer.PublishBatch(ctx, odds)
	require.NoError(t, err)

	// Assert
	msg := mockWriter.messages[0]
	var kafkaMsg KafkaNormalizedOddsMessage
	err = json.Unmarshal(msg.Value, &kafkaMsg)
	require.NoError(t, err)

	assert.Equal(t, kafkaMsg.BatchID, string(msg.Key))
}

// TestPublishBatch_HeadersFormat tests the format of message headers
func TestPublishBatch_HeadersFormat(t *testing.T) {
	mockWriter := &mockKafkaWriter{}
	producer := createTestProducer(mockWriter)
	ctx := context.Background()

	odds := []models.NormalizedOdds{
		{
			ID:           uuid.New(),
			EventID:      "event-1",
			EventName:    "Test Event",
			Sport:        "Football",
			Competition:  "Premier League",
			Market:       "Match Winner",
			Selection:    "Home",
			BackPrice:    decimal.NewFromFloat(2.5),
			LayPrice:     decimal.NewFromFloat(2.6),
			BackSize:     decimal.NewFromFloat(1000),
			LaySize:      decimal.NewFromFloat(1500),
			Timestamp:    time.Now(),
			NormalizedAt: time.Now(),
		},
	}

	// Execute
	err := producer.PublishBatch(ctx, odds)
	require.NoError(t, err)

	// Assert headers
	msg := mockWriter.messages[0]
	headers := make(map[string]string)
	for _, h := range msg.Headers {
		headers[h.Key] = string(h.Value)
	}

	assert.Contains(t, headers, "batch_id")
	assert.Contains(t, headers, "timestamp")
	assert.Contains(t, headers, "count")
	assert.Equal(t, "1", headers["count"])

	// Validate timestamp format
	_, err = time.Parse(time.RFC3339, headers["timestamp"])
	assert.NoError(t, err)
}

// TestPublishBatch_LargeBatch tests publishing a large batch
func TestPublishBatch_LargeBatch(t *testing.T) {
	mockWriter := &mockKafkaWriter{}
	producer := createTestProducer(mockWriter)
	ctx := context.Background()

	// Create 100 odds
	odds := make([]models.NormalizedOdds, 100)
	for i := 0; i < 100; i++ {
		odds[i] = models.NormalizedOdds{
			ID:           uuid.New(),
			EventID:      "event-" + string(rune(i)),
			EventName:    "Test Event",
			Sport:        "Football",
			Competition:  "Premier League",
			Market:       "Match Winner",
			Selection:    "Home",
			BackPrice:    decimal.NewFromFloat(2.5),
			LayPrice:     decimal.NewFromFloat(2.6),
			BackSize:     decimal.NewFromFloat(1000),
			LaySize:      decimal.NewFromFloat(1500),
			Timestamp:    time.Now(),
			NormalizedAt: time.Now(),
		}
	}

	// Execute
	err := producer.PublishBatch(ctx, odds)

	// Assert
	assert.NoError(t, err)
	assert.Equal(t, 1, len(mockWriter.messages))

	var kafkaMsg KafkaNormalizedOddsMessage
	err = json.Unmarshal(mockWriter.messages[0].Value, &kafkaMsg)
	require.NoError(t, err)
	assert.Equal(t, 100, len(kafkaMsg.OddsData))
}

// TestProducerClose_Success tests successful closing of producer
func TestProducerClose_Success(t *testing.T) {
	mockWriter := &mockKafkaWriter{}
	producer := createTestProducer(mockWriter)

	// Execute
	err := producer.Close()

	// Assert
	assert.NoError(t, err)
	assert.True(t, mockWriter.closed)
}

// TestPublishBatch_DecimalPrecision tests that decimal values maintain precision
func TestPublishBatch_DecimalPrecision(t *testing.T) {
	mockWriter := &mockKafkaWriter{}
	producer := createTestProducer(mockWriter)
	ctx := context.Background()

	odds := []models.NormalizedOdds{
		{
			ID:           uuid.New(),
			EventID:      "event-1",
			EventName:    "Test Event",
			Sport:        "Football",
			Competition:  "Premier League",
			Market:       "Match Winner",
			Selection:    "Home",
			BackPrice:    decimal.NewFromFloat(2.123456),
			LayPrice:     decimal.NewFromFloat(2.654321),
			BackSize:     decimal.NewFromFloat(1000.999),
			LaySize:      decimal.NewFromFloat(1500.111),
			Timestamp:    time.Now(),
			NormalizedAt: time.Now(),
		},
	}

	// Execute
	err := producer.PublishBatch(ctx, odds)
	require.NoError(t, err)

	// Assert
	var kafkaMsg KafkaNormalizedOddsMessage
	err = json.Unmarshal(mockWriter.messages[0].Value, &kafkaMsg)
	require.NoError(t, err)

	// Verify decimal precision is maintained
	assert.True(t, kafkaMsg.OddsData[0].BackPrice.Equal(decimal.NewFromFloat(2.123456)))
	assert.True(t, kafkaMsg.OddsData[0].LayPrice.Equal(decimal.NewFromFloat(2.654321)))
}

// TestKafkaNormalizedOddsMessage_Serialization tests message serialization
func TestKafkaNormalizedOddsMessage_Serialization(t *testing.T) {
	now := time.Now().UTC()
	batchID := uuid.New().String()

	msg := &KafkaNormalizedOddsMessage{
		OddsData: []models.NormalizedOdds{
			{
				ID:           uuid.New(),
				EventID:      "event-1",
				EventName:    "Test Event",
				Sport:        "Football",
				Competition:  "Premier League",
				Market:       "Match Winner",
				Selection:    "Home",
				BackPrice:    decimal.NewFromFloat(2.5),
				LayPrice:     decimal.NewFromFloat(2.6),
				BackSize:     decimal.NewFromFloat(1000),
				LaySize:      decimal.NewFromFloat(1500),
				Timestamp:    now,
				NormalizedAt: now,
			},
		},
		Timestamp: now,
		BatchID:   batchID,
	}

	// Serialize
	data, err := json.Marshal(msg)
	require.NoError(t, err)

	// Deserialize
	var decoded KafkaNormalizedOddsMessage
	err = json.Unmarshal(data, &decoded)
	require.NoError(t, err)

	// Assert
	assert.Equal(t, batchID, decoded.BatchID)
	assert.Equal(t, 1, len(decoded.OddsData))
	assert.Equal(t, "event-1", decoded.OddsData[0].EventID)
}
