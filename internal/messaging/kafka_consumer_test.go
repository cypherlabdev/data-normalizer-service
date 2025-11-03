package messaging

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog"
	"github.com/segmentio/kafka-go"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/cypherlabdev/data-normalizer-service/internal/mocks"
	"github.com/cypherlabdev/data-normalizer-service/internal/models"
)

// mockKafkaReader is a mock implementation of kafka.Reader for testing
type mockKafkaReader struct {
	messages      []kafka.Message
	currentIndex  int
	shouldError   bool
	closed        bool
	fetchMessage  func(ctx context.Context) (kafka.Message, error)
	commitMessage func(ctx context.Context, msgs ...kafka.Message) error
	config        kafka.ReaderConfig
}

func (m *mockKafkaReader) FetchMessage(ctx context.Context) (kafka.Message, error) {
	if m.fetchMessage != nil {
		return m.fetchMessage(ctx)
	}
	if m.shouldError {
		return kafka.Message{}, assert.AnError
	}
	if m.currentIndex >= len(m.messages) {
		return kafka.Message{}, context.Canceled
	}
	msg := m.messages[m.currentIndex]
	m.currentIndex++
	return msg, nil
}

func (m *mockKafkaReader) CommitMessages(ctx context.Context, msgs ...kafka.Message) error {
	if m.commitMessage != nil {
		return m.commitMessage(ctx, msgs...)
	}
	return nil
}

func (m *mockKafkaReader) Close() error {
	m.closed = true
	return nil
}

func (m *mockKafkaReader) Config() kafka.ReaderConfig {
	return m.config
}

// createTestConsumer creates a consumer with mocks for testing
func createTestConsumer(t *testing.T, mockReader *mockKafkaReader) (*KafkaConsumer, *mocks.MockProducer, *gomock.Controller) {
	ctrl := gomock.NewController(t)
	mockProducer := mocks.NewMockProducer(ctrl)
	logger := zerolog.Nop()

	consumer := &KafkaConsumer{
		reader:   mockReader,
		producer: mockProducer,
		logger:   logger.With().Str("component", "kafka_consumer").Logger(),
	}

	return consumer, mockProducer, ctrl
}

// createTestRawOddsMessage creates a test Kafka message
func createTestRawOddsMessage() kafka.Message {
	rawOdds := KafkaRawOddsMessage{
		OddsData: []RawOddsData{
			{
				ID:          uuid.New(),
				Provider:    "betradar",
				EventID:     "event-1",
				EventName:   "Test Event",
				Sport:       "Football",
				Competition: "Premier League",
				Market:      "Match Winner",
				Selection:   "Home",
				BackPrice:   decimal.NewFromFloat(2.5),
				LayPrice:    decimal.NewFromFloat(2.6),
				BackSize:    decimal.NewFromFloat(1000),
				LaySize:     decimal.NewFromFloat(1500),
				Timestamp:   time.Now(),
			},
		},
		Provider:  "betradar",
		Timestamp: time.Now(),
		BatchID:   uuid.New().String(),
	}

	data, _ := json.Marshal(rawOdds)
	return kafka.Message{
		Topic:     "raw_odds",
		Partition: 0,
		Offset:    0,
		Key:       []byte(rawOdds.BatchID),
		Value:     data,
		Time:      time.Now(),
	}
}

// TestNewKafkaConsumer tests the creation of a new Kafka consumer
func TestNewKafkaConsumer(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockProducer := mocks.NewMockProducer(ctrl)
	brokers := []string{"localhost:9092"}
	topic := "test-topic"
	groupID := "test-group"
	logger := zerolog.Nop()

	// Execute
	consumer := NewKafkaConsumer(brokers, topic, groupID, mockProducer, logger)

	// Assert
	assert.NotNil(t, consumer)
	assert.NotNil(t, consumer.reader)
	assert.NotNil(t, consumer.producer)
	assert.NotNil(t, consumer.logger)
}

// TestStart_Success tests successful message consumption
func TestStart_Success(t *testing.T) {
	msg := createTestRawOddsMessage()
	mockReader := &mockKafkaReader{
		messages: []kafka.Message{msg},
		config: kafka.ReaderConfig{
			Topic:   "raw_odds",
			GroupID: "test-group",
		},
	}

	consumer, mockProducer, ctrl := createTestConsumer(t, mockReader)
	defer ctrl.Finish()

	ctx, cancel := context.WithCancel(context.Background())

	// Setup expectations
	mockProducer.EXPECT().
		PublishBatch(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, odds []models.NormalizedOdds) error {
			assert.Equal(t, 1, len(odds))
			assert.Equal(t, "event-1", odds[0].EventID)
			cancel() // Stop the consumer after processing
			return nil
		})

	// Execute
	err := consumer.Start(ctx)

	// Assert
	assert.NoError(t, err)
	assert.True(t, mockReader.closed)
}

// TestStart_ContextCancellation tests consumer stops on context cancellation
func TestStart_ContextCancellation(t *testing.T) {
	mockReader := &mockKafkaReader{
		messages: []kafka.Message{},
		config: kafka.ReaderConfig{
			Topic:   "raw_odds",
			GroupID: "test-group",
		},
		fetchMessage: func(ctx context.Context) (kafka.Message, error) {
			<-ctx.Done()
			return kafka.Message{}, context.Canceled
		},
	}

	consumer, _, ctrl := createTestConsumer(t, mockReader)
	defer ctrl.Finish()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	// Execute
	err := consumer.Start(ctx)

	// Assert
	assert.NoError(t, err)
	assert.True(t, mockReader.closed)
}

// TestProcessMessage_Success tests successful message processing
func TestProcessMessage_Success(t *testing.T) {
	msg := createTestRawOddsMessage()
	mockReader := &mockKafkaReader{
		config: kafka.ReaderConfig{
			Topic:   "raw_odds",
			GroupID: "test-group",
		},
	}

	consumer, mockProducer, ctrl := createTestConsumer(t, mockReader)
	defer ctrl.Finish()

	ctx := context.Background()

	// Setup expectations
	mockProducer.EXPECT().
		PublishBatch(ctx, gomock.Any()).
		DoAndReturn(func(ctx context.Context, odds []models.NormalizedOdds) error {
			assert.Equal(t, 1, len(odds))
			assert.Equal(t, "event-1", odds[0].EventID)
			assert.Equal(t, "Test Event", odds[0].EventName)
			assert.Equal(t, "Football", odds[0].Sport)
			assert.Equal(t, "Premier League", odds[0].Competition)
			assert.Equal(t, "Match Winner", odds[0].Market)
			assert.Equal(t, "Home", odds[0].Selection)
			assert.True(t, odds[0].BackPrice.Equal(decimal.NewFromFloat(2.5)))
			assert.True(t, odds[0].LayPrice.Equal(decimal.NewFromFloat(2.6)))
			assert.NotZero(t, odds[0].NormalizedAt)
			return nil
		})

	// Execute
	err := consumer.processMessage(ctx, msg)

	// Assert
	assert.NoError(t, err)
}

// TestProcessMessage_MultipleOdds tests processing message with multiple odds
func TestProcessMessage_MultipleOdds(t *testing.T) {
	rawOdds := KafkaRawOddsMessage{
		OddsData: []RawOddsData{
			{
				ID:          uuid.New(),
				Provider:    "betradar",
				EventID:     "event-1",
				EventName:   "Test Event 1",
				Sport:       "Football",
				Competition: "Premier League",
				Market:      "Match Winner",
				Selection:   "Home",
				BackPrice:   decimal.NewFromFloat(2.5),
				LayPrice:    decimal.NewFromFloat(2.6),
				BackSize:    decimal.NewFromFloat(1000),
				LaySize:     decimal.NewFromFloat(1500),
				Timestamp:   time.Now(),
			},
			{
				ID:          uuid.New(),
				Provider:    "betradar",
				EventID:     "event-2",
				EventName:   "Test Event 2",
				Sport:       "Tennis",
				Competition: "Wimbledon",
				Market:      "Match Winner",
				Selection:   "Player A",
				BackPrice:   decimal.NewFromFloat(1.8),
				LayPrice:    decimal.NewFromFloat(1.9),
				BackSize:    decimal.NewFromFloat(2000),
				LaySize:     decimal.NewFromFloat(2500),
				Timestamp:   time.Now(),
			},
		},
		Provider:  "betradar",
		Timestamp: time.Now(),
		BatchID:   uuid.New().String(),
	}

	data, _ := json.Marshal(rawOdds)
	msg := kafka.Message{
		Value: data,
	}

	mockReader := &mockKafkaReader{
		config: kafka.ReaderConfig{
			Topic:   "raw_odds",
			GroupID: "test-group",
		},
	}

	consumer, mockProducer, ctrl := createTestConsumer(t, mockReader)
	defer ctrl.Finish()

	ctx := context.Background()

	// Setup expectations
	mockProducer.EXPECT().
		PublishBatch(ctx, gomock.Any()).
		DoAndReturn(func(ctx context.Context, odds []models.NormalizedOdds) error {
			assert.Equal(t, 2, len(odds))
			assert.Equal(t, "event-1", odds[0].EventID)
			assert.Equal(t, "event-2", odds[1].EventID)
			return nil
		})

	// Execute
	err := consumer.processMessage(ctx, msg)

	// Assert
	assert.NoError(t, err)
}

// TestProcessMessage_InvalidJSON tests handling invalid JSON
func TestProcessMessage_InvalidJSON(t *testing.T) {
	msg := kafka.Message{
		Value: []byte("invalid json"),
	}

	mockReader := &mockKafkaReader{
		config: kafka.ReaderConfig{
			Topic:   "raw_odds",
			GroupID: "test-group",
		},
	}

	consumer, _, ctrl := createTestConsumer(t, mockReader)
	defer ctrl.Finish()

	ctx := context.Background()

	// Execute
	err := consumer.processMessage(ctx, msg)

	// Assert
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to unmarshal message")
}

// TestProcessMessage_ProducerError tests handling producer publish errors
func TestProcessMessage_ProducerError(t *testing.T) {
	msg := createTestRawOddsMessage()
	mockReader := &mockKafkaReader{
		config: kafka.ReaderConfig{
			Topic:   "raw_odds",
			GroupID: "test-group",
		},
	}

	consumer, mockProducer, ctrl := createTestConsumer(t, mockReader)
	defer ctrl.Finish()

	ctx := context.Background()

	// Setup expectations - producer fails
	mockProducer.EXPECT().
		PublishBatch(ctx, gomock.Any()).
		Return(errors.New("kafka publish failed"))

	// Execute
	err := consumer.processMessage(ctx, msg)

	// Assert
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to publish normalized odds")
}

// TestProcessMessage_EmptyOddsData tests processing message with no odds
func TestProcessMessage_EmptyOddsData(t *testing.T) {
	rawOdds := KafkaRawOddsMessage{
		OddsData:  []RawOddsData{},
		Provider:  "betradar",
		Timestamp: time.Now(),
		BatchID:   uuid.New().String(),
	}

	data, _ := json.Marshal(rawOdds)
	msg := kafka.Message{
		Value: data,
	}

	mockReader := &mockKafkaReader{
		config: kafka.ReaderConfig{
			Topic:   "raw_odds",
			GroupID: "test-group",
		},
	}

	consumer, mockProducer, ctrl := createTestConsumer(t, mockReader)
	defer ctrl.Finish()

	ctx := context.Background()

	// Setup expectations - should still call PublishBatch with empty slice
	mockProducer.EXPECT().
		PublishBatch(ctx, gomock.Any()).
		DoAndReturn(func(ctx context.Context, odds []models.NormalizedOdds) error {
			assert.Equal(t, 0, len(odds))
			return nil
		})

	// Execute
	err := consumer.processMessage(ctx, msg)

	// Assert
	assert.NoError(t, err)
}

// TestStart_FetchError tests handling fetch errors
func TestStart_FetchError(t *testing.T) {
	mockReader := &mockKafkaReader{
		config: kafka.ReaderConfig{
			Topic:   "raw_odds",
			GroupID: "test-group",
		},
		fetchMessage: func(ctx context.Context) (kafka.Message, error) {
			return kafka.Message{}, errors.New("fetch error")
		},
	}

	consumer, _, ctrl := createTestConsumer(t, mockReader)
	defer ctrl.Finish()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	// Execute
	err := consumer.Start(ctx)

	// Assert - should not return error, just log and continue
	assert.NoError(t, err)
}

// TestStart_CommitError tests handling commit errors
func TestStart_CommitError(t *testing.T) {
	msg := createTestRawOddsMessage()
	commitCalled := false

	mockReader := &mockKafkaReader{
		messages: []kafka.Message{msg},
		config: kafka.ReaderConfig{
			Topic:   "raw_odds",
			GroupID: "test-group",
		},
		commitMessage: func(ctx context.Context, msgs ...kafka.Message) error {
			commitCalled = true
			return errors.New("commit error")
		},
	}

	consumer, mockProducer, ctrl := createTestConsumer(t, mockReader)
	defer ctrl.Finish()

	ctx, cancel := context.WithCancel(context.Background())

	// Setup expectations
	mockProducer.EXPECT().
		PublishBatch(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, odds []models.NormalizedOdds) error {
			cancel() // Stop after processing
			return nil
		})

	// Execute
	err := consumer.Start(ctx)

	// Assert - should continue despite commit error
	assert.NoError(t, err)
	assert.True(t, commitCalled)
}

// TestStart_ProcessMessageError tests handling process message errors
func TestStart_ProcessMessageError(t *testing.T) {
	msg := createTestRawOddsMessage()
	mockReader := &mockKafkaReader{
		messages: []kafka.Message{msg},
		config: kafka.ReaderConfig{
			Topic:   "raw_odds",
			GroupID: "test-group",
		},
	}

	consumer, mockProducer, ctrl := createTestConsumer(t, mockReader)
	defer ctrl.Finish()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	// Setup expectations - producer fails
	mockProducer.EXPECT().
		PublishBatch(gomock.Any(), gomock.Any()).
		Return(errors.New("processing error")).
		AnyTimes()

	// Execute
	err := consumer.Start(ctx)

	// Assert - should not return error, just log and continue
	assert.NoError(t, err)
}

// TestConsumerClose_Success tests successful closing of consumer
func TestConsumerClose_Success(t *testing.T) {
	mockReader := &mockKafkaReader{
		config: kafka.ReaderConfig{
			Topic:   "raw_odds",
			GroupID: "test-group",
		},
	}

	consumer, _, ctrl := createTestConsumer(t, mockReader)
	defer ctrl.Finish()

	// Execute
	err := consumer.Close()

	// Assert
	assert.NoError(t, err)
	assert.True(t, mockReader.closed)
}

// TestProcessMessage_NormalizedAtTimestamp tests that NormalizedAt is set
func TestProcessMessage_NormalizedAtTimestamp(t *testing.T) {
	beforeProcess := time.Now().UTC()

	msg := createTestRawOddsMessage()
	mockReader := &mockKafkaReader{
		config: kafka.ReaderConfig{
			Topic:   "raw_odds",
			GroupID: "test-group",
		},
	}

	consumer, mockProducer, ctrl := createTestConsumer(t, mockReader)
	defer ctrl.Finish()

	ctx := context.Background()

	// Setup expectations
	mockProducer.EXPECT().
		PublishBatch(ctx, gomock.Any()).
		DoAndReturn(func(ctx context.Context, odds []models.NormalizedOdds) error {
			assert.Equal(t, 1, len(odds))
			// NormalizedAt should be set to current time
			assert.True(t, odds[0].NormalizedAt.After(beforeProcess) || odds[0].NormalizedAt.Equal(beforeProcess))
			assert.True(t, odds[0].NormalizedAt.Before(time.Now().UTC().Add(1*time.Second)))
			return nil
		})

	// Execute
	err := consumer.processMessage(ctx, msg)

	// Assert
	assert.NoError(t, err)
}

// TestProcessMessage_PreservesOriginalFields tests that original fields are preserved
func TestProcessMessage_PreservesOriginalFields(t *testing.T) {
	originalID := uuid.New()
	originalTimestamp := time.Now().Add(-1 * time.Hour)

	rawOdds := KafkaRawOddsMessage{
		OddsData: []RawOddsData{
			{
				ID:          originalID,
				Provider:    "betradar",
				EventID:     "event-1",
				EventName:   "Test Event",
				Sport:       "Football",
				Competition: "Premier League",
				Market:      "Match Winner",
				Selection:   "Home",
				BackPrice:   decimal.NewFromFloat(2.5),
				LayPrice:    decimal.NewFromFloat(2.6),
				BackSize:    decimal.NewFromFloat(1000),
				LaySize:     decimal.NewFromFloat(1500),
				Timestamp:   originalTimestamp,
			},
		},
		Provider:  "betradar",
		Timestamp: time.Now(),
		BatchID:   uuid.New().String(),
	}

	data, _ := json.Marshal(rawOdds)
	msg := kafka.Message{
		Value: data,
	}

	mockReader := &mockKafkaReader{
		config: kafka.ReaderConfig{
			Topic:   "raw_odds",
			GroupID: "test-group",
		},
	}

	consumer, mockProducer, ctrl := createTestConsumer(t, mockReader)
	defer ctrl.Finish()

	ctx := context.Background()

	// Setup expectations
	mockProducer.EXPECT().
		PublishBatch(ctx, gomock.Any()).
		DoAndReturn(func(ctx context.Context, odds []models.NormalizedOdds) error {
			assert.Equal(t, 1, len(odds))
			assert.Equal(t, originalID, odds[0].ID)
			// Use Equal for time comparison to avoid monotonic clock issues
			assert.True(t, odds[0].Timestamp.Equal(originalTimestamp))
			return nil
		})

	// Execute
	err := consumer.processMessage(ctx, msg)

	// Assert
	assert.NoError(t, err)
}

// TestProcessMessage_DecimalPrecision tests that decimal values maintain precision
func TestProcessMessage_DecimalPrecision(t *testing.T) {
	rawOdds := KafkaRawOddsMessage{
		OddsData: []RawOddsData{
			{
				ID:          uuid.New(),
				Provider:    "betradar",
				EventID:     "event-1",
				EventName:   "Test Event",
				Sport:       "Football",
				Competition: "Premier League",
				Market:      "Match Winner",
				Selection:   "Home",
				BackPrice:   decimal.NewFromFloat(2.123456),
				LayPrice:    decimal.NewFromFloat(2.654321),
				BackSize:    decimal.NewFromFloat(1000.999),
				LaySize:     decimal.NewFromFloat(1500.111),
				Timestamp:   time.Now(),
			},
		},
		Provider:  "betradar",
		Timestamp: time.Now(),
		BatchID:   uuid.New().String(),
	}

	data, _ := json.Marshal(rawOdds)
	msg := kafka.Message{
		Value: data,
	}

	mockReader := &mockKafkaReader{
		config: kafka.ReaderConfig{
			Topic:   "raw_odds",
			GroupID: "test-group",
		},
	}

	consumer, mockProducer, ctrl := createTestConsumer(t, mockReader)
	defer ctrl.Finish()

	ctx := context.Background()

	// Setup expectations
	mockProducer.EXPECT().
		PublishBatch(ctx, gomock.Any()).
		DoAndReturn(func(ctx context.Context, odds []models.NormalizedOdds) error {
			assert.Equal(t, 1, len(odds))
			assert.True(t, odds[0].BackPrice.Equal(decimal.NewFromFloat(2.123456)))
			assert.True(t, odds[0].LayPrice.Equal(decimal.NewFromFloat(2.654321)))
			assert.True(t, odds[0].BackSize.Equal(decimal.NewFromFloat(1000.999)))
			assert.True(t, odds[0].LaySize.Equal(decimal.NewFromFloat(1500.111)))
			return nil
		})

	// Execute
	err := consumer.processMessage(ctx, msg)

	// Assert
	assert.NoError(t, err)
}

// TestRawOddsData_Serialization tests RawOddsData serialization
func TestRawOddsData_Serialization(t *testing.T) {
	now := time.Now().UTC()
	id := uuid.New()

	data := RawOddsData{
		ID:          id,
		Provider:    "betradar",
		EventID:     "event-1",
		EventName:   "Test Event",
		Sport:       "Football",
		Competition: "Premier League",
		Market:      "Match Winner",
		Selection:   "Home",
		BackPrice:   decimal.NewFromFloat(2.5),
		LayPrice:    decimal.NewFromFloat(2.6),
		BackSize:    decimal.NewFromFloat(1000),
		LaySize:     decimal.NewFromFloat(1500),
		Timestamp:   now,
	}

	// Serialize
	jsonData, err := json.Marshal(data)
	require.NoError(t, err)

	// Deserialize
	var decoded RawOddsData
	err = json.Unmarshal(jsonData, &decoded)
	require.NoError(t, err)

	// Assert
	assert.Equal(t, id, decoded.ID)
	assert.Equal(t, "betradar", decoded.Provider)
	assert.Equal(t, "event-1", decoded.EventID)
	assert.True(t, decoded.BackPrice.Equal(decimal.NewFromFloat(2.5)))
}
