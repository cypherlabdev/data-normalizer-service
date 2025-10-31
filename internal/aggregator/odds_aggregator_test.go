package aggregator

import (
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cypherlabdev/data-normalizer-service/internal/models"
)

// TestNewOddsAggregator tests the creation of a new odds aggregator
func TestNewOddsAggregator(t *testing.T) {
	// Execute
	aggregator := NewOddsAggregator()

	// Assert
	assert.NotNil(t, aggregator)
	assert.NotNil(t, aggregator.cache)
	assert.Equal(t, 0, len(aggregator.cache))
}

// TestUpdateOdds_SingleProvider tests updating odds from a single provider
func TestUpdateOdds_SingleProvider(t *testing.T) {
	aggregator := NewOddsAggregator()

	oddsData := []*models.OddsData{
		{
			Provider:    models.ProviderBetradar,
			EventID:     "event-1",
			MarketID:    "market-1",
			SelectionID: "selection-1",
			BackOdds:    decimal.NewFromFloat(2.5),
			LayOdds:     decimal.NewFromFloat(2.6),
			Volume:      decimal.NewFromFloat(1000),
			Timestamp:   time.Now(),
		},
	}

	// Execute
	aggregator.UpdateOdds(oddsData)

	// Assert
	normalized := aggregator.GetOdds("event-1", "market-1", "selection-1")
	require.NotNil(t, normalized)
	assert.Equal(t, "event-1", normalized.EventID)
	assert.Equal(t, "market-1", normalized.MarketID)
	assert.Equal(t, "selection-1", normalized.SelectionID)
	assert.True(t, normalized.BestBack.Equal(decimal.NewFromFloat(2.5)))
	assert.True(t, normalized.BestLay.Equal(decimal.NewFromFloat(2.6)))
	assert.True(t, normalized.AvgBack.Equal(decimal.NewFromFloat(2.5)))
	assert.True(t, normalized.AvgLay.Equal(decimal.NewFromFloat(2.6)))
	assert.Equal(t, 1, normalized.Sources)
	assert.True(t, normalized.Confidence.GreaterThan(decimal.Zero))
}

// TestUpdateOdds_MultipleProviders tests aggregating odds from multiple providers
func TestUpdateOdds_MultipleProviders(t *testing.T) {
	aggregator := NewOddsAggregator()

	oddsData := []*models.OddsData{
		{
			Provider:    models.ProviderBetradar,
			EventID:     "event-1",
			MarketID:    "market-1",
			SelectionID: "selection-1",
			BackOdds:    decimal.NewFromFloat(2.5),
			LayOdds:     decimal.NewFromFloat(2.6),
			Volume:      decimal.NewFromFloat(1000),
			Timestamp:   time.Now(),
		},
		{
			Provider:    models.ProviderBet365,
			EventID:     "event-1",
			MarketID:    "market-1",
			SelectionID: "selection-1",
			BackOdds:    decimal.NewFromFloat(2.7), // Better back odds
			LayOdds:     decimal.NewFromFloat(2.8),
			Volume:      decimal.NewFromFloat(1500),
			Timestamp:   time.Now(),
		},
		{
			Provider:    models.ProviderPinnacle,
			EventID:     "event-1",
			MarketID:    "market-1",
			SelectionID: "selection-1",
			BackOdds:    decimal.NewFromFloat(2.4),
			LayOdds:     decimal.NewFromFloat(2.5), // Better lay odds
			Volume:      decimal.NewFromFloat(2000),
			Timestamp:   time.Now(),
		},
	}

	// Execute
	aggregator.UpdateOdds(oddsData)

	// Assert
	normalized := aggregator.GetOdds("event-1", "market-1", "selection-1")
	require.NotNil(t, normalized)

	// Best back should be 2.7 (highest)
	assert.True(t, normalized.BestBack.Equal(decimal.NewFromFloat(2.7)))

	// Best lay should be 2.5 (lowest)
	assert.True(t, normalized.BestLay.Equal(decimal.NewFromFloat(2.5)))

	// Average back should be (2.5 + 2.7 + 2.4) / 3 = 2.533...
	expectedAvgBack := decimal.NewFromFloat(2.5).
		Add(decimal.NewFromFloat(2.7)).
		Add(decimal.NewFromFloat(2.4)).
		Div(decimal.NewFromInt(3))
	assert.True(t, normalized.AvgBack.Equal(expectedAvgBack))

	// Average lay should be (2.6 + 2.8 + 2.5) / 3 = 2.633...
	expectedAvgLay := decimal.NewFromFloat(2.6).
		Add(decimal.NewFromFloat(2.8)).
		Add(decimal.NewFromFloat(2.5)).
		Div(decimal.NewFromInt(3))
	assert.True(t, normalized.AvgLay.Equal(expectedAvgLay))

	// Should have 3 sources
	assert.Equal(t, 3, normalized.Sources)

	// Confidence should be 3/5 = 0.6
	expectedConfidence := decimal.NewFromInt(3).Div(decimal.NewFromInt(5))
	assert.True(t, normalized.Confidence.Equal(expectedConfidence))
}

// TestUpdateOdds_ConfidenceCapAt1 tests that confidence is capped at 1.0
func TestUpdateOdds_ConfidenceCapAt1(t *testing.T) {
	aggregator := NewOddsAggregator()

	// Create 6 providers (more than the 5 threshold)
	oddsData := make([]*models.OddsData, 6)
	for i := 0; i < 6; i++ {
		oddsData[i] = &models.OddsData{
			Provider:    models.Provider("provider-" + string(rune(i))),
			EventID:     "event-1",
			MarketID:    "market-1",
			SelectionID: "selection-1",
			BackOdds:    decimal.NewFromFloat(2.5),
			LayOdds:     decimal.NewFromFloat(2.6),
			Volume:      decimal.NewFromFloat(1000),
			Timestamp:   time.Now(),
		}
	}

	// Execute
	aggregator.UpdateOdds(oddsData)

	// Assert
	normalized := aggregator.GetOdds("event-1", "market-1", "selection-1")
	require.NotNil(t, normalized)

	// Confidence should be capped at 1.0 even with 6 providers
	assert.True(t, normalized.Confidence.Equal(decimal.NewFromInt(1)))
	assert.Equal(t, 6, normalized.Sources)
}

// TestUpdateOdds_MultipleEvents tests updating odds for multiple events
func TestUpdateOdds_MultipleEvents(t *testing.T) {
	aggregator := NewOddsAggregator()

	oddsData := []*models.OddsData{
		{
			Provider:    models.ProviderBetradar,
			EventID:     "event-1",
			MarketID:    "market-1",
			SelectionID: "selection-1",
			BackOdds:    decimal.NewFromFloat(2.5),
			LayOdds:     decimal.NewFromFloat(2.6),
			Volume:      decimal.NewFromFloat(1000),
			Timestamp:   time.Now(),
		},
		{
			Provider:    models.ProviderBetradar,
			EventID:     "event-2",
			MarketID:    "market-1",
			SelectionID: "selection-1",
			BackOdds:    decimal.NewFromFloat(3.5),
			LayOdds:     decimal.NewFromFloat(3.6),
			Volume:      decimal.NewFromFloat(1500),
			Timestamp:   time.Now(),
		},
	}

	// Execute
	aggregator.UpdateOdds(oddsData)

	// Assert - both events should be in cache
	normalized1 := aggregator.GetOdds("event-1", "market-1", "selection-1")
	require.NotNil(t, normalized1)
	assert.True(t, normalized1.BestBack.Equal(decimal.NewFromFloat(2.5)))

	normalized2 := aggregator.GetOdds("event-2", "market-1", "selection-1")
	require.NotNil(t, normalized2)
	assert.True(t, normalized2.BestBack.Equal(decimal.NewFromFloat(3.5)))
}

// TestUpdateOdds_EmptyData tests updating with empty odds data
func TestUpdateOdds_EmptyData(t *testing.T) {
	aggregator := NewOddsAggregator()

	// Execute
	aggregator.UpdateOdds([]*models.OddsData{})

	// Assert - cache should remain empty
	assert.Equal(t, 0, len(aggregator.cache))
}

// TestUpdateOdds_UpdateExisting tests updating existing odds
func TestUpdateOdds_UpdateExisting(t *testing.T) {
	aggregator := NewOddsAggregator()

	// First update
	oddsData1 := []*models.OddsData{
		{
			Provider:    models.ProviderBetradar,
			EventID:     "event-1",
			MarketID:    "market-1",
			SelectionID: "selection-1",
			BackOdds:    decimal.NewFromFloat(2.5),
			LayOdds:     decimal.NewFromFloat(2.6),
			Volume:      decimal.NewFromFloat(1000),
			Timestamp:   time.Now(),
		},
	}
	aggregator.UpdateOdds(oddsData1)

	// Second update with different odds
	oddsData2 := []*models.OddsData{
		{
			Provider:    models.ProviderBet365,
			EventID:     "event-1",
			MarketID:    "market-1",
			SelectionID: "selection-1",
			BackOdds:    decimal.NewFromFloat(3.0),
			LayOdds:     decimal.NewFromFloat(3.1),
			Volume:      decimal.NewFromFloat(1200),
			Timestamp:   time.Now(),
		},
	}

	// Execute
	aggregator.UpdateOdds(oddsData2)

	// Assert - should only have the latest update
	normalized := aggregator.GetOdds("event-1", "market-1", "selection-1")
	require.NotNil(t, normalized)
	assert.True(t, normalized.BestBack.Equal(decimal.NewFromFloat(3.0)))
	assert.Equal(t, 1, normalized.Sources) // Only one provider in the latest update
}

// TestGetOdds_NotFound tests retrieving odds that don't exist
func TestGetOdds_NotFound(t *testing.T) {
	aggregator := NewOddsAggregator()

	// Execute
	normalized := aggregator.GetOdds("nonexistent-event", "market-1", "selection-1")

	// Assert
	assert.Nil(t, normalized)
}

// TestGetOdds_AfterUpdate tests retrieving odds after update
func TestGetOdds_AfterUpdate(t *testing.T) {
	aggregator := NewOddsAggregator()

	oddsData := []*models.OddsData{
		{
			Provider:    models.ProviderBetradar,
			EventID:     "event-1",
			MarketID:    "market-1",
			SelectionID: "selection-1",
			BackOdds:    decimal.NewFromFloat(2.5),
			LayOdds:     decimal.NewFromFloat(2.6),
			Volume:      decimal.NewFromFloat(1000),
			Timestamp:   time.Now(),
		},
	}

	aggregator.UpdateOdds(oddsData)

	// Execute
	normalized := aggregator.GetOdds("event-1", "market-1", "selection-1")

	// Assert
	require.NotNil(t, normalized)
	assert.Equal(t, "event-1", normalized.EventID)
	assert.NotZero(t, normalized.UpdatedAt)
}

// TestMakeKey tests the key generation function
func TestMakeKey(t *testing.T) {
	tests := []struct {
		name        string
		eventID     string
		marketID    string
		selectionID string
		expected    string
	}{
		{
			name:        "normal case",
			eventID:     "event-1",
			marketID:    "market-1",
			selectionID: "selection-1",
			expected:    "event-1-market-1-selection-1",
		},
		{
			name:        "with dashes in IDs",
			eventID:     "event-1-2",
			marketID:    "market-1-2",
			selectionID: "selection-1-2",
			expected:    "event-1-2-market-1-2-selection-1-2",
		},
		{
			name:        "empty IDs",
			eventID:     "",
			marketID:    "",
			selectionID: "",
			expected:    "--",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Execute
			result := makeKey(tt.eventID, tt.marketID, tt.selectionID)

			// Assert
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestAggregate_NilData tests aggregation with nil data
func TestAggregate_NilData(t *testing.T) {
	aggregator := NewOddsAggregator()

	// Execute
	result := aggregator.aggregate(nil)

	// Assert
	assert.Nil(t, result)
}

// TestAggregate_EmptyData tests aggregation with empty data
func TestAggregate_EmptyData(t *testing.T) {
	aggregator := NewOddsAggregator()

	// Execute
	result := aggregator.aggregate([]*models.OddsData{})

	// Assert
	assert.Nil(t, result)
}

// TestUpdateOdds_ConcurrentAccess tests concurrent access to the aggregator
func TestUpdateOdds_ConcurrentAccess(t *testing.T) {
	aggregator := NewOddsAggregator()

	// Create test data
	oddsData := []*models.OddsData{
		{
			Provider:    models.ProviderBetradar,
			EventID:     "event-1",
			MarketID:    "market-1",
			SelectionID: "selection-1",
			BackOdds:    decimal.NewFromFloat(2.5),
			LayOdds:     decimal.NewFromFloat(2.6),
			Volume:      decimal.NewFromFloat(1000),
			Timestamp:   time.Now(),
		},
	}

	// Run concurrent updates and reads
	done := make(chan bool)

	// Writer goroutines
	for i := 0; i < 10; i++ {
		go func() {
			aggregator.UpdateOdds(oddsData)
			done <- true
		}()
	}

	// Reader goroutines
	for i := 0; i < 10; i++ {
		go func() {
			_ = aggregator.GetOdds("event-1", "market-1", "selection-1")
			done <- true
		}()
	}

	// Wait for all goroutines
	for i := 0; i < 20; i++ {
		<-done
	}

	// Assert - should not panic and should have data
	normalized := aggregator.GetOdds("event-1", "market-1", "selection-1")
	assert.NotNil(t, normalized)
}

// TestUpdateOdds_MultipleMarkets tests updating odds for multiple markets
func TestUpdateOdds_MultipleMarkets(t *testing.T) {
	aggregator := NewOddsAggregator()

	oddsData := []*models.OddsData{
		{
			Provider:    models.ProviderBetradar,
			EventID:     "event-1",
			MarketID:    "market-1",
			SelectionID: "selection-1",
			BackOdds:    decimal.NewFromFloat(2.5),
			LayOdds:     decimal.NewFromFloat(2.6),
			Volume:      decimal.NewFromFloat(1000),
			Timestamp:   time.Now(),
		},
		{
			Provider:    models.ProviderBetradar,
			EventID:     "event-1",
			MarketID:    "market-2",
			SelectionID: "selection-1",
			BackOdds:    decimal.NewFromFloat(1.5),
			LayOdds:     decimal.NewFromFloat(1.6),
			Volume:      decimal.NewFromFloat(2000),
			Timestamp:   time.Now(),
		},
	}

	// Execute
	aggregator.UpdateOdds(oddsData)

	// Assert - both markets should be cached
	normalized1 := aggregator.GetOdds("event-1", "market-1", "selection-1")
	require.NotNil(t, normalized1)
	assert.Equal(t, "market-1", normalized1.MarketID)
	assert.True(t, normalized1.BestBack.Equal(decimal.NewFromFloat(2.5)))

	normalized2 := aggregator.GetOdds("event-1", "market-2", "selection-1")
	require.NotNil(t, normalized2)
	assert.Equal(t, "market-2", normalized2.MarketID)
	assert.True(t, normalized2.BestBack.Equal(decimal.NewFromFloat(1.5)))
}

// TestUpdateOdds_ZeroOdds tests updating with zero odds values
func TestUpdateOdds_ZeroOdds(t *testing.T) {
	aggregator := NewOddsAggregator()

	oddsData := []*models.OddsData{
		{
			Provider:    models.ProviderBetradar,
			EventID:     "event-1",
			MarketID:    "market-1",
			SelectionID: "selection-1",
			BackOdds:    decimal.Zero,
			LayOdds:     decimal.Zero,
			Volume:      decimal.Zero,
			Timestamp:   time.Now(),
		},
	}

	// Execute
	aggregator.UpdateOdds(oddsData)

	// Assert
	normalized := aggregator.GetOdds("event-1", "market-1", "selection-1")
	require.NotNil(t, normalized)
	assert.True(t, normalized.BestBack.Equal(decimal.Zero))
	// BestLay will be zero since it's the only value provided (and LessThan picks the smallest)
	assert.True(t, normalized.BestLay.Equal(decimal.Zero))
}

// TestAggregate_VeryHighLayOdds tests aggregation picks the lowest lay odds
func TestAggregate_VeryHighLayOdds(t *testing.T) {
	aggregator := NewOddsAggregator()

	oddsData := []*models.OddsData{
		{
			Provider:    models.ProviderBetradar,
			EventID:     "event-1",
			MarketID:    "market-1",
			SelectionID: "selection-1",
			BackOdds:    decimal.NewFromFloat(2.5),
			LayOdds:     decimal.NewFromFloat(100.0),
			Volume:      decimal.NewFromFloat(1000),
			Timestamp:   time.Now(),
		},
		{
			Provider:    models.ProviderBet365,
			EventID:     "event-1",
			MarketID:    "market-1",
			SelectionID: "selection-1",
			BackOdds:    decimal.NewFromFloat(2.4),
			LayOdds:     decimal.NewFromFloat(2.7),
			Volume:      decimal.NewFromFloat(1500),
			Timestamp:   time.Now(),
		},
	}

	// Execute
	aggregator.UpdateOdds(oddsData)

	// Assert - should pick the lowest lay odds
	normalized := aggregator.GetOdds("event-1", "market-1", "selection-1")
	require.NotNil(t, normalized)
	assert.True(t, normalized.BestLay.Equal(decimal.NewFromFloat(2.7)))
}
