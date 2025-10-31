package aggregator

import (
	"sync"
	"time"

	"github.com/shopspring/decimal"

	"github.com/cypherlabdev/data-normalizer-service/internal/models"
)

// OddsAggregator aggregates odds from multiple providers
type OddsAggregator struct {
	cache map[string]*models.NormalizedOdds // key: eventID-marketID-selectionID
	mu    sync.RWMutex
}

// NewOddsAggregator creates a new odds aggregator
func NewOddsAggregator() *OddsAggregator {
	return &OddsAggregator{
		cache: make(map[string]*models.NormalizedOdds),
	}
}

// UpdateOdds updates odds from a provider
func (a *OddsAggregator) UpdateOdds(data []*models.OddsData) {
	a.mu.Lock()
	defer a.mu.Unlock()

	// Group by event-market-selection
	grouped := make(map[string][]*models.OddsData)
	for _, od := range data {
		key := makeKey(od.EventID, od.MarketID, od.SelectionID)
		grouped[key] = append(grouped[key], od)
	}

	// Aggregate each group
	for key, odds := range grouped {
		normalized := a.aggregate(odds)
		a.cache[key] = normalized
	}
}

// GetOdds retrieves normalized odds
func (a *OddsAggregator) GetOdds(eventID, marketID, selectionID string) *models.NormalizedOdds {
	a.mu.RLock()
	defer a.mu.RUnlock()

	key := makeKey(eventID, marketID, selectionID)
	return a.cache[key]
}

// aggregate combines odds from multiple providers
func (a *OddsAggregator) aggregate(odds []*models.OddsData) *models.NormalizedOdds {
	if len(odds) == 0 {
		return nil
	}

	// Find best odds
	bestBack := decimal.Zero
	bestLay := decimal.NewFromInt(1000)
	sumBack := decimal.Zero
	sumLay := decimal.Zero
	count := 0

	for _, od := range odds {
		if od.BackOdds.GreaterThan(bestBack) {
			bestBack = od.BackOdds
		}
		if od.LayOdds.LessThan(bestLay) {
			bestLay = od.LayOdds
		}
		sumBack = sumBack.Add(od.BackOdds)
		sumLay = sumLay.Add(od.LayOdds)
		count++
	}

	avgBack := sumBack.Div(decimal.NewFromInt(int64(count)))
	avgLay := sumLay.Div(decimal.NewFromInt(int64(count)))

	// Calculate confidence (more providers = higher confidence)
	confidence := decimal.NewFromInt(int64(count)).Div(decimal.NewFromInt(5))
	if confidence.GreaterThan(decimal.NewFromInt(1)) {
		confidence = decimal.NewFromInt(1)
	}

	return &models.NormalizedOdds{
		EventID:     odds[0].EventID,
		MarketID:    odds[0].MarketID,
		SelectionID: odds[0].SelectionID,
		BestBack:    bestBack,
		BestLay:     bestLay,
		AvgBack:     avgBack,
		AvgLay:      avgLay,
		Sources:     count,
		Confidence:  confidence,
		UpdatedAt:   time.Now(),
	}
}

func makeKey(eventID, marketID, selectionID string) string {
	return eventID + "-" + marketID + "-" + selectionID
}
