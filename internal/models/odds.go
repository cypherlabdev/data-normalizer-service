package models

import (
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
)

// Provider represents an odds provider
type Provider string

const (
	ProviderBetradar Provider = "betradar"
	ProviderBet365   Provider = "bet365"
	ProviderPinnacle Provider = "pinnacle"
	ProviderInternal Provider = "internal"
)

// Market represents a betting market
type Market struct {
	ID          string    `json:"id"`
	EventID     string    `json:"event_id"`
	Type        string    `json:"type"`        // e.g., "match-winner", "over-under"
	Name        string    `json:"name"`
	IsOpen      bool      `json:"is_open"`
	Selections  []Selection `json:"selections"`
	UpdatedAt   time.Time `json:"updated_at"`
}

// Selection represents a betting selection within a market
type Selection struct {
	ID          string          `json:"id"`
	Name        string          `json:"name"`
	BackOdds    decimal.Decimal `json:"back_odds"`    // Best back price
	LayOdds     decimal.Decimal `json:"lay_odds"`     // Best lay price
	BackVolume  decimal.Decimal `json:"back_volume"`  // Total back liquidity
	LayVolume   decimal.Decimal `json:"lay_volume"`   // Total lay liquidity
}

// OddsData represents raw odds data from a provider
type OddsData struct {
	Provider    Provider        `json:"provider"`
	EventID     string          `json:"event_id"`
	MarketID    string          `json:"market_id"`
	SelectionID string          `json:"selection_id"`
	BackOdds    decimal.Decimal `json:"back_odds"`
	LayOdds     decimal.Decimal `json:"lay_odds"`
	Volume      decimal.Decimal `json:"volume"`
	Timestamp   time.Time       `json:"timestamp"`
}

// NormalizedOdds represents aggregated odds from multiple providers
type NormalizedOdds struct {
	ID           uuid.UUID       `json:"id"`
	EventID      string          `json:"event_id"`
	EventName    string          `json:"event_name"`
	Sport        string          `json:"sport"`
	Competition  string          `json:"competition"`
	Market       string          `json:"market"`
	Selection    string          `json:"selection"`
	MarketID     string          `json:"market_id,omitempty"`     // Legacy field
	SelectionID  string          `json:"selection_id,omitempty"`  // Legacy field
	BackPrice    decimal.Decimal `json:"back_price"`
	LayPrice     decimal.Decimal `json:"lay_price"`
	BackSize     decimal.Decimal `json:"back_size"`
	LaySize      decimal.Decimal `json:"lay_size"`
	BestBack     decimal.Decimal `json:"best_back,omitempty"`     // Best back odds across providers
	BestLay      decimal.Decimal `json:"best_lay,omitempty"`      // Best lay odds across providers
	AvgBack      decimal.Decimal `json:"avg_back,omitempty"`      // Average back odds
	AvgLay       decimal.Decimal `json:"avg_lay,omitempty"`       // Average lay odds
	Sources      int             `json:"sources,omitempty"`       // Number of providers
	Confidence   decimal.Decimal `json:"confidence,omitempty"`    // Confidence score (0-1)
	Timestamp    time.Time       `json:"timestamp"`
	NormalizedAt time.Time       `json:"normalized_at"`
	UpdatedAt    time.Time       `json:"updated_at,omitempty"`    // Legacy field
}
