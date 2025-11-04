package messaging

import (
	"context"

	"github.com/cypherlabdev/data-normalizer-service/internal/models"
)

// Producer interface for publishing normalized odds
type Producer interface {
	PublishBatch(ctx context.Context, odds []models.NormalizedOdds) error
	Close() error
}

// Consumer interface for consuming raw odds
type Consumer interface {
	Start(ctx context.Context) error
	Close() error
}
