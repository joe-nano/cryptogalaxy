package storage

import (
	"time"
)

// Ticker represents final form of market ticker info received from exchange
// ready to store.
type Ticker struct {
	Exchange      string
	MktID         string
	MktCommitName string
	Price         float64
	Timestamp     time.Time
}

// Trade represents final form of market trade info received from exchange
// ready to store.
type Trade struct {
	Exchange      string
	MktID         string
	MktCommitName string
	TradeID       uint64
	Side          string
	Size          float64
	Price         float64
	Timestamp     time.Time
}
