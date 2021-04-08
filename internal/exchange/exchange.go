package exchange

import (
	"time"

	"github.com/milkywaybrain/cryptogalaxy/internal/storage"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
)

// cfgLookupKey is a key in the config lookup map.
type cfgLookupKey struct {
	market  string
	channel string
}

// cfgLookupVal is a value in the config lookup map.
type cfgLookupVal struct {
	wsConsiderIntSec int
	wsLastUpdated    time.Time
	terStr           bool
	mysqlStr         bool
	esStr            bool
	id               int
	mktCommitName    string
}

type commitData struct {
	terTickersCount   int
	terTradesCount    int
	mysqlTickersCount int
	mysqlTradesCount  int
	esTickersCount    int
	esTradesCount     int
	terTickers        []storage.Ticker
	terTrades         []storage.Trade
	mysqlTickers      []storage.Ticker
	mysqlTrades       []storage.Trade
	esTickers         []storage.Ticker
	esTrades          []storage.Trade
}

// logErrStack logs error with stack trace.
func logErrStack(err error) {
	log.Error().Stack().Err(errors.WithStack(err)).Msg("")
}
