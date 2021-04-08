package exchange

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/milkywaybrain/cryptogalaxy/internal/config"
	"github.com/milkywaybrain/cryptogalaxy/internal/connector"
	"github.com/milkywaybrain/cryptogalaxy/internal/storage"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"
)

// StartHbtc is for starting hbtc exchange functions.
func StartHbtc(appCtx context.Context, markets []config.Market, retry *config.Retry, connCfg *config.Connection) error {

	// If any error occurs or connection is lost, retry the exchange functions with a time gap till it reaches
	// a configured number of retry.
	// Retry counter will be reset back to zero if the elapsed time since the last retry is greater than the configured one.
	var retryCount int
	lastRetryTime := time.Now()

	for {
		err := newHbtc(appCtx, markets, connCfg)
		if err != nil {
			log.Error().Err(err).Str("exchange", "hbtc").Msg("error occurred")
			if retry.Number == 0 {
				return errors.New("not able to connect hbtc exchange. please check the log for details")
			}
			if retry.ResetSec == 0 || time.Since(lastRetryTime).Seconds() < float64(retry.ResetSec) {
				retryCount++
			} else {
				retryCount = 1
			}
			lastRetryTime = time.Now()
			if retryCount > retry.Number {
				return fmt.Errorf("not able to connect hbtc exchange even after %v retry. please check the log for details", retry.Number)
			}

			log.Error().Str("exchange", "hbtc").Int("retry", retryCount).Msg(fmt.Sprintf("retrying functions in %v seconds", retry.GapSec))
			tick := time.NewTicker(time.Duration(retry.GapSec) * time.Second)
			select {
			case <-tick.C:
				tick.Stop()

			// Return, if there is any error from another exchange.
			case <-appCtx.Done():
				log.Error().Str("exchange", "hbtc").Msg("ctx canceled, return from StartHbtc")
				return appCtx.Err()
			}
		}
	}
}

type hbtc struct {
	ws             connector.Websocket
	rest           *connector.REST
	connCfg        *config.Connection
	cfgMap         map[cfgLookupKey]cfgLookupVal
	ter            *storage.Terminal
	es             *storage.ElasticSearch
	mysql          *storage.MySQL
	wsTerTickers   chan []storage.Ticker
	wsTerTrades    chan []storage.Trade
	wsMysqlTickers chan []storage.Ticker
	wsMysqlTrades  chan []storage.Trade
	wsEsTickers    chan []storage.Ticker
	wsEsTrades     chan []storage.Trade
}

type wsSubHbtc struct {
	Topic  string          `json:"topic"`
	Event  string          `json:"event"`
	Params wsSubParamsHbtc `json:"params"`
}

type wsSubParamsHbtc struct {
	Symbol string `json:"symbol"`
}

type wsRespHbtc struct {
	Pong          int64           `json:"pong"`
	Topic         string          `json:"topic"`
	Event         string          `json:"event"`
	Params        wsSubParamsHbtc `json:"params"`
	Data          wsRespDataHbtc  `json:"data"`
	Code          string          `json:"code"`
	Msg           string          `json:"msg"`
	mktCommitName string
}

type wsRespDataHbtc struct {
	Qty         string              `json:"q"`
	TickerPrice string              `json:"c"`
	TradePrice  string              `json:"p"`
	Time        int64               `json:"t"`
	Maker       jsoniter.RawMessage `json:"m"`
}

type restRespHbtc struct {
	Maker bool   `json:"isBuyerMaker"`
	Qty   string `json:"qty"`
	Price string `json:"price"`
	Time  int64  `json:"time"`
}

func newHbtc(appCtx context.Context, markets []config.Market, connCfg *config.Connection) error {

	// If any exchange function fails, force all the other functions to stop and return.
	hbtcErrGroup, ctx := errgroup.WithContext(appCtx)

	h := hbtc{connCfg: connCfg}

	err := h.cfgLookup(markets)
	if err != nil {
		return err
	}

	var (
		wsCount   int
		restCount int
	)

	for _, market := range markets {
		for _, info := range market.Info {
			switch info.Connector {
			case "websocket":
				if wsCount == 0 {

					err = h.connectWs(ctx)
					if err != nil {
						return err
					}

					hbtcErrGroup.Go(func() error {
						return h.closeWsConnOnError(ctx)
					})

					hbtcErrGroup.Go(func() error {
						return h.pingWs(ctx)
					})

					hbtcErrGroup.Go(func() error {
						return h.readWs(ctx)
					})

					if h.ter != nil {
						hbtcErrGroup.Go(func() error {
							return h.wsTickersToTerminal(ctx)
						})
						hbtcErrGroup.Go(func() error {
							return h.wsTradesToTerminal(ctx)
						})
					}

					if h.mysql != nil {
						hbtcErrGroup.Go(func() error {
							return h.wsTickersToMySQL(ctx)
						})
						hbtcErrGroup.Go(func() error {
							return h.wsTradesToMySQL(ctx)
						})
					}

					if h.es != nil {
						hbtcErrGroup.Go(func() error {
							return h.wsTickersToES(ctx)
						})
						hbtcErrGroup.Go(func() error {
							return h.wsTradesToES(ctx)
						})
					}
				}

				err = h.subWsChannel(market.ID, info.Channel)
				if err != nil {
					return err
				}

				wsCount++
			case "rest":
				if restCount == 0 {
					err = h.connectRest()
					if err != nil {
						return err
					}
				}

				var mktCommitName string
				if market.CommitName != "" {
					mktCommitName = market.CommitName
				} else {
					mktCommitName = market.ID
				}
				mktID := market.ID
				channel := info.Channel
				restPingIntSec := info.RESTPingIntSec
				hbtcErrGroup.Go(func() error {
					return h.processREST(ctx, mktID, mktCommitName, channel, restPingIntSec)
				})

				restCount++
			}
		}
	}

	err = hbtcErrGroup.Wait()
	if err != nil {
		return err
	}
	return nil
}

func (h *hbtc) cfgLookup(markets []config.Market) error {

	// Configurations flat map is prepared for easy lookup later in the app.
	h.cfgMap = make(map[cfgLookupKey]cfgLookupVal)
	for _, market := range markets {
		var mktCommitName string
		if market.CommitName != "" {
			mktCommitName = market.CommitName
		} else {
			mktCommitName = market.ID
		}
		for _, info := range market.Info {
			key := cfgLookupKey{market: market.ID, channel: info.Channel}
			val := cfgLookupVal{}
			val.wsConsiderIntSec = info.WsConsiderIntSec
			for _, str := range info.Storages {
				switch str {
				case "terminal":
					val.terStr = true
					if h.ter == nil {
						h.ter = storage.GetTerminal()
						h.wsTerTickers = make(chan []storage.Ticker, 1)
						h.wsTerTrades = make(chan []storage.Trade, 1)
					}
				case "mysql":
					val.mysqlStr = true
					if h.mysql == nil {
						h.mysql = storage.GetMySQL()
						h.wsMysqlTickers = make(chan []storage.Ticker, 1)
						h.wsMysqlTrades = make(chan []storage.Trade, 1)
					}
				case "elastic_search":
					val.esStr = true
					if h.es == nil {
						h.es = storage.GetElasticSearch()
						h.wsEsTickers = make(chan []storage.Ticker, 1)
						h.wsEsTrades = make(chan []storage.Trade, 1)
					}
				}
			}
			val.mktCommitName = mktCommitName
			h.cfgMap[key] = val
		}
	}
	return nil
}

func (h *hbtc) connectWs(ctx context.Context) error {
	ws, err := connector.NewWebsocket(ctx, &h.connCfg.WS, config.HbtcWebsocketURL)
	if err != nil {
		if !errors.Is(err, ctx.Err()) {
			logErrStack(err)
		}
		return err
	}
	h.ws = ws
	log.Info().Str("exchange", "hbtc").Msg("websocket connected")
	return nil
}

// closeWsConnOnError closes websocket connection if there is any error in app context.
// This will unblock all read and writes on websocket.
func (h *hbtc) closeWsConnOnError(ctx context.Context) error {
	<-ctx.Done()
	err := h.ws.Conn.Close()
	if err != nil {
		return err
	}
	return ctx.Err()
}

// pingWs sends ping request to websocket server for every 4 minutes (~10% earlier to required 5 minutes on a safer side).
func (h *hbtc) pingWs(ctx context.Context) error {
	tick := time.NewTicker(4 * time.Minute)
	defer tick.Stop()
	for {
		select {
		case <-tick.C:
			frame, err := jsoniter.Marshal(map[string]int64{"ping": time.Now().Unix()})
			if err != nil {
				logErrStack(err)
				return err
			}
			err = h.ws.Write(frame)
			if err != nil {
				if errors.Is(err, net.ErrClosed) {
					err = errors.New("context canceled")
				} else {
					logErrStack(err)
				}
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// subWsChannel sends channel subscription requests to the websocket server.
func (h *hbtc) subWsChannel(market string, channel string) error {
	if channel == "ticker" {
		channel = "realtimes"
	}
	sub := wsSubHbtc{
		Topic: channel,
		Event: "sub",
		Params: wsSubParamsHbtc{
			Symbol: market,
		},
	}
	frame, err := jsoniter.Marshal(sub)
	if err != nil {
		logErrStack(err)
		return err
	}
	err = h.ws.Write(frame)
	if err != nil {
		if errors.Is(err, net.ErrClosed) {
			err = errors.New("context canceled")
		} else {
			logErrStack(err)
		}
		return err
	}
	return nil
}

// readWs reads ticker / trade data from websocket channels.
func (h *hbtc) readWs(ctx context.Context) error {

	// To avoid data race, creating a new local lookup map.
	cfgLookup := make(map[cfgLookupKey]cfgLookupVal, len(h.cfgMap))
	for k, v := range h.cfgMap {
		cfgLookup[k] = v
	}

	cd := commitData{
		terTickers:   make([]storage.Ticker, 0, h.connCfg.Terminal.TickerCommitBuf),
		terTrades:    make([]storage.Trade, 0, h.connCfg.Terminal.TradeCommitBuf),
		mysqlTickers: make([]storage.Ticker, 0, h.connCfg.MySQL.TickerCommitBuf),
		mysqlTrades:  make([]storage.Trade, 0, h.connCfg.MySQL.TradeCommitBuf),
		esTickers:    make([]storage.Ticker, 0, h.connCfg.ES.TickerCommitBuf),
		esTrades:     make([]storage.Trade, 0, h.connCfg.ES.TradeCommitBuf),
	}

	for {
		select {
		default:
			frame, err := h.ws.Read()
			if err != nil {
				if errors.Is(err, net.ErrClosed) {
					err = errors.New("context canceled")
				} else {
					if err == io.EOF {
						err = errors.Wrap(err, "connection close by exchange server")
					}
					logErrStack(err)
				}
				return err
			}
			if len(frame) == 0 {
				continue
			}

			wr := wsRespHbtc{}
			err = jsoniter.Unmarshal(frame, &wr)
			if err != nil {
				logErrStack(err)
				return err
			}

			if wr.Pong > 0 {
				continue
			}

			if wr.Topic == "realtimes" {
				wr.Topic = "ticker"
			}

			if wr.Msg == "Success" && wr.Event == "sub" {
				log.Debug().Str("exchange", "hbtc").Str("func", "readWs").Str("market", wr.Params.Symbol).Str("channel", wr.Topic).Msg("channel subscribed")
				continue
			}

			// Consider frame only in configured interval, otherwise ignore it.
			switch wr.Topic {
			case "ticker", "trade":
				key := cfgLookupKey{market: wr.Params.Symbol, channel: wr.Topic}
				val := cfgLookup[key]
				if val.wsConsiderIntSec == 0 || time.Since(val.wsLastUpdated).Seconds() >= float64(val.wsConsiderIntSec) {
					val.wsLastUpdated = time.Now()
					wr.mktCommitName = val.mktCommitName
					cfgLookup[key] = val
				} else {
					continue
				}

				err := h.processWs(ctx, &wr, &cd)
				if err != nil {
					return err
				}
			}

		// Return, if there is any error from another function or exchange.
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// processWs receives ticker / trade data,
// transforms it to a common ticker / trade store format,
// buffers the same in memory and
// then sends it to different storage systems for commit through go channels.
func (h *hbtc) processWs(ctx context.Context, wr *wsRespHbtc, cd *commitData) error {
	switch wr.Topic {
	case "ticker":
		ticker := storage.Ticker{}
		ticker.Exchange = "hbtc"
		ticker.MktID = wr.Params.Symbol
		ticker.MktCommitName = wr.mktCommitName

		// Price sent is in string format.
		price, err := strconv.ParseFloat(wr.Data.TickerPrice, 64)
		if err != nil {
			logErrStack(err)
			return err
		}
		ticker.Price = price

		// Time sent is in milliseconds.
		ticker.Timestamp = time.Unix(0, wr.Data.Time*int64(time.Millisecond)).UTC()

		key := cfgLookupKey{market: ticker.MktID, channel: "ticker"}
		val := h.cfgMap[key]
		if val.terStr {
			cd.terTickersCount++
			cd.terTickers = append(cd.terTickers, ticker)
			if cd.terTickersCount == h.connCfg.Terminal.TickerCommitBuf {
				select {
				case h.wsTerTickers <- cd.terTickers:
				case <-ctx.Done():
					return ctx.Err()
				}
				cd.terTickersCount = 0
				cd.terTickers = nil
			}
		}
		if val.mysqlStr {
			cd.mysqlTickersCount++
			cd.mysqlTickers = append(cd.mysqlTickers, ticker)
			if cd.mysqlTickersCount == h.connCfg.MySQL.TickerCommitBuf {
				select {
				case h.wsMysqlTickers <- cd.mysqlTickers:
				case <-ctx.Done():
					return ctx.Err()
				}
				cd.mysqlTickersCount = 0
				cd.mysqlTickers = nil
			}
		}
		if val.esStr {
			cd.esTickersCount++
			cd.esTickers = append(cd.esTickers, ticker)
			if cd.esTickersCount == h.connCfg.ES.TickerCommitBuf {
				select {
				case h.wsEsTickers <- cd.esTickers:
				case <-ctx.Done():
					return ctx.Err()
				}
				cd.esTickersCount = 0
				cd.esTickers = nil
			}
		}
	case "trade":
		trade := storage.Trade{}
		trade.Exchange = "hbtc"
		trade.MktID = wr.Params.Symbol
		trade.MktCommitName = wr.mktCommitName

		// Maker sent is in bool format for trade.
		// (Maker sent is in string format for ticker which has different meaning, so the json raw type)
		maker, err := strconv.ParseBool(string(wr.Data.Maker))
		if err != nil {
			logErrStack(err)
			return err
		}
		if maker {
			trade.Side = "buy"
		} else {
			trade.Side = "sell"
		}

		size, err := strconv.ParseFloat(wr.Data.Qty, 64)
		if err != nil {
			logErrStack(err)
			return err
		}
		trade.Size = size

		price, err := strconv.ParseFloat(wr.Data.TradePrice, 64)
		if err != nil {
			logErrStack(err)
			return err
		}
		trade.Price = price

		// Time sent is in milliseconds.
		trade.Timestamp = time.Unix(0, wr.Data.Time*int64(time.Millisecond)).UTC()

		key := cfgLookupKey{market: trade.MktID, channel: "trade"}
		val := h.cfgMap[key]
		if val.terStr {
			cd.terTradesCount++
			cd.terTrades = append(cd.terTrades, trade)
			if cd.terTradesCount == h.connCfg.Terminal.TradeCommitBuf {
				select {
				case h.wsTerTrades <- cd.terTrades:
				case <-ctx.Done():
					return ctx.Err()
				}
				cd.terTradesCount = 0
				cd.terTrades = nil
			}
		}
		if val.mysqlStr {
			cd.mysqlTradesCount++
			cd.mysqlTrades = append(cd.mysqlTrades, trade)
			if cd.mysqlTradesCount == h.connCfg.MySQL.TradeCommitBuf {
				select {
				case h.wsMysqlTrades <- cd.mysqlTrades:
				case <-ctx.Done():
					return ctx.Err()
				}
				cd.mysqlTradesCount = 0
				cd.mysqlTrades = nil
			}
		}
		if val.esStr {
			cd.esTradesCount++
			cd.esTrades = append(cd.esTrades, trade)
			if cd.esTradesCount == h.connCfg.ES.TradeCommitBuf {
				select {
				case h.wsEsTrades <- cd.esTrades:
				case <-ctx.Done():
					return ctx.Err()
				}
				cd.esTradesCount = 0
				cd.esTrades = nil
			}
		}
	}
	return nil
}

func (h *hbtc) wsTickersToTerminal(ctx context.Context) error {
	for {
		select {
		case data := <-h.wsTerTickers:
			h.ter.CommitTickers(data)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (h *hbtc) wsTradesToTerminal(ctx context.Context) error {
	for {
		select {
		case data := <-h.wsTerTrades:
			h.ter.CommitTrades(data)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (h *hbtc) wsTickersToMySQL(ctx context.Context) error {
	for {
		select {
		case data := <-h.wsMysqlTickers:
			err := h.mysql.CommitTickers(ctx, data)
			if err != nil {
				if !errors.Is(err, ctx.Err()) {
					logErrStack(err)
				}
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (h *hbtc) wsTradesToMySQL(ctx context.Context) error {
	for {
		select {
		case data := <-h.wsMysqlTrades:
			err := h.mysql.CommitTrades(ctx, data)
			if err != nil {
				if !errors.Is(err, ctx.Err()) {
					logErrStack(err)
				}
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (h *hbtc) wsTickersToES(ctx context.Context) error {
	for {
		select {
		case data := <-h.wsEsTickers:
			err := h.es.CommitTickers(ctx, data)
			if err != nil {
				if !errors.Is(err, ctx.Err()) {
					logErrStack(err)
				}
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (h *hbtc) wsTradesToES(ctx context.Context) error {
	for {
		select {
		case data := <-h.wsEsTrades:
			err := h.es.CommitTrades(ctx, data)
			if err != nil {
				if !errors.Is(err, ctx.Err()) {
					logErrStack(err)
				}
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (h *hbtc) connectRest() error {
	rest, err := connector.GetREST()
	if err != nil {
		logErrStack(err)
		return err
	}
	h.rest = rest
	log.Info().Str("exchange", "hbtc").Msg("REST connection setup is done")
	return nil
}

// processREST queries exchange for ticker / trade data through REST API in configured intervals,
// transforms it to a common ticker / trade store format,
// buffers the same in memory and
// then sends it to different storage systems for commit through go channels.
func (h *hbtc) processREST(ctx context.Context, mktID string, mktCommitName string, channel string, interval int) error {
	var (
		req *http.Request
		q   url.Values
		err error
	)

	cd := commitData{
		terTickers:   make([]storage.Ticker, 0, h.connCfg.Terminal.TickerCommitBuf),
		terTrades:    make([]storage.Trade, 0, h.connCfg.Terminal.TradeCommitBuf),
		mysqlTickers: make([]storage.Ticker, 0, h.connCfg.MySQL.TickerCommitBuf),
		mysqlTrades:  make([]storage.Trade, 0, h.connCfg.MySQL.TradeCommitBuf),
		esTickers:    make([]storage.Ticker, 0, h.connCfg.ES.TickerCommitBuf),
		esTrades:     make([]storage.Trade, 0, h.connCfg.ES.TradeCommitBuf),
	}

	switch channel {
	case "ticker":
		req, err = h.rest.Request(ctx, config.HbtcRESTBaseURL+"openapi/quote/v1/ticker/price")
		if err != nil {
			if !errors.Is(err, ctx.Err()) {
				logErrStack(err)
			}
			return err
		}
		q = req.URL.Query()
		q.Add("symbol", mktID)
	case "trade":
		req, err = h.rest.Request(ctx, config.HbtcRESTBaseURL+"openapi/quote/v1/trades")
		if err != nil {
			if !errors.Is(err, ctx.Err()) {
				logErrStack(err)
			}
			return err
		}
		q = req.URL.Query()
		q.Add("symbol", mktID)

		// Querying for 100 trades.
		// If the configured interval gap is big, then maybe it will not return all the trades
		// and if the gap is too small, maybe it will return duplicate ones.
		// Better to use websocket.
		q.Add("limit", strconv.Itoa(100))
	}

	tick := time.NewTicker(time.Duration(interval) * time.Second)
	defer tick.Stop()
	for {
		select {
		case <-tick.C:

			switch channel {
			case "ticker":
				req.URL.RawQuery = q.Encode()
				resp, err := h.rest.Do(req)
				if err != nil {
					if !errors.Is(err, ctx.Err()) {
						logErrStack(err)
					}
					return err
				}

				rr := restRespHbtc{}
				if err = jsoniter.NewDecoder(resp.Body).Decode(&rr); err != nil {
					logErrStack(err)
					resp.Body.Close()
					return err
				}
				resp.Body.Close()

				price, err := strconv.ParseFloat(rr.Price, 64)
				if err != nil {
					logErrStack(err)
					return err
				}

				ticker := storage.Ticker{
					Exchange:      "hbtc",
					MktID:         mktID,
					MktCommitName: mktCommitName,
					Price:         price,
					Timestamp:     time.Now().UTC(),
				}

				key := cfgLookupKey{market: ticker.MktID, channel: "ticker"}
				val := h.cfgMap[key]
				if val.terStr {
					cd.terTickersCount++
					cd.terTickers = append(cd.terTickers, ticker)
					if cd.terTickersCount == h.connCfg.Terminal.TickerCommitBuf {
						h.ter.CommitTickers(cd.terTickers)
						cd.terTickersCount = 0
						cd.terTickers = nil
					}
				}
				if val.mysqlStr {
					cd.mysqlTickersCount++
					cd.mysqlTickers = append(cd.mysqlTickers, ticker)
					if cd.mysqlTickersCount == h.connCfg.MySQL.TickerCommitBuf {
						err := h.mysql.CommitTickers(ctx, cd.mysqlTickers)
						if err != nil {
							if !errors.Is(err, ctx.Err()) {
								logErrStack(err)
							}
							return err
						}
						cd.mysqlTickersCount = 0
						cd.mysqlTickers = nil
					}
				}
				if val.esStr {
					cd.esTickersCount++
					cd.esTickers = append(cd.esTickers, ticker)
					if cd.esTickersCount == h.connCfg.ES.TickerCommitBuf {
						err := h.es.CommitTickers(ctx, cd.esTickers)
						if err != nil {
							if !errors.Is(err, ctx.Err()) {
								logErrStack(err)
							}
							return err
						}
						cd.esTickersCount = 0
						cd.esTickers = nil
					}
				}
			case "trade":
				req.URL.RawQuery = q.Encode()
				resp, err := h.rest.Do(req)
				if err != nil {
					if !errors.Is(err, ctx.Err()) {
						logErrStack(err)
					}
					return err
				}

				rr := []restRespHbtc{}
				if err := jsoniter.NewDecoder(resp.Body).Decode(&rr); err != nil {
					logErrStack(err)
					resp.Body.Close()
					return err
				}
				resp.Body.Close()

				for i := range rr {
					r := rr[i]
					var side string
					if r.Maker {
						side = "buy"
					} else {
						side = "sell"
					}

					size, err := strconv.ParseFloat(r.Qty, 64)
					if err != nil {
						logErrStack(err)
						return err
					}

					price, err := strconv.ParseFloat(r.Price, 64)
					if err != nil {
						logErrStack(err)
						return err
					}

					// Time sent is in milliseconds.
					timestamp := time.Unix(0, r.Time*int64(time.Millisecond)).UTC()

					trade := storage.Trade{
						Exchange:      "hbtc",
						MktID:         mktID,
						MktCommitName: mktCommitName,
						Side:          side,
						Size:          size,
						Price:         price,
						Timestamp:     timestamp,
					}

					key := cfgLookupKey{market: trade.MktID, channel: "trade"}
					val := h.cfgMap[key]
					if val.terStr {
						cd.terTradesCount++
						cd.terTrades = append(cd.terTrades, trade)
						if cd.terTradesCount == h.connCfg.Terminal.TradeCommitBuf {
							h.ter.CommitTrades(cd.terTrades)
							cd.terTradesCount = 0
							cd.terTrades = nil
						}
					}
					if val.mysqlStr {
						cd.mysqlTradesCount++
						cd.mysqlTrades = append(cd.mysqlTrades, trade)
						if cd.mysqlTradesCount == h.connCfg.MySQL.TradeCommitBuf {
							err := h.mysql.CommitTrades(ctx, cd.mysqlTrades)
							if err != nil {
								if !errors.Is(err, ctx.Err()) {
									logErrStack(err)
								}
								return err
							}
							cd.mysqlTradesCount = 0
							cd.mysqlTrades = nil
						}
					}
					if val.esStr {
						cd.esTradesCount++
						cd.esTrades = append(cd.esTrades, trade)
						if cd.esTradesCount == h.connCfg.ES.TradeCommitBuf {
							err := h.es.CommitTrades(ctx, cd.esTrades)
							if err != nil {
								if !errors.Is(err, ctx.Err()) {
									logErrStack(err)
								}
								return err
							}
							cd.esTradesCount = 0
							cd.esTrades = nil
						}
					}
				}
			}

		// Return, if there is any error from another function or exchange.
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
