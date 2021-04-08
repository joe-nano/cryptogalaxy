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

// StartCoinbasePro is for starting coinbase-pro exchange functions.
func StartCoinbasePro(appCtx context.Context, markets []config.Market, retry *config.Retry, connCfg *config.Connection) error {

	// If any error occurs or connection is lost, retry the exchange functions with a time gap till it reaches
	// a configured number of retry.
	// Retry counter will be reset back to zero if the elapsed time since the last retry is greater than the configured one.
	var retryCount int
	lastRetryTime := time.Now()

	for {
		err := newCoinbasePro(appCtx, markets, connCfg)
		if err != nil {
			log.Error().Err(err).Str("exchange", "coinbase-pro").Msg("error occurred")
			if retry.Number == 0 {
				return errors.New("not able to connect coinbase-pro exchange. please check the log for details")
			}
			if retry.ResetSec == 0 || time.Since(lastRetryTime).Seconds() < float64(retry.ResetSec) {
				retryCount++
			} else {
				retryCount = 1
			}
			lastRetryTime = time.Now()
			if retryCount > retry.Number {
				return fmt.Errorf("not able to connect coinbase-pro exchange even after %v retry. please check the log for details", retry.Number)
			}

			log.Error().Str("exchange", "coinbase-pro").Int("retry", retryCount).Msg(fmt.Sprintf("retrying functions in %v seconds", retry.GapSec))
			tick := time.NewTicker(time.Duration(retry.GapSec) * time.Second)
			select {
			case <-tick.C:
				tick.Stop()

			// Return, if there is any error from another exchange.
			case <-appCtx.Done():
				log.Error().Str("exchange", "coinbase-pro").Msg("ctx canceled, return from StartCoinbasePro")
				return appCtx.Err()
			}
		}
	}
}

type coinbasePro struct {
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

type wsSubCoinPro struct {
	Type     string             `json:"type"`
	Channels []wsSubChanCoinPro `json:"channels"`
}

type wsSubChanCoinPro struct {
	Name       string    `json:"name"`
	ProductIds [1]string `json:"product_ids"`
}

type respCoinPro struct {
	Type          string             `json:"type"`
	ProductID     string             `json:"product_id"`
	TradeID       uint64             `json:"trade_id"`
	Side          string             `json:"side"`
	Size          string             `json:"size"`
	Price         string             `json:"price"`
	Time          string             `json:"time"`
	Message       string             `json:"message"`
	Channels      []wsSubChanCoinPro `json:"channels"`
	mktCommitName string
}

func newCoinbasePro(appCtx context.Context, markets []config.Market, connCfg *config.Connection) error {

	// If any exchange function fails, force all the other functions to stop and return.
	coinbaseProErrGroup, ctx := errgroup.WithContext(appCtx)

	c := coinbasePro{connCfg: connCfg}

	err := c.cfgLookup(markets)
	if err != nil {
		return err
	}

	var (
		wsCount   int
		restCount int
		threshold int
	)

	for _, market := range markets {
		for _, info := range market.Info {
			switch info.Connector {
			case "websocket":
				if wsCount == 0 {

					err = c.connectWs(ctx)
					if err != nil {
						return err
					}

					coinbaseProErrGroup.Go(func() error {
						return c.closeWsConnOnError(ctx)
					})

					coinbaseProErrGroup.Go(func() error {
						return c.readWs(ctx)
					})

					if c.ter != nil {
						coinbaseProErrGroup.Go(func() error {
							return c.wsTickersToTerminal(ctx)
						})
						coinbaseProErrGroup.Go(func() error {
							return c.wsTradesToTerminal(ctx)
						})
					}

					if c.mysql != nil {
						coinbaseProErrGroup.Go(func() error {
							return c.wsTickersToMySQL(ctx)
						})
						coinbaseProErrGroup.Go(func() error {
							return c.wsTradesToMySQL(ctx)
						})
					}

					if c.es != nil {
						coinbaseProErrGroup.Go(func() error {
							return c.wsTickersToES(ctx)
						})
						coinbaseProErrGroup.Go(func() error {
							return c.wsTradesToES(ctx)
						})
					}
				}

				err = c.subWsChannel(market.ID, info.Channel)
				if err != nil {
					return err
				}
				wsCount++

				// Maximum messages sent to a websocket connection per sec is 100.
				// So on a safer side, this will wait for 2 sec before proceeding once it reaches ~90% of the limit.
				threshold++
				if threshold == 90 {
					log.Debug().Str("exchange", "coinbase-pro").Int("count", threshold).Msg("subscribe threshold reached, waiting 2 sec")
					time.Sleep(2 * time.Second)
					threshold = 0
				}

			case "rest":
				if restCount == 0 {
					err = c.connectRest()
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
				coinbaseProErrGroup.Go(func() error {
					return c.processREST(ctx, mktID, mktCommitName, channel, restPingIntSec)
				})

				restCount++
			}
		}
	}

	err = coinbaseProErrGroup.Wait()
	if err != nil {
		return err
	}
	return nil
}

func (c *coinbasePro) cfgLookup(markets []config.Market) error {

	// Configurations flat map is prepared for easy lookup later in the app.
	c.cfgMap = make(map[cfgLookupKey]cfgLookupVal)
	for _, market := range markets {
		var marketCommitName string
		if market.CommitName != "" {
			marketCommitName = market.CommitName
		} else {
			marketCommitName = market.ID
		}
		for _, info := range market.Info {
			key := cfgLookupKey{market: market.ID, channel: info.Channel}
			val := cfgLookupVal{}
			val.wsConsiderIntSec = info.WsConsiderIntSec
			for _, str := range info.Storages {
				switch str {
				case "terminal":
					val.terStr = true
					if c.ter == nil {
						c.ter = storage.GetTerminal()
						c.wsTerTickers = make(chan []storage.Ticker, 1)
						c.wsTerTrades = make(chan []storage.Trade, 1)
					}
				case "mysql":
					val.mysqlStr = true
					if c.mysql == nil {
						c.mysql = storage.GetMySQL()
						c.wsMysqlTickers = make(chan []storage.Ticker, 1)
						c.wsMysqlTrades = make(chan []storage.Trade, 1)
					}
				case "elastic_search":
					val.esStr = true
					if c.es == nil {
						c.es = storage.GetElasticSearch()
						c.wsEsTickers = make(chan []storage.Ticker, 1)
						c.wsEsTrades = make(chan []storage.Trade, 1)
					}
				}
			}
			val.mktCommitName = marketCommitName
			c.cfgMap[key] = val
		}
	}
	return nil
}

func (c *coinbasePro) connectWs(ctx context.Context) error {
	ws, err := connector.NewWebsocket(ctx, &c.connCfg.WS, config.CoinbaseProWebsocketURL)
	if err != nil {
		if !errors.Is(err, ctx.Err()) {
			logErrStack(err)
		}
		return err
	}
	c.ws = ws
	log.Info().Str("exchange", "coinbase-pro").Msg("websocket connected")
	return nil
}

// closeWsConnOnError closes websocket connection if there is any error in app context.
// This will unblock all read and writes on websocket.
func (c *coinbasePro) closeWsConnOnError(ctx context.Context) error {
	<-ctx.Done()
	err := c.ws.Conn.Close()
	if err != nil {
		return err
	}
	return ctx.Err()
}

// subWsChannel sends channel subscription requests to the websocket server.
func (c *coinbasePro) subWsChannel(market string, channel string) error {
	if channel == "trade" {
		channel = "matches"
	}
	channels := make([]wsSubChanCoinPro, 1)
	channels[0].Name = channel
	channels[0].ProductIds = [1]string{market}
	sub := wsSubCoinPro{
		Type:     "subscribe",
		Channels: channels,
	}
	frame, err := jsoniter.Marshal(sub)
	if err != nil {
		logErrStack(err)
		return err
	}
	err = c.ws.Write(frame)
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
func (c *coinbasePro) readWs(ctx context.Context) error {

	// To avoid data race, creating a new local lookup map.
	cfgLookup := make(map[cfgLookupKey]cfgLookupVal, len(c.cfgMap))
	for k, v := range c.cfgMap {
		cfgLookup[k] = v
	}

	cd := commitData{
		terTickers:   make([]storage.Ticker, 0, c.connCfg.Terminal.TickerCommitBuf),
		terTrades:    make([]storage.Trade, 0, c.connCfg.Terminal.TradeCommitBuf),
		mysqlTickers: make([]storage.Ticker, 0, c.connCfg.MySQL.TickerCommitBuf),
		mysqlTrades:  make([]storage.Trade, 0, c.connCfg.MySQL.TradeCommitBuf),
		esTickers:    make([]storage.Ticker, 0, c.connCfg.ES.TickerCommitBuf),
		esTrades:     make([]storage.Trade, 0, c.connCfg.ES.TradeCommitBuf),
	}

	for {
		select {
		default:
			frame, err := c.ws.Read()
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

			wr := respCoinPro{}
			err = jsoniter.Unmarshal(frame, &wr)
			if err != nil {
				logErrStack(err)
				return err
			}

			if wr.Type == "match" {
				wr.Type = "trade"
			}

			switch wr.Type {
			case "error":
				log.Error().Str("exchange", "coinbase-pro").Str("func", "readWs").Str("msg", wr.Message).Msg("")
				return errors.New("coinbase-pro websocket error")
			case "subscriptions":
				for _, channel := range wr.Channels {
					for _, market := range channel.ProductIds {
						if channel.Name == "matches" {
							log.Debug().Str("exchange", "coinbase-pro").Str("func", "readWs").Str("market", market).Str("channel", "trade").Msg("channel subscribed (this message may be duplicate as server sends list of all subscriptions on each channel subscribe)")
						} else {
							log.Debug().Str("exchange", "coinbase-pro").Str("func", "readWs").Str("market", market).Str("channel", channel.Name).Msg("channel subscribed (this message may be duplicate as server sends list of all subscriptions on each channel subscribe)")
						}
					}
				}

			// Consider frame only in configured interval, otherwise ignore it.
			case "ticker", "trade":
				key := cfgLookupKey{market: wr.ProductID, channel: wr.Type}
				val := cfgLookup[key]
				if val.wsConsiderIntSec == 0 || time.Since(val.wsLastUpdated).Seconds() >= float64(val.wsConsiderIntSec) {
					val.wsLastUpdated = time.Now()
					wr.mktCommitName = val.mktCommitName
					cfgLookup[key] = val
				} else {
					continue
				}

				err := c.processWs(ctx, &wr, &cd)
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
func (c *coinbasePro) processWs(ctx context.Context, wr *respCoinPro, cd *commitData) error {
	switch wr.Type {
	case "ticker":
		ticker := storage.Ticker{}
		ticker.Exchange = "coinbase-pro"
		ticker.MktID = wr.ProductID
		ticker.MktCommitName = wr.mktCommitName

		price, err := strconv.ParseFloat(wr.Price, 64)
		if err != nil {
			logErrStack(err)
			return err
		}
		ticker.Price = price

		// Time sent is in string format.
		timestamp, err := time.Parse(time.RFC3339Nano, wr.Time)
		if err != nil {
			logErrStack(err)
			return err
		}
		ticker.Timestamp = timestamp

		key := cfgLookupKey{market: ticker.MktID, channel: "ticker"}
		val := c.cfgMap[key]
		if val.terStr {
			cd.terTickersCount++
			cd.terTickers = append(cd.terTickers, ticker)
			if cd.terTickersCount == c.connCfg.Terminal.TickerCommitBuf {
				select {
				case c.wsTerTickers <- cd.terTickers:
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
			if cd.mysqlTickersCount == c.connCfg.MySQL.TickerCommitBuf {
				select {
				case c.wsMysqlTickers <- cd.mysqlTickers:
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
			if cd.esTickersCount == c.connCfg.ES.TickerCommitBuf {
				select {
				case c.wsEsTickers <- cd.esTickers:
				case <-ctx.Done():
					return ctx.Err()
				}
				cd.esTickersCount = 0
				cd.esTickers = nil
			}
		}
	case "trade":
		trade := storage.Trade{}
		trade.Exchange = "coinbase-pro"
		trade.MktID = wr.ProductID
		trade.MktCommitName = wr.mktCommitName
		trade.TradeID = wr.TradeID
		trade.Side = wr.Side

		size, err := strconv.ParseFloat(wr.Size, 64)
		if err != nil {
			logErrStack(err)
			return err
		}
		trade.Size = size

		price, err := strconv.ParseFloat(wr.Price, 64)
		if err != nil {
			logErrStack(err)
			return err
		}
		trade.Price = price

		// Time sent is in string format.
		timestamp, err := time.Parse(time.RFC3339Nano, wr.Time)
		if err != nil {
			logErrStack(err)
			return err
		}
		trade.Timestamp = timestamp

		key := cfgLookupKey{market: trade.MktID, channel: "trade"}
		val := c.cfgMap[key]
		if val.terStr {
			cd.terTradesCount++
			cd.terTrades = append(cd.terTrades, trade)
			if cd.terTradesCount == c.connCfg.Terminal.TradeCommitBuf {
				select {
				case c.wsTerTrades <- cd.terTrades:
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
			if cd.mysqlTradesCount == c.connCfg.MySQL.TradeCommitBuf {
				select {
				case c.wsMysqlTrades <- cd.mysqlTrades:
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
			if cd.esTradesCount == c.connCfg.ES.TradeCommitBuf {
				select {
				case c.wsEsTrades <- cd.esTrades:
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

func (c *coinbasePro) wsTickersToTerminal(ctx context.Context) error {
	for {
		select {
		case data := <-c.wsTerTickers:
			c.ter.CommitTickers(data)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (c *coinbasePro) wsTradesToTerminal(ctx context.Context) error {
	for {
		select {
		case data := <-c.wsTerTrades:
			c.ter.CommitTrades(data)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (c *coinbasePro) wsTickersToMySQL(ctx context.Context) error {
	for {
		select {
		case data := <-c.wsMysqlTickers:
			err := c.mysql.CommitTickers(ctx, data)
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

func (c *coinbasePro) wsTradesToMySQL(ctx context.Context) error {
	for {
		select {
		case data := <-c.wsMysqlTrades:
			err := c.mysql.CommitTrades(ctx, data)
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

func (c *coinbasePro) wsTickersToES(ctx context.Context) error {
	for {
		select {
		case data := <-c.wsEsTickers:
			err := c.es.CommitTickers(ctx, data)
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

func (c *coinbasePro) wsTradesToES(ctx context.Context) error {
	for {
		select {
		case data := <-c.wsEsTrades:
			err := c.es.CommitTrades(ctx, data)
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

func (c *coinbasePro) connectRest() error {
	rest, err := connector.GetREST()
	if err != nil {
		logErrStack(err)
		return err
	}
	c.rest = rest
	log.Info().Str("exchange", "coinbase-pro").Msg("REST connection setup is done")
	return nil
}

// processREST queries exchange for ticker / trade data through REST API in configured intervals,
// transforms it to a common ticker / trade store format,
// buffers the same in memory and
// then sends it to different storage systems for commit through go channels.
func (c *coinbasePro) processREST(ctx context.Context, mktID string, mktCommitName string, channel string, interval int) error {
	var (
		req *http.Request
		q   url.Values
		err error
	)

	cd := commitData{
		terTickers:   make([]storage.Ticker, 0, c.connCfg.Terminal.TickerCommitBuf),
		terTrades:    make([]storage.Trade, 0, c.connCfg.Terminal.TradeCommitBuf),
		mysqlTickers: make([]storage.Ticker, 0, c.connCfg.MySQL.TickerCommitBuf),
		mysqlTrades:  make([]storage.Trade, 0, c.connCfg.MySQL.TradeCommitBuf),
		esTickers:    make([]storage.Ticker, 0, c.connCfg.ES.TickerCommitBuf),
		esTrades:     make([]storage.Trade, 0, c.connCfg.ES.TradeCommitBuf),
	}

	switch channel {
	case "ticker":
		req, err = c.rest.Request(ctx, config.CoinbaseProRESTBaseURL+"products/"+mktID+"/ticker")
		if err != nil {
			if !errors.Is(err, ctx.Err()) {
				logErrStack(err)
			}
			return err
		}
	case "trade":
		req, err = c.rest.Request(ctx, config.CoinbaseProRESTBaseURL+"products/"+mktID+"/trades")
		if err != nil {
			if !errors.Is(err, ctx.Err()) {
				logErrStack(err)
			}
			return err
		}
		q = req.URL.Query()

		// Querying for 100 trades, which is a max allowed for a request by the exchange.
		// If the configured interval gap is big, then maybe it will not return all the trades
		// and if the gap is too small, maybe it will return duplicate ones.
		// Cursor pagination is not implemented.
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
				resp, err := c.rest.Do(req)
				if err != nil {
					if !errors.Is(err, ctx.Err()) {
						logErrStack(err)
					}
					return err
				}

				rr := respCoinPro{}
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
					Exchange:      "coinbase-pro",
					MktID:         mktID,
					MktCommitName: mktCommitName,
					Price:         price,
					Timestamp:     time.Now().UTC(),
				}

				key := cfgLookupKey{market: ticker.MktID, channel: "ticker"}
				val := c.cfgMap[key]
				if val.terStr {
					cd.terTickersCount++
					cd.terTickers = append(cd.terTickers, ticker)
					if cd.terTickersCount == c.connCfg.Terminal.TickerCommitBuf {
						c.ter.CommitTickers(cd.terTickers)
						cd.terTickersCount = 0
						cd.terTickers = nil
					}
				}
				if val.mysqlStr {
					cd.mysqlTickersCount++
					cd.mysqlTickers = append(cd.mysqlTickers, ticker)
					if cd.mysqlTickersCount == c.connCfg.MySQL.TickerCommitBuf {
						err := c.mysql.CommitTickers(ctx, cd.mysqlTickers)
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
					if cd.esTickersCount == c.connCfg.ES.TickerCommitBuf {
						err := c.es.CommitTickers(ctx, cd.esTickers)
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
				resp, err := c.rest.Do(req)
				if err != nil {
					if !errors.Is(err, ctx.Err()) {
						logErrStack(err)
					}
					return err
				}

				rr := []respCoinPro{}
				if err := jsoniter.NewDecoder(resp.Body).Decode(&rr); err != nil {
					logErrStack(err)
					resp.Body.Close()
					return err
				}
				resp.Body.Close()

				for i := range rr {
					r := rr[i]

					size, err := strconv.ParseFloat(r.Size, 64)
					if err != nil {
						logErrStack(err)
						return err
					}

					price, err := strconv.ParseFloat(r.Price, 64)
					if err != nil {
						logErrStack(err)
						return err
					}

					// Time sent is in string format.
					timestamp, err := time.Parse(time.RFC3339Nano, r.Time)
					if err != nil {
						logErrStack(err)
						return err
					}

					trade := storage.Trade{
						Exchange:      "coinbase-pro",
						MktID:         mktID,
						MktCommitName: mktCommitName,
						TradeID:       r.TradeID,
						Side:          r.Side,
						Size:          size,
						Price:         price,
						Timestamp:     timestamp,
					}

					key := cfgLookupKey{market: trade.MktID, channel: "trade"}
					val := c.cfgMap[key]
					if val.terStr {
						cd.terTradesCount++
						cd.terTrades = append(cd.terTrades, trade)
						if cd.terTradesCount == c.connCfg.Terminal.TradeCommitBuf {
							c.ter.CommitTrades(cd.terTrades)
							cd.terTradesCount = 0
							cd.terTrades = nil
						}
					}
					if val.mysqlStr {
						cd.mysqlTradesCount++
						cd.mysqlTrades = append(cd.mysqlTrades, trade)
						if cd.mysqlTradesCount == c.connCfg.MySQL.TradeCommitBuf {
							err := c.mysql.CommitTrades(ctx, cd.mysqlTrades)
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
						if cd.esTradesCount == c.connCfg.ES.TradeCommitBuf {
							err := c.es.CommitTrades(ctx, cd.esTrades)
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
