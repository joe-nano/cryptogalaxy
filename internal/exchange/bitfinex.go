package exchange

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
	"unicode"

	jsoniter "github.com/json-iterator/go"
	"github.com/milkywaybrain/cryptogalaxy/internal/config"
	"github.com/milkywaybrain/cryptogalaxy/internal/connector"
	"github.com/milkywaybrain/cryptogalaxy/internal/storage"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"
)

// StartBitfinex is for starting bitfinex exchange functions.
func StartBitfinex(appCtx context.Context, markets []config.Market, retry *config.Retry, connCfg *config.Connection) error {

	// If any error occurs or connection is lost, retry the exchange functions with a time gap till it reaches
	// a configured number of retry.
	// Retry counter will be reset back to zero if the elapsed time since the last retry is greater than the configured one.
	var retryCount int
	lastRetryTime := time.Now()

	for {
		err := newBitfinex(appCtx, markets, connCfg)
		if err != nil {
			log.Error().Err(err).Str("exchange", "bitfinex").Msg("error occurred")
			if retry.Number == 0 {
				return errors.New("not able to connect bitfinex exchange. please check the log for details")
			}
			if retry.ResetSec == 0 || time.Since(lastRetryTime).Seconds() < float64(retry.ResetSec) {
				retryCount++
			} else {
				retryCount = 1
			}
			lastRetryTime = time.Now()
			if retryCount > retry.Number {
				return fmt.Errorf("not able to connect bitfinex exchange even after %v retry. please check the log for details", retry.Number)
			}

			log.Error().Str("exchange", "bitfinex").Int("retry", retryCount).Msg(fmt.Sprintf("retrying functions in %v seconds", retry.GapSec))
			tick := time.NewTicker(time.Duration(retry.GapSec) * time.Second)
			select {
			case <-tick.C:
				tick.Stop()

			// Return, if there is any error from another exchange.
			case <-appCtx.Done():
				log.Error().Str("exchange", "bitfinex").Msg("ctx canceled, return from StartBitfinex")
				return appCtx.Err()
			}
		}
	}
}

type bitfinex struct {
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

type respBitfinex []interface{}

type wsRespInfoBitfinex struct {
	market        string
	channel       string
	respBitfinex  respBitfinex
	mktCommitName string
}

type wsEventRespBitfinex struct {
	Event     string `json:"event"`
	Channel   string `json:"channel"`
	ChannelID int    `json:"chanId"`
	Symbol    string `json:"symbol"`
	Code      int    `json:"code"`
	Msg       string `json:"msg"`
	Version   int    `json:"version"`
	Platform  struct {
		Status int `json:"status"`
	} `json:"platform"`
}

func newBitfinex(appCtx context.Context, markets []config.Market, connCfg *config.Connection) error {

	// If any exchange function fails, force all the other functions to stop and return.
	bitfinexErrGroup, ctx := errgroup.WithContext(appCtx)

	b := bitfinex{connCfg: connCfg}

	err := b.cfgLookup(markets)
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

					err = b.connectWs(ctx)
					if err != nil {
						return err
					}

					bitfinexErrGroup.Go(func() error {
						return b.closeWsConnOnError(ctx)
					})

					bitfinexErrGroup.Go(func() error {
						return b.readWs(ctx)
					})

					if b.ter != nil {
						bitfinexErrGroup.Go(func() error {
							return b.wsTickersToTerminal(ctx)
						})
						bitfinexErrGroup.Go(func() error {
							return b.wsTradesToTerminal(ctx)
						})
					}

					if b.mysql != nil {
						bitfinexErrGroup.Go(func() error {
							return b.wsTickersToMySQL(ctx)
						})
						bitfinexErrGroup.Go(func() error {
							return b.wsTradesToMySQL(ctx)
						})
					}

					if b.es != nil {
						bitfinexErrGroup.Go(func() error {
							return b.wsTickersToES(ctx)
						})
						bitfinexErrGroup.Go(func() error {
							return b.wsTradesToES(ctx)
						})
					}
				}

				err = b.subWsChannel(market.ID, info.Channel)
				if err != nil {
					return err
				}
				wsCount++
			case "rest":
				if restCount == 0 {
					err = b.connectRest()
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
				bitfinexErrGroup.Go(func() error {
					return b.processREST(ctx, mktID, mktCommitName, channel, restPingIntSec)
				})

				restCount++
			}
		}
	}

	err = bitfinexErrGroup.Wait()
	if err != nil {
		return err
	}
	return nil
}

func (b *bitfinex) cfgLookup(markets []config.Market) error {

	// Configurations flat map is prepared for easy lookup later in the app.
	b.cfgMap = make(map[cfgLookupKey]cfgLookupVal)
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
					if b.ter == nil {
						b.ter = storage.GetTerminal()
						b.wsTerTickers = make(chan []storage.Ticker, 1)
						b.wsTerTrades = make(chan []storage.Trade, 1)
					}
				case "mysql":
					val.mysqlStr = true
					if b.mysql == nil {
						b.mysql = storage.GetMySQL()
						b.wsMysqlTickers = make(chan []storage.Ticker, 1)
						b.wsMysqlTrades = make(chan []storage.Trade, 1)
					}
				case "elastic_search":
					val.esStr = true
					if b.es == nil {
						b.es = storage.GetElasticSearch()
						b.wsEsTickers = make(chan []storage.Ticker, 1)
						b.wsEsTrades = make(chan []storage.Trade, 1)
					}
				}
			}
			val.mktCommitName = mktCommitName
			b.cfgMap[key] = val
		}
	}
	return nil
}

func (b *bitfinex) connectWs(ctx context.Context) error {
	ws, err := connector.NewWebsocket(ctx, &b.connCfg.WS, config.BitfinexWebsocketURL)
	if err != nil {
		if !errors.Is(err, ctx.Err()) {
			logErrStack(err)
		}
		return err
	}
	b.ws = ws
	log.Info().Str("exchange", "bitfinex").Msg("websocket connected")
	return nil
}

// closeWsConnOnError closes websocket connection if there is any error in app context.
// This will unblock all read and writes on websocket.
func (b *bitfinex) closeWsConnOnError(ctx context.Context) error {
	<-ctx.Done()
	err := b.ws.Conn.Close()
	if err != nil {
		return err
	}
	return ctx.Err()
}

// subWsChannel sends channel subscription requests to the websocket server.
func (b *bitfinex) subWsChannel(market string, channel string) error {
	if channel == "trade" {
		channel = "trades"
	}
	market = "t" + strings.ToUpper(market)
	frame, err := jsoniter.Marshal(map[string]string{
		"event":   "subscribe",
		"channel": channel,
		"symbol":  market,
	})
	if err != nil {
		logErrStack(err)
		return err
	}
	err = b.ws.Write(frame)
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
func (b *bitfinex) readWs(ctx context.Context) error {
	channelMap := make(map[int]map[string]string)

	// To avoid data race, creating a new local lookup map.
	cfgLookup := make(map[cfgLookupKey]cfgLookupVal, len(b.cfgMap))
	for k, v := range b.cfgMap {
		cfgLookup[k] = v
	}

	cd := commitData{
		terTickers:   make([]storage.Ticker, 0, b.connCfg.Terminal.TickerCommitBuf),
		terTrades:    make([]storage.Trade, 0, b.connCfg.Terminal.TradeCommitBuf),
		mysqlTickers: make([]storage.Ticker, 0, b.connCfg.MySQL.TickerCommitBuf),
		mysqlTrades:  make([]storage.Trade, 0, b.connCfg.MySQL.TradeCommitBuf),
		esTickers:    make([]storage.Ticker, 0, b.connCfg.ES.TickerCommitBuf),
		esTrades:     make([]storage.Trade, 0, b.connCfg.ES.TradeCommitBuf),
	}

	for {
		select {
		default:
			frame, err := b.ws.Read()
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

			// Need to differentiate event and data responses.
			temp := bytes.TrimLeftFunc(frame, unicode.IsSpace)
			if bytes.HasPrefix(temp, []byte("{")) {
				wr := wsEventRespBitfinex{}
				err = jsoniter.Unmarshal(frame, &wr)
				if err != nil {
					logErrStack(err)
					return err
				}

				// Keep a map of id to subscribed market channel as subsequent data frames only contain
				// channel id, actual data and not the market info.
				switch wr.Event {
				case "hb":
				case "subscribed":
					channelInfo := make(map[string]string, 2)
					channelInfo["market"] = wr.Symbol[1:]
					if wr.Channel == "trades" {
						wr.Channel = "trade"
					}
					channelInfo["channel"] = wr.Channel
					channelMap[wr.ChannelID] = channelInfo
					log.Debug().Str("exchange", "bitfinex").Str("func", "readWs").Str("market", channelInfo["market"]).Str("channel", wr.Channel).Msg("channel subscribed")
				case "error":
					log.Error().Str("exchange", "bitfinex").Str("func", "readWs").Int("code", wr.Code).Str("msg", wr.Msg).Msg("")
					return errors.New("bitfinex websocket error")
				case "info":
					if wr.Code != 0 {
						log.Info().Str("exchange", "bitfinex").Str("func", "readWs").Int("code", wr.Code).Str("msg", wr.Msg).Msg("info received")
					} else if wr.Version != 0 {
						log.Info().Str("exchange", "bitfinex").Str("func", "readWs").Int("version", wr.Version).Int("platform-status", wr.Platform.Status).Msg("info received")
					}
				}
			} else if bytes.HasPrefix(temp, []byte("[")) {
				wr := respBitfinex{}
				err = jsoniter.Unmarshal(frame, &wr)
				if err != nil {
					logErrStack(err)
					return err
				}

				if chanID, ok := wr[0].(float64); ok {
					wri := wsRespInfoBitfinex{
						market:  channelMap[int(chanID)]["market"],
						channel: channelMap[int(chanID)]["channel"],
					}

					// Ignore trade snapshot and trade update.
					// Consider ticker and trade execute.
					switch data := wr[1].(type) {
					case string:
						if data != "te" {
							continue
						}
						if wsData, ok := wr[2].([]interface{}); ok {
							wri.respBitfinex = wsData
						} else {
							log.Error().Str("exchange", "bitfinex").Str("func", "readWs").Interface("data", wr[2]).Msg("")
							return errors.New("cannot convert frame data to []interface{}")
						}
					case []interface{}:
						if wri.channel != "ticker" {
							continue
						}
						wri.respBitfinex = data
					}

					// Consider frame only in configured interval, otherwise ignore it.
					key := cfgLookupKey{market: wri.market, channel: wri.channel}
					val := cfgLookup[key]
					if val.wsConsiderIntSec == 0 || time.Since(val.wsLastUpdated).Seconds() >= float64(val.wsConsiderIntSec) {
						val.wsLastUpdated = time.Now()
						wri.mktCommitName = val.mktCommitName
						cfgLookup[key] = val
					} else {
						continue
					}

					err := b.processWs(ctx, &wri, &cd)
					if err != nil {
						return err
					}
				} else {
					log.Error().Str("exchange", "bitfinex").Str("func", "readWs").Interface("channel id", wr[0]).Msg("")
					return errors.New("cannot convert frame data field channel id to float")
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
func (b *bitfinex) processWs(ctx context.Context, wr *wsRespInfoBitfinex, cd *commitData) error {
	switch wr.channel {
	case "ticker":
		ticker := storage.Ticker{}
		ticker.Exchange = "bitfinex"
		ticker.MktID = wr.market
		ticker.MktCommitName = wr.mktCommitName

		// Price sent is an array value, needed to access it by it's position.
		// (Sent array has different data type values so the interface is used.)
		if price, ok := wr.respBitfinex[6].(float64); ok {
			ticker.Price = price
		} else {
			log.Error().Str("exchange", "bitfinex").Str("func", "processWs").Interface("price", wr.respBitfinex[6]).Msg("")
			return errors.New("cannot convert ticker data field price to float")
		}

		ticker.Timestamp = time.Now().UTC()

		key := cfgLookupKey{market: ticker.MktID, channel: "ticker"}
		val := b.cfgMap[key]
		if val.terStr {
			cd.terTickersCount++
			cd.terTickers = append(cd.terTickers, ticker)
			if cd.terTickersCount == b.connCfg.Terminal.TickerCommitBuf {
				select {
				case b.wsTerTickers <- cd.terTickers:
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
			if cd.mysqlTickersCount == b.connCfg.MySQL.TickerCommitBuf {
				select {
				case b.wsMysqlTickers <- cd.mysqlTickers:
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
			if cd.esTickersCount == b.connCfg.ES.TickerCommitBuf {
				select {
				case b.wsEsTickers <- cd.esTickers:
				case <-ctx.Done():
					return ctx.Err()
				}
				cd.esTickersCount = 0
				cd.esTickers = nil
			}
		}
	case "trade":
		trade := storage.Trade{}
		trade.Exchange = "bitfinex"
		trade.MktID = wr.market
		trade.MktCommitName = wr.mktCommitName

		// All the values sent are an array value, needed to access it by it's position.
		// (Sent array has different data type values so the interface is used.)

		if tradeID, ok := wr.respBitfinex[0].(float64); ok {
			trade.TradeID = uint64(tradeID)
		} else {
			log.Error().Str("exchange", "bitfinex").Str("func", "processWs").Interface("trade id", wr.respBitfinex[0]).Msg("")
			return errors.New("cannot convert trade data field channel id to float")
		}

		if size, ok := wr.respBitfinex[2].(float64); ok {
			if size > 0 {
				trade.Side = "buy"
			} else {
				trade.Side = "sell"
			}
			trade.Size = math.Abs(size)
		} else {
			log.Error().Str("exchange", "bitfinex").Str("func", "processWs").Interface("size", wr.respBitfinex[2]).Msg("")
			return errors.New("cannot convert trade data field size to float")
		}

		if price, ok := wr.respBitfinex[3].(float64); ok {
			trade.Price = price
		} else {
			log.Error().Str("exchange", "bitfinex").Str("func", "processWs").Interface("price", wr.respBitfinex[3]).Msg("")
			return errors.New("cannot convert trade data field price to float")
		}

		if timestamp, ok := wr.respBitfinex[1].(float64); ok {
			trade.Timestamp = time.Unix(0, int64(timestamp)*int64(time.Millisecond)).UTC()
		} else {
			log.Error().Str("exchange", "bitfinex").Str("func", "processWs").Interface("timestamp", wr.respBitfinex[1]).Msg("")
			return errors.New("cannot convert trade data field timestamp to float")
		}

		key := cfgLookupKey{market: trade.MktID, channel: "trade"}
		val := b.cfgMap[key]
		if val.terStr {
			cd.terTradesCount++
			cd.terTrades = append(cd.terTrades, trade)
			if cd.terTradesCount == b.connCfg.Terminal.TradeCommitBuf {
				select {
				case b.wsTerTrades <- cd.terTrades:
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
			if cd.mysqlTradesCount == b.connCfg.MySQL.TradeCommitBuf {
				select {
				case b.wsMysqlTrades <- cd.mysqlTrades:
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
			if cd.esTradesCount == b.connCfg.ES.TradeCommitBuf {
				select {
				case b.wsEsTrades <- cd.esTrades:
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

func (b *bitfinex) wsTickersToTerminal(ctx context.Context) error {
	for {
		select {
		case data := <-b.wsTerTickers:
			b.ter.CommitTickers(data)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (b *bitfinex) wsTradesToTerminal(ctx context.Context) error {
	for {
		select {
		case data := <-b.wsTerTrades:
			b.ter.CommitTrades(data)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (b *bitfinex) wsTickersToMySQL(ctx context.Context) error {
	for {
		select {
		case data := <-b.wsMysqlTickers:
			err := b.mysql.CommitTickers(ctx, data)
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

func (b *bitfinex) wsTradesToMySQL(ctx context.Context) error {
	for {
		select {
		case data := <-b.wsMysqlTrades:
			err := b.mysql.CommitTrades(ctx, data)
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

func (b *bitfinex) wsTickersToES(ctx context.Context) error {
	for {
		select {
		case data := <-b.wsEsTickers:
			err := b.es.CommitTickers(ctx, data)
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

func (b *bitfinex) wsTradesToES(ctx context.Context) error {
	for {
		select {
		case data := <-b.wsEsTrades:
			err := b.es.CommitTrades(ctx, data)
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

func (b *bitfinex) connectRest() error {
	rest, err := connector.GetREST()
	if err != nil {
		logErrStack(err)
		return err
	}
	b.rest = rest
	log.Info().Str("exchange", "bitfinex").Msg("REST connection setup is done")
	return nil
}

// processREST queries exchange for ticker / trade data through REST API in configured intervals,
// transforms it to a common ticker / trade store format,
// buffers the same in memory and
// then sends it to different storage systems for commit through go channels.
func (b *bitfinex) processREST(ctx context.Context, mktID string, mktCommitName string, channel string, interval int) error {
	var (
		req  *http.Request
		q    url.Values
		err  error
		side string
	)

	cd := commitData{
		terTickers:   make([]storage.Ticker, 0, b.connCfg.Terminal.TickerCommitBuf),
		terTrades:    make([]storage.Trade, 0, b.connCfg.Terminal.TradeCommitBuf),
		mysqlTickers: make([]storage.Ticker, 0, b.connCfg.MySQL.TickerCommitBuf),
		mysqlTrades:  make([]storage.Trade, 0, b.connCfg.MySQL.TradeCommitBuf),
		esTickers:    make([]storage.Ticker, 0, b.connCfg.ES.TickerCommitBuf),
		esTrades:     make([]storage.Trade, 0, b.connCfg.ES.TradeCommitBuf),
	}

	switch channel {
	case "ticker":
		req, err = b.rest.Request(ctx, config.BitfinexRESTBaseURL+"ticker/t"+mktID)
		if err != nil {
			if !errors.Is(err, ctx.Err()) {
				logErrStack(err)
			}
			return err
		}
	case "trade":
		req, err = b.rest.Request(ctx, config.BitfinexRESTBaseURL+"trades/t"+mktID+"/hist")
		if err != nil {
			if !errors.Is(err, ctx.Err()) {
				logErrStack(err)
			}
			return err
		}
		q = req.URL.Query()

		// Querying for 100 trades, which is a max allowed for a request by the exchange.
		// If the configured interval gap is big, then maybe it will not return all the trades.
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
				resp, err := b.rest.Do(req)
				if err != nil {
					if !errors.Is(err, ctx.Err()) {
						logErrStack(err)
					}
					return err
				}

				rr := respBitfinex{}
				if err := jsoniter.NewDecoder(resp.Body).Decode(&rr); err != nil {
					logErrStack(err)
					resp.Body.Close()
					return err
				}
				resp.Body.Close()

				// Price sent is an array value, needed to access it by it's position.
				// (Sent array has different data type values so the interface is used.)
				price, ok := rr[6].(float64)
				if !ok {
					log.Error().Str("exchange", "bitfinex").Str("func", "processREST").Interface("price", rr[6]).Msg("")
					return errors.New("cannot convert ticker data field price to float")
				}

				ticker := storage.Ticker{
					Exchange:      "bitfinex",
					MktID:         mktID,
					MktCommitName: mktCommitName,
					Price:         price,
					Timestamp:     time.Now().UTC(),
				}

				key := cfgLookupKey{market: ticker.MktID, channel: "ticker"}
				val := b.cfgMap[key]
				if val.terStr {
					cd.terTickersCount++
					cd.terTickers = append(cd.terTickers, ticker)
					if cd.terTickersCount == b.connCfg.Terminal.TickerCommitBuf {
						b.ter.CommitTickers(cd.terTickers)
						cd.terTickersCount = 0
						cd.terTickers = nil
					}
				}
				if val.mysqlStr {
					cd.mysqlTickersCount++
					cd.mysqlTickers = append(cd.mysqlTickers, ticker)
					if cd.mysqlTickersCount == b.connCfg.MySQL.TickerCommitBuf {
						err := b.mysql.CommitTickers(ctx, cd.mysqlTickers)
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
					if cd.esTickersCount == b.connCfg.ES.TickerCommitBuf {
						err := b.es.CommitTickers(ctx, cd.esTickers)
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
				q.Del("start")
				req.URL.RawQuery = q.Encode()
				resp, err := b.rest.Do(req)
				if err != nil {
					if !errors.Is(err, ctx.Err()) {
						logErrStack(err)
					}
					return err
				}

				rr := []respBitfinex{}
				if err := jsoniter.NewDecoder(resp.Body).Decode(&rr); err != nil {
					logErrStack(err)
					resp.Body.Close()
					return err
				}
				resp.Body.Close()

				// All the values sent are an array value, needed to access it by it's position.
				// (Sent array has different data type values so the interface is used.)
				for i := range rr {
					r := rr[i]
					tradeID, ok := r[0].(float64)
					if !ok {
						log.Error().Str("exchange", "bitfinex").Str("func", "processREST").Interface("trade id", r[0]).Msg("")
						return errors.New("cannot convert trade data field trade id to float")
					}

					size, ok := r[2].(float64)
					if !ok {
						log.Error().Str("exchange", "bitfinex").Str("func", "processREST").Interface("size", r[2]).Msg("")
						return errors.New("cannot convert trade data field size to float")
					}
					if size > 0 {
						side = "buy"
					} else {
						side = "sell"
					}
					size = math.Abs(size)

					price, ok := r[3].(float64)
					if !ok {
						log.Error().Str("exchange", "bitfinex").Str("func", "processREST").Interface("price", r[3]).Msg("")
						return errors.New("cannot convert trade data field price to float")
					}

					timestamp, ok := r[1].(float64)
					if !ok {
						log.Error().Str("exchange", "bitfinex").Str("func", "processREST").Interface("timestamp", r[1]).Msg("")
						return errors.New("cannot convert trade data field timestamp to float")
					}

					trade := storage.Trade{
						Exchange:      "bitfinex",
						MktID:         mktID,
						MktCommitName: mktCommitName,
						TradeID:       uint64(tradeID),
						Side:          side,
						Size:          size,
						Price:         price,
						Timestamp:     time.Unix(0, int64(timestamp)*int64(time.Millisecond)).UTC(),
					}

					key := cfgLookupKey{market: trade.MktID, channel: "trade"}
					val := b.cfgMap[key]
					if val.terStr {
						cd.terTradesCount++
						cd.terTrades = append(cd.terTrades, trade)
						if cd.terTradesCount == b.connCfg.Terminal.TradeCommitBuf {
							b.ter.CommitTrades(cd.terTrades)
							cd.terTradesCount = 0
							cd.terTrades = nil
						}
					}
					if val.mysqlStr {
						cd.mysqlTradesCount++
						cd.mysqlTrades = append(cd.mysqlTrades, trade)
						if cd.mysqlTradesCount == b.connCfg.MySQL.TradeCommitBuf {
							err := b.mysql.CommitTrades(ctx, cd.mysqlTrades)
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
						if cd.esTradesCount == b.connCfg.ES.TradeCommitBuf {
							err := b.es.CommitTrades(ctx, cd.esTrades)
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
