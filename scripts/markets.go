package main

import (
	"encoding/csv"
	"fmt"
	"net/http"
	"os"

	jsoniter "github.com/json-iterator/go"
	"github.com/milkywaybrain/cryptogalaxy/internal/config"
	"github.com/rs/zerolog/log"
)

// This function will query all the exchanges for market info and store it in a csv file.
// Users can look up to this csv file to give market ID in the app configuration.
// CSV file created at ./examples/markets.csv.
func main() {
	f, err := os.Create("./examples/markets.csv")
	if err != nil {
		log.Error().Err(err).Str("exchange", "ftx").Msg("csv file create")
		return
	}
	w := csv.NewWriter(f)
	defer w.Flush()
	defer f.Close()

	// FTX exchange.
	resp, err := http.Get(config.FtxRESTBaseURL + "markets")
	if err != nil {
		log.Error().Err(err).Str("exchange", "ftx").Msg("exchange request for markets")
		return
	}
	ftxMarkets := ftxResp{}
	if err = jsoniter.NewDecoder(resp.Body).Decode(&ftxMarkets); err != nil {
		log.Error().Err(err).Str("exchange", "ftx").Msg("convert markets response")
		return
	}
	resp.Body.Close()
	for _, record := range ftxMarkets.Result {
		if record.Type != "spot" {
			continue
		}
		if err = w.Write([]string{"ftx", record.Name}); err != nil {
			log.Error().Err(err).Str("exchange", "ftx").Msg("writing markets to csv")
			return
		}
	}

	// Coinbase-Pro exchange.
	resp, err = http.Get(config.CoinbaseProRESTBaseURL + "products")
	if err != nil {
		log.Error().Err(err).Str("exchange", "coinbase-pro").Msg("exchange request for markets")
		return
	}
	coinbaseProMarkets := []coinbaseProResp{}
	if err = jsoniter.NewDecoder(resp.Body).Decode(&coinbaseProMarkets); err != nil {
		log.Error().Err(err).Str("exchange", "coinbase-pro").Msg("convert markets response")
		return
	}
	resp.Body.Close()
	for _, record := range coinbaseProMarkets {
		if err = w.Write([]string{"coinbase-pro", record.Name}); err != nil {
			log.Error().Err(err).Str("exchange", "coinbase-pro").Msg("writing markets to csv")
			return
		}
	}

	// Binance exchange.
	resp, err = http.Get(config.BinanceRESTBaseURL + "exchangeInfo")
	if err != nil {
		log.Error().Err(err).Str("exchange", "binance").Msg("exchange request for markets")
		return
	}
	binanceMarkets := binanceResp{}
	if err = jsoniter.NewDecoder(resp.Body).Decode(&binanceMarkets); err != nil {
		log.Error().Err(err).Str("exchange", "binance").Msg("convert markets response")
		return
	}
	resp.Body.Close()
	for _, record := range binanceMarkets.Result {
		if err = w.Write([]string{"binance", record.Name}); err != nil {
			log.Error().Err(err).Str("exchange", "binance").Msg("writing markets to csv")
			return
		}
	}

	// Bitfinex exchange.
	resp, err = http.Get(config.BitfinexRESTBaseURL + "conf/pub:list:pair:exchange")
	if err != nil {
		log.Error().Err(err).Str("exchange", "bitfinex").Msg("exchange request for markets")
		return
	}
	bitfinexMarkets := bitfinexResp{}
	if err = jsoniter.NewDecoder(resp.Body).Decode(&bitfinexMarkets); err != nil {
		log.Error().Err(err).Str("exchange", "bitfinex").Msg("convert markets response")
		return
	}
	resp.Body.Close()
	for _, record := range bitfinexMarkets[0] {
		if err = w.Write([]string{"bitfinex", record}); err != nil {
			log.Error().Err(err).Str("exchange", "bitfinex").Msg("writing markets to csv")
			return
		}
	}

	// Hbtc exchange.
	resp, err = http.Get(config.HbtcRESTBaseURL + "openapi/v1/pairs")
	if err != nil {
		log.Error().Err(err).Str("exchange", "hbtc").Msg("exchange request for markets")
		return
	}
	hbtcMarkets := []hbtcRespRes{}
	if err = jsoniter.NewDecoder(resp.Body).Decode(&hbtcMarkets); err != nil {
		log.Error().Err(err).Str("exchange", "hbtc").Msg("convert markets response")
		return
	}
	resp.Body.Close()
	for _, record := range hbtcMarkets {
		if err := w.Write([]string{"hbtc", record.Name}); err != nil {
			log.Error().Err(err).Str("exchange", "hbtc").Msg("writing markets to csv")
			return
		}
	}

	fmt.Println("CSV file generated successfully at ./examples/markets.csv")
}

type ftxResp struct {
	Result []ftxRespRes `json:"result"`
}
type ftxRespRes struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

type binanceResp struct {
	Result []binanceRespRes `json:"symbols"`
}
type binanceRespRes struct {
	Name string `json:"symbol"`
}

type coinbaseProResp struct {
	Name string `json:"id"`
}

type bitfinexResp [][]string

type hbtcRespRes struct {
	Name string `json:"symbol"`
}
