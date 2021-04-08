package test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	jsoniter "github.com/json-iterator/go"
	"github.com/milkywaybrain/cryptogalaxy/internal/config"
	"github.com/milkywaybrain/cryptogalaxy/internal/initializer"
	"github.com/milkywaybrain/cryptogalaxy/internal/storage"
	"golang.org/x/sync/errgroup"
)

// TestCryptogalaxy is a combination of unit and integration test for the app.
func TestCryptogalaxy(t *testing.T) {

	// Load config file values.
	cfgPath := "./config_test.json"
	cfgFile, err := os.Open(cfgPath)
	if err != nil {
		t.Log("ERROR : Not able to find config file :", cfgPath)
		t.FailNow()
	}
	var cfg config.Config
	if err = jsoniter.NewDecoder(cfgFile).Decode(&cfg); err != nil {
		t.Log("ERROR : Not able to parse JSON from config file :", cfgPath)
		t.FailNow()
	}
	cfgFile.Close()

	// For testing, mysql schema name should start with the name test to avoid mistakenly messing up with production one.
	if !strings.HasPrefix(cfg.Connection.MySQL.Schema, "test") {
		t.Log("ERROR : mysql schema name should start with test for testing")
		t.FailNow()
	}

	// For testing, elastic search index name should start with the name test to avoid mistakenly messing up with
	// production one.
	if !strings.HasPrefix(cfg.Connection.ES.IndexName, "test") {
		t.Log("ERROR : elastic search index name should start with test for testing")
		t.FailNow()
	}

	// Terminal output we can't actually test, so make file as terminal output.
	outFile, err := os.Create("./data_test/ter_storage_test.txt")
	if err != nil {
		t.Log("ERROR : not able to create test terminal storage file : ./data_test/ter_storage_test.txt")
		t.FailNow()
	}
	_ = storage.InitTerminal(outFile)

	// Delete all data from mysql to have fresh one.
	mysql, err := storage.InitMySQL(&cfg.Connection.MySQL)
	if err != nil {
		t.Log("ERROR : " + err.Error())
		t.FailNow()
	}

	var mysqlCtx context.Context
	if cfg.Connection.MySQL.ReqTimeoutSec > 0 {
		timeoutCtx, cancel := context.WithTimeout(context.Background(), time.Duration(cfg.Connection.MySQL.ReqTimeoutSec)*time.Second)
		mysqlCtx = timeoutCtx
		defer cancel()
	} else {
		mysqlCtx = context.Background()
	}
	_, err = mysql.DB.ExecContext(mysqlCtx, "truncate ticker")
	if err != nil {
		t.Log("ERROR : " + err.Error())
		t.FailNow()
	}

	if cfg.Connection.MySQL.ReqTimeoutSec > 0 {
		timeoutCtx, cancel := context.WithTimeout(context.Background(), time.Duration(cfg.Connection.MySQL.ReqTimeoutSec)*time.Second)
		mysqlCtx = timeoutCtx
		defer cancel()
	} else {
		mysqlCtx = context.Background()
	}
	_, err = mysql.DB.ExecContext(mysqlCtx, "truncate trade")
	if err != nil {
		t.Log("ERROR : " + err.Error())
		t.FailNow()
	}

	// Delete all data from elastic search to have fresh one.
	es, err := storage.InitElasticSearch(&cfg.Connection.ES)
	if err != nil {
		t.Log("ERROR : " + err.Error())
		t.FailNow()
	}

	indexNames := make([]string, 0, 1)
	indexNames = append(indexNames, es.IndexName)
	var buf bytes.Buffer
	deleteQuery := []byte(`{"query":{"match_all":{}}}`)
	buf.Grow(len(deleteQuery))
	buf.Write(deleteQuery)

	var esCtx context.Context
	if cfg.Connection.ES.ReqTimeoutSec > 0 {
		timeoutCtx, cancel := context.WithTimeout(context.Background(), time.Duration(cfg.Connection.ES.ReqTimeoutSec)*time.Second)
		esCtx = timeoutCtx
		defer cancel()
	} else {
		esCtx = context.Background()
	}
	resp, err := es.ES.DeleteByQuery(indexNames, bytes.NewReader(buf.Bytes()), es.ES.DeleteByQuery.WithContext(esCtx))
	if err != nil {
		t.Log("ERROR : " + err.Error())
		t.FailNow()
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Log("ERROR : " + fmt.Errorf("code : %v, status : %v", resp.StatusCode, resp.Status()).Error())
		t.FailNow()
	}
	_, err = io.Copy(io.Discard, resp.Body)
	if err != nil {
		t.Log("ERROR : " + err.Error())
		t.FailNow()
	}

	// Execute the app for 2 minute, which is good enough time to get the data from exchanges.
	// After that cancel app execution through context error.
	// If there is any actual error from app execution, then stop testing.
	testErrGroup, testCtx := errgroup.WithContext(context.Background())

	testErrGroup.Go(func() error {
		return initializer.Start(testCtx, &cfg)
	})

	t.Log("INFO : Executing app for 2 minute to get the data from exchanges.")
	testErrGroup.Go(func() error {
		tick := time.NewTicker(2 * time.Minute)
		defer tick.Stop()
		select {
		case <-tick.C:
			return errors.New("canceling app execution and starting data test")
		case <-testCtx.Done():
			return testCtx.Err()
		}
	})

	err = testErrGroup.Wait()
	if err != nil {
		if err.Error() == "canceling app execution and starting data test" {
			t.Log("INFO : " + err.Error())
		} else {
			t.Log("ERROR : " + err.Error())
			t.FailNow()
		}
	}

	// Close file which has been set as the terminal output in the previous step.
	err = outFile.Close()
	if err != nil {
		t.Log("ERROR : " + err.Error())
		t.FailNow()
	}

	// Read data from different storage which has been set in the app execution stage.
	// Then verify it.

	// FTX exchange.
	var ftxFail bool

	terTickers := make(map[string]storage.Ticker)
	terTrades := make(map[string]storage.Trade)
	mysqlTickers := make(map[string]storage.Ticker)
	mysqlTrades := make(map[string]storage.Trade)
	esTickers := make(map[string]storage.Ticker)
	esTrades := make(map[string]storage.Trade)

	err = readTerminal("ftx", terTickers, terTrades)
	if err != nil {
		t.Log("ERROR : " + err.Error())
		t.Error("FAILURE : ftx exchange function")
		ftxFail = true
	}

	if !ftxFail {
		err = readMySQL("ftx", mysqlTickers, mysqlTrades, mysql)
		if err != nil {
			t.Log("ERROR : " + err.Error())
			t.Error("FAILURE : ftx exchange function")
			ftxFail = true
		}
	}

	if !ftxFail {
		err = readElasticSearch("ftx", esTickers, esTrades, es)
		if err != nil {
			t.Log("ERROR : " + err.Error())
			t.Error("FAILURE : ftx exchange function")
			ftxFail = true
		}
	}

	if !ftxFail {
		err = verifyData("ftx", terTickers, terTrades, mysqlTickers, mysqlTrades, esTickers, esTrades, &cfg)
		if err != nil {
			t.Log("ERROR : " + err.Error())
			t.Error("FAILURE : ftx exchange function")
			ftxFail = true
		} else {
			t.Log("SUCCESS : ftx exchange function")
		}
	}

	// Coinbase-Pro exchange.
	var coinbaseProFail bool

	terTickers = make(map[string]storage.Ticker)
	terTrades = make(map[string]storage.Trade)
	mysqlTickers = make(map[string]storage.Ticker)
	mysqlTrades = make(map[string]storage.Trade)
	esTickers = make(map[string]storage.Ticker)
	esTrades = make(map[string]storage.Trade)

	err = readTerminal("coinbase-pro", terTickers, terTrades)
	if err != nil {
		t.Log("ERROR : " + err.Error())
		t.Error("FAILURE : coinbase-pro exchange function")
		coinbaseProFail = true
	}

	if !coinbaseProFail {
		err = readMySQL("coinbase-pro", mysqlTickers, mysqlTrades, mysql)
		if err != nil {
			t.Log("ERROR : " + err.Error())
			t.Error("FAILURE : coinbase-pro exchange function")
			coinbaseProFail = true
		}
	}

	if !coinbaseProFail {
		err = readElasticSearch("coinbase-pro", esTickers, esTrades, es)
		if err != nil {
			t.Log("ERROR : " + err.Error())
			t.Error("FAILURE : coinbase-pro exchange function")
			coinbaseProFail = true
		}
	}

	if !coinbaseProFail {
		err = verifyData("ftx", terTickers, terTrades, mysqlTickers, mysqlTrades, esTickers, esTrades, &cfg)
		if err != nil {
			t.Log("ERROR : " + err.Error())
			t.Error("FAILURE : coinbase-pro exchange function")
			coinbaseProFail = true
		} else {
			t.Log("SUCCESS : coinbase-pro exchange function")
		}
	}

	// Binance exchange.
	var binanceFail bool

	terTickers = make(map[string]storage.Ticker)
	terTrades = make(map[string]storage.Trade)
	mysqlTickers = make(map[string]storage.Ticker)
	mysqlTrades = make(map[string]storage.Trade)
	esTickers = make(map[string]storage.Ticker)
	esTrades = make(map[string]storage.Trade)

	err = readTerminal("binance", terTickers, terTrades)
	if err != nil {
		t.Log("ERROR : " + err.Error())
		t.Error("FAILURE : binance exchange function")
		binanceFail = true
	}

	if !binanceFail {
		err = readMySQL("binance", mysqlTickers, mysqlTrades, mysql)
		if err != nil {
			t.Log("ERROR : " + err.Error())
			t.Error("FAILURE : binance exchange function")
			binanceFail = true
		}
	}

	if !binanceFail {
		err = readElasticSearch("binance", esTickers, esTrades, es)
		if err != nil {
			t.Log("ERROR : " + err.Error())
			t.Error("FAILURE : binance exchange function")
			binanceFail = true
		}
	}

	if !binanceFail {
		err = verifyData("binance", terTickers, terTrades, mysqlTickers, mysqlTrades, esTickers, esTrades, &cfg)
		if err != nil {
			t.Log("ERROR : " + err.Error())
			t.Error("FAILURE : binance exchange function")
			binanceFail = true
		} else {
			t.Log("SUCCESS : binance exchange function")
		}
	}

	// Bitfinex exchange.
	var bitfinexFail bool

	terTickers = make(map[string]storage.Ticker)
	terTrades = make(map[string]storage.Trade)
	mysqlTickers = make(map[string]storage.Ticker)
	mysqlTrades = make(map[string]storage.Trade)
	esTickers = make(map[string]storage.Ticker)
	esTrades = make(map[string]storage.Trade)

	err = readTerminal("bitfinex", terTickers, terTrades)
	if err != nil {
		t.Log("ERROR : " + err.Error())
		t.Error("FAILURE : bitfinex exchange function")
		bitfinexFail = true
	}

	if !bitfinexFail {
		err = readMySQL("bitfinex", mysqlTickers, mysqlTrades, mysql)
		if err != nil {
			t.Log("ERROR : " + err.Error())
			t.Error("FAILURE : bitfinex exchange function")
			bitfinexFail = true
		}
	}

	if !bitfinexFail {
		err = readElasticSearch("bitfinex", esTickers, esTrades, es)
		if err != nil {
			t.Log("ERROR : " + err.Error())
			t.Error("FAILURE : bitfinex exchange function")
			bitfinexFail = true
		}
	}

	if !bitfinexFail {
		err = verifyData("bitfinex", terTickers, terTrades, mysqlTickers, mysqlTrades, esTickers, esTrades, &cfg)
		if err != nil {
			t.Log("ERROR : " + err.Error())
			t.Error("FAILURE : bitfinex exchange function")
			bitfinexFail = true
		} else {
			t.Log("SUCCESS : bitfinex exchange function")
		}
	}

	// Hbtc exchange.
	var hbtcFail bool

	terTickers = make(map[string]storage.Ticker)
	terTrades = make(map[string]storage.Trade)
	mysqlTickers = make(map[string]storage.Ticker)
	mysqlTrades = make(map[string]storage.Trade)
	esTickers = make(map[string]storage.Ticker)
	esTrades = make(map[string]storage.Trade)

	err = readTerminal("hbtc", terTickers, terTrades)
	if err != nil {
		t.Log("ERROR : " + err.Error())
		t.Error("FAILURE : hbtc exchange function")
		hbtcFail = true
	}

	if !hbtcFail {
		err = readMySQL("hbtc", mysqlTickers, mysqlTrades, mysql)
		if err != nil {
			t.Log("ERROR : " + err.Error())
			t.Error("FAILURE : hbtc exchange function")
			hbtcFail = true
		}
	}

	if !hbtcFail {
		err = readElasticSearch("hbtc", esTickers, esTrades, es)
		if err != nil {
			t.Log("ERROR : " + err.Error())
			t.Error("FAILURE : hbtc exchange function")
			hbtcFail = true
		}
	}

	if !hbtcFail {
		err = verifyData("hbtc", terTickers, terTrades, mysqlTickers, mysqlTrades, esTickers, esTrades, &cfg)
		if err != nil {
			t.Log("ERROR : " + err.Error())
			t.Error("FAILURE : hbtc exchange function")
			hbtcFail = true
		} else {
			t.Log("SUCCESS : hbtc exchange function")
		}
	}

	if ftxFail || coinbaseProFail || binanceFail || bitfinexFail || hbtcFail {
		t.Log("INFO : May be 2 minute app execution time is not good enough to get the data. Try to increse it before actual debugging.")
	}
}

// readTerminal reads ticker and trade data for an exchange from a file, which has been set as terminal output,
// into passed in maps.
func readTerminal(exchName string, terTickers map[string]storage.Ticker, terTrades map[string]storage.Trade) error {
	outFile, err := os.OpenFile("./data_test/ter_storage_test.txt", os.O_RDONLY, os.ModePerm)
	if err != nil {
		return err
	}
	defer outFile.Close()
	rd := bufio.NewReader(outFile)
	for {
		line, err := rd.ReadString('\n')
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return err
		}
		if line == "\n" {
			continue
		}
		words := strings.Fields(line)
		if words[1] == exchName {
			switch words[0] {
			case "Ticker":
				key := words[2]
				price, err := strconv.ParseFloat(words[3], 64)
				if err != nil {
					return err
				}
				val := storage.Ticker{
					Price: price,
				}
				terTickers[key] = val
			case "Trade":
				key := words[2]
				size, err := strconv.ParseFloat(words[3], 64)
				if err != nil {
					return err
				}
				price, err := strconv.ParseFloat(words[4], 64)
				if err != nil {
					return err
				}
				val := storage.Trade{
					Size:  size,
					Price: price,
				}
				terTrades[key] = val
			}
		}
	}
	return nil
}

// readMySQL reads ticker and trade data for an exchange from mysql into passed in maps.
func readMySQL(exchName string, mysqlTickers map[string]storage.Ticker, mysqlTrades map[string]storage.Trade, mysql *storage.MySQL) error {
	var ctx context.Context
	if mysql.Cfg.ReqTimeoutSec > 0 {
		timeoutCtx, cancel := context.WithTimeout(context.Background(), time.Duration(mysql.Cfg.ReqTimeoutSec)*time.Second)
		ctx = timeoutCtx
		defer cancel()
	} else {
		ctx = context.Background()
	}
	tickerRows, err := mysql.DB.QueryContext(ctx, "select market, avg(price) as price from ticker where exchange = ? group by market", exchName)
	if err != nil {
		return err
	}
	defer tickerRows.Close()
	for tickerRows.Next() {
		var market string
		var price float64
		err = tickerRows.Scan(&market, &price)
		if err != nil {
			return err
		}
		val := storage.Ticker{
			Price: price,
		}
		mysqlTickers[market] = val
	}
	err = tickerRows.Err()
	if err != nil {
		return err
	}

	if mysql.Cfg.ReqTimeoutSec > 0 {
		timeoutCtx, cancel := context.WithTimeout(context.Background(), time.Duration(mysql.Cfg.ReqTimeoutSec)*time.Second)
		ctx = timeoutCtx
		defer cancel()
	} else {
		ctx = context.Background()
	}
	tradeRows, err := mysql.DB.QueryContext(ctx, "select market, avg(size) as size, avg(price) as price from trade where exchange = ? group by market", exchName)
	if err != nil {
		return err
	}
	defer tradeRows.Close()
	for tradeRows.Next() {
		var market string
		var size float64
		var price float64
		err = tradeRows.Scan(&market, &size, &price)
		if err != nil {
			return err
		}
		val := storage.Trade{
			Size:  size,
			Price: price,
		}
		mysqlTrades[market] = val
	}
	err = tradeRows.Err()
	if err != nil {
		return err
	}
	tradeRows.Close()
	return nil
}

// readElasticSearch reads ticker and trade data for an exchange from elastic search into passed in maps.
func readElasticSearch(exchName string, esTickers map[string]storage.Ticker, esTrades map[string]storage.Trade, es *storage.ElasticSearch) error {
	var buf bytes.Buffer
	query := map[string]interface{}{
		"size": 0,
		"query": map[string]interface{}{
			"match": map[string]interface{}{
				"exchange": exchName,
			},
		},
		"aggs": map[string]interface{}{
			"by_channel": map[string]interface{}{
				"terms": map[string]interface{}{
					"field": "channel",
				},
				"aggs": map[string]interface{}{
					"by_market": map[string]interface{}{
						"terms": map[string]interface{}{
							"field": "market",
						},
						"aggs": map[string]interface{}{
							"size": map[string]interface{}{
								"avg": map[string]interface{}{
									"field": "size",
								},
							},
							"price": map[string]interface{}{
								"avg": map[string]interface{}{
									"field": "price",
								},
							},
						},
					},
				},
			},
		},
	}
	if err := jsoniter.NewEncoder(&buf).Encode(query); err != nil {
		return err
	}

	var ctx context.Context
	if es.Cfg.ReqTimeoutSec > 0 {
		timeoutCtx, cancel := context.WithTimeout(context.Background(), time.Duration(es.Cfg.ReqTimeoutSec)*time.Second)
		ctx = timeoutCtx
		defer cancel()
	} else {
		ctx = context.Background()
	}
	res, err := es.ES.Search(
		es.ES.Search.WithIndex(es.IndexName),
		es.ES.Search.WithBody(&buf),
		es.ES.Search.WithTrackTotalHits(false),
		es.ES.Search.WithPretty(),
		es.ES.Search.WithContext(ctx),
	)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.IsError() {
		return err
	}

	var esResp esResp
	if err := json.NewDecoder(res.Body).Decode(&esResp); err != nil {
		return err
	}
	for _, channel := range esResp.Aggregations.ByChannel.Buckets {
		switch channel.Key {
		case "ticker":
			for _, market := range channel.ByMarket.Buckets {
				val := storage.Ticker{
					Price: market.Price.Value,
				}
				esTickers[market.Key] = val
			}
		case "trade":
			for _, market := range channel.ByMarket.Buckets {
				val := storage.Trade{
					Size:  market.Size.Value,
					Price: market.Price.Value,
				}
				esTrades[market.Key] = val
			}
		}
	}
	return nil
}

// verifyData checks whether all the configured storage system for an exchange got the required data or not.
func verifyData(exchName string, terTickers map[string]storage.Ticker, terTrades map[string]storage.Trade,
	mysqlTickers map[string]storage.Ticker, mysqlTrades map[string]storage.Trade,
	esTickers map[string]storage.Ticker, esTrades map[string]storage.Trade,
	cfg *config.Config) error {

	for _, exch := range cfg.Exchanges {
		if exch.Name == exchName {
			for _, market := range exch.Markets {
				var marketCommitName string
				if market.CommitName != "" {
					marketCommitName = market.CommitName
				} else {
					marketCommitName = market.ID
				}
				for _, info := range market.Info {
					switch info.Channel {
					case "ticker":
						for _, str := range info.Storages {
							switch str {
							case "terminal":
								terTicker := terTickers[marketCommitName]
								if terTicker.Price <= 0 {
									return fmt.Errorf("%s ticker data stored in terminal is not complete", market.ID)
								}
							case "mysql":
								sqlTicker := mysqlTickers[marketCommitName]
								if sqlTicker.Price <= 0 {
									return fmt.Errorf("%s ticker data stored in mysql is not complete", market.ID)
								}
							case "elastic_search":
								esTicker := esTickers[marketCommitName]
								if esTicker.Price <= 0 {
									return fmt.Errorf("%s ticker data stored in elastic search is not complete", market.ID)
								}
							}
						}
					case "trade":
						for _, str := range info.Storages {
							switch str {
							case "terminal":
								terTrade := terTrades[marketCommitName]
								if terTrade.Size <= 0 || terTrade.Price <= 0 {
									return fmt.Errorf("%s trade data stored in terminal is not complete", market.ID)
								}
							case "mysql":
								sqlTrade := mysqlTrades[marketCommitName]
								if sqlTrade.Size <= 0 || sqlTrade.Price <= 0 {
									return fmt.Errorf("%s trade data stored in mysql is not complete", market.ID)
								}
							case "elastic_search":
								esTrade := esTrades[marketCommitName]
								if esTrade.Size <= 0 || esTrade.Price <= 0 {
									return fmt.Errorf("%s trade data stored in elastic search is not complete", market.ID)
								}
							}
						}
					}
				}
			}
		}
	}
	return nil
}

type esResp struct {
	Aggregations struct {
		ByChannel struct {
			Buckets []struct {
				Key      string `json:"key"`
				ByMarket struct {
					Buckets []struct {
						Key  string `json:"key"`
						Size struct {
							Value float64 `json:"value"`
						} `json:"size"`
						Price struct {
							Value float64 `json:"value"`
						} `json:"price"`
					} `json:"buckets"`
				} `json:"by_market"`
			} `json:"buckets"`
		} `json:"by_channel"`
	} `json:"aggregations"`
}
