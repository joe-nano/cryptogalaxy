package storage

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	elasticsearch "github.com/elastic/go-elasticsearch/v7"
	jsoniter "github.com/json-iterator/go"
	"github.com/milkywaybrain/cryptogalaxy/internal/config"
)

// ElasticSearch is for connecting and indexing data to elastic search.
type ElasticSearch struct {
	ES        *elasticsearch.Client
	IndexName string
	Cfg       *config.ES
}

var elasticSearch ElasticSearch

// InitElasticSearch initializes elastic search connection with configured values.
func InitElasticSearch(cfg *config.ES) (*ElasticSearch, error) {
	if elasticSearch.ES == nil {
		t := http.DefaultTransport.(*http.Transport).Clone()
		t.MaxIdleConns = cfg.MaxIdleConns
		t.MaxIdleConnsPerHost = cfg.MaxIdleConnsPerHost
		esCfg := elasticsearch.Config{
			Addresses: cfg.Addresses,
			Username:  cfg.Username,
			Password:  cfg.Password,
			Transport: t,
		}
		es, err := elasticsearch.NewClient(esCfg)
		if err != nil {
			return nil, err
		}
		var ctx context.Context
		if cfg.ReqTimeoutSec > 0 {
			timeoutCtx, cancel := context.WithTimeout(context.Background(), time.Duration(cfg.ReqTimeoutSec)*time.Second)
			ctx = timeoutCtx
			defer cancel()
		} else {
			ctx = context.Background()
		}
		_, err = es.Ping(es.Ping.WithContext(ctx))
		if err != nil {
			return nil, err
		}
		elasticSearch = ElasticSearch{
			ES:        es,
			IndexName: cfg.IndexName,
			Cfg:       cfg,
		}
	}
	return &elasticSearch, nil
}

// GetElasticSearch returns already prepared elastic search instance.
func GetElasticSearch() *ElasticSearch {
	return &elasticSearch
}

// esData holds either ticker or trade data which will be sent to elastic search
type esData struct {
	Channel   string    `json:"channel"`
	Exchange  string    `json:"exchange"`
	Market    string    `json:"market"`
	TradeID   uint64    `json:"trade_id"`
	Side      string    `json:"side"`
	Size      float64   `json:"size"`
	Price     float64   `json:"price"`
	Timestamp time.Time `json:"timestamp"`
	CreatedAt time.Time `json:"created_at"`
}

// CommitTickers batch inserts input ticker data to elastic search.
func (e *ElasticSearch) CommitTickers(appCtx context.Context, data []Ticker) error {
	var buf bytes.Buffer
	for _, ticker := range data {
		meta := []byte(fmt.Sprintf(`{"create":{}}%s`, "\n"))
		ed := esData{
			Channel:   "ticker",
			Exchange:  ticker.Exchange,
			Market:    ticker.MktCommitName,
			Price:     ticker.Price,
			Timestamp: ticker.Timestamp,
			CreatedAt: time.Now().UTC(),
		}
		esBytes, err := jsoniter.Marshal(ed)
		if err != nil {
			return err
		}
		esBytes = append(esBytes, "\n"...)
		buf.Grow(len(meta) + len(esBytes))
		buf.Write(meta)
		buf.Write(esBytes)
	}
	var ctx context.Context
	if e.Cfg.ReqTimeoutSec > 0 {
		timeoutCtx, cancel := context.WithTimeout(appCtx, time.Duration(e.Cfg.ReqTimeoutSec)*time.Second)
		ctx = timeoutCtx
		defer cancel()
	} else {
		ctx = context.Background()
	}
	resp, err := e.ES.Bulk(bytes.NewReader(buf.Bytes()), e.ES.Bulk.WithIndex(e.IndexName), e.ES.Bulk.WithContext(ctx))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("code : %v, status : %v", resp.StatusCode, resp.Status())
	}
	_, err = io.Copy(io.Discard, resp.Body)
	if err != nil {
		return err
	}
	return nil
}

// CommitTrades batch inserts input trade data to elastic search.
func (e *ElasticSearch) CommitTrades(appCtx context.Context, data []Trade) error {
	var buf bytes.Buffer
	for _, trade := range data {
		meta := []byte(fmt.Sprintf(`{"create":{}}%s`, "\n"))
		ed := esData{
			Channel:   "trade",
			Exchange:  trade.Exchange,
			Market:    trade.MktCommitName,
			TradeID:   trade.TradeID,
			Side:      trade.Side,
			Size:      trade.Size,
			Price:     trade.Price,
			Timestamp: trade.Timestamp,
			CreatedAt: time.Now().UTC(),
		}
		esBytes, err := jsoniter.Marshal(ed)
		if err != nil {
			return err
		}
		esBytes = append(esBytes, "\n"...)
		buf.Grow(len(meta) + len(esBytes))
		buf.Write(meta)
		buf.Write(esBytes)
	}
	var ctx context.Context
	if e.Cfg.ReqTimeoutSec > 0 {
		timeoutCtx, cancel := context.WithTimeout(appCtx, time.Duration(e.Cfg.ReqTimeoutSec)*time.Second)
		ctx = timeoutCtx
		defer cancel()
	} else {
		ctx = context.Background()
	}
	resp, err := e.ES.Bulk(bytes.NewReader(buf.Bytes()), e.ES.Bulk.WithIndex(e.IndexName), e.ES.Bulk.WithContext(ctx))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("code : %v, status : %v", resp.StatusCode, resp.Status())
	}
	_, err = io.Copy(io.Discard, resp.Body)
	if err != nil {
		return err
	}
	return nil
}
