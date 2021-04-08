package storage

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/milkywaybrain/cryptogalaxy/internal/config"
)

// MySQL is for connecting and inserting data to mysql.
type MySQL struct {
	DB  *sql.DB
	Cfg *config.MySQL
}

var mysql MySQL

// Go time gives Z00:00, mysql timestamp needs +00:00 for UTC.
const mysqlTimestamp = "2006-01-02T15:04:05.999+00:00"

// InitMySQL initializes mysql connection with configured values.
func InitMySQL(cfg *config.MySQL) (*MySQL, error) {
	if mysql.DB == nil {
		dataSourceName := cfg.User + ":" + cfg.Password + cfg.URL + "/" + cfg.Schema
		db, err := sql.Open("mysql",
			dataSourceName)
		if err != nil {
			return nil, err
		}
		db.SetConnMaxLifetime(time.Second * time.Duration(cfg.ConnMaxLifetimeSec))
		db.SetMaxOpenConns(cfg.MaxOpenConns)
		db.SetMaxIdleConns(cfg.MaxIdleConns)

		var ctx context.Context
		if cfg.ReqTimeoutSec > 0 {
			timeoutCtx, cancel := context.WithTimeout(context.Background(), time.Duration(cfg.ReqTimeoutSec)*time.Second)
			ctx = timeoutCtx
			defer cancel()
		} else {
			ctx = context.Background()
		}
		err = db.PingContext(ctx)
		if err != nil {
			return nil, err
		}
		mysql = MySQL{
			DB:  db,
			Cfg: cfg,
		}
	}
	return &mysql, nil
}

// GetMySQL returns already prepared mysql instance.
func GetMySQL() *MySQL {
	return &mysql
}

// CommitTickers batch inserts input ticker data to database.
func (m *MySQL) CommitTickers(appCtx context.Context, data []Ticker) error {
	var sb strings.Builder
	sb.WriteString("INSERT INTO ticker(exchange, market, price, timestamp, created_at) VALUES ")
	for i, ticker := range data {
		if i == 0 {
			sb.WriteString(fmt.Sprintf("(\"%v\", \"%v\", %v, \"%v\", \"%v\")", ticker.Exchange, ticker.MktCommitName, ticker.Price, ticker.Timestamp.Format(mysqlTimestamp), time.Now().UTC().Format(mysqlTimestamp)))
		} else {
			sb.WriteString(fmt.Sprintf(",(\"%v\", \"%v\", %v, \"%v\", \"%v\")", ticker.Exchange, ticker.MktCommitName, ticker.Price, ticker.Timestamp.Format(mysqlTimestamp), time.Now().UTC().Format(mysqlTimestamp)))
		}
	}
	var ctx context.Context
	if m.Cfg.ReqTimeoutSec > 0 {
		timeoutCtx, cancel := context.WithTimeout(appCtx, time.Duration(m.Cfg.ReqTimeoutSec)*time.Second)
		ctx = timeoutCtx
		defer cancel()
	} else {
		ctx = context.Background()
	}
	_, err := m.DB.ExecContext(ctx, sb.String())
	if err != nil {
		return err
	}
	return nil
}

// CommitTrades batch inserts input trade data to database.
func (m *MySQL) CommitTrades(appCtx context.Context, data []Trade) error {
	var sb strings.Builder
	sb.WriteString("INSERT INTO trade(exchange, market, trade_id, side, size, price, timestamp, created_at) VALUES ")
	for i, trade := range data {
		if i == 0 {
			sb.WriteString(fmt.Sprintf("(\"%v\", \"%v\", %v, \"%v\", %v, %v, \"%v\", \"%v\")", trade.Exchange, trade.MktCommitName, trade.TradeID, trade.Side, trade.Size, trade.Price, trade.Timestamp.Format(mysqlTimestamp), time.Now().UTC().Format(mysqlTimestamp)))
		} else {
			sb.WriteString(fmt.Sprintf(",(\"%v\", \"%v\", %v, \"%v\", %v, %v, \"%v\", \"%v\")", trade.Exchange, trade.MktCommitName, trade.TradeID, trade.Side, trade.Size, trade.Price, trade.Timestamp.Format(mysqlTimestamp), time.Now().UTC().Format(mysqlTimestamp)))
		}
	}
	var ctx context.Context
	if m.Cfg.ReqTimeoutSec > 0 {
		timeoutCtx, cancel := context.WithTimeout(appCtx, time.Duration(m.Cfg.ReqTimeoutSec)*time.Second)
		ctx = timeoutCtx
		defer cancel()
	} else {
		ctx = context.Background()
	}
	_, err := m.DB.ExecContext(ctx, sb.String())
	if err != nil {
		return err
	}
	return nil
}
