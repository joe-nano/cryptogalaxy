package connector

import (
	"context"
	"net"
	"time"

	"github.com/gobwas/ws"
	"github.com/gobwas/ws/wsutil"
	"github.com/milkywaybrain/cryptogalaxy/internal/config"
)

// Websocket is for websocket connection.
type Websocket struct {
	Conn net.Conn
	Cfg  *config.WS
}

// NewWebsocket creates a new websocket connection for the exchange.
func NewWebsocket(appCtx context.Context, cfg *config.WS, url string) (Websocket, error) {
	var ctx context.Context
	if cfg.ConnTimeoutSec > 0 {
		timeoutCtx, cancel := context.WithTimeout(appCtx, time.Duration(cfg.ConnTimeoutSec)*time.Second)
		ctx = timeoutCtx
		defer cancel()
	} else {
		ctx = context.Background()
	}
	conn, _, _, err := ws.Dial(ctx, url)
	if err != nil {
		return Websocket{}, err
	}
	websocket := Websocket{Conn: conn, Cfg: cfg}
	return websocket, nil
}

// Write writes data frame on websocket connection.
func (w *Websocket) Write(data []byte) error {
	err := wsutil.WriteClientText(w.Conn, data)
	if err != nil {
		return err
	}
	return nil
}

// Read reads data frame from websocket connection.
func (w *Websocket) Read() ([]byte, error) {
	if w.Cfg.ReadTimeoutSec > 0 {
		err := w.Conn.SetReadDeadline(time.Now().Add(time.Duration(w.Cfg.ReadTimeoutSec) * time.Second))
		if err != nil {
			return nil, err
		}
	}
	data, err := wsutil.ReadServerText(w.Conn)
	if err != nil {
		return nil, err
	}
	return data, nil
}
