package config

const (
	// FtxWebsocketURL is the ftx exchange websocket url.
	FtxWebsocketURL = "wss://ftx.com/ws/"
	// FtxRESTBaseURL is the ftx exchange base REST url.
	FtxRESTBaseURL = "https://ftx.com/api/"

	// CoinbaseProWebsocketURL is the coinbase-pro exchange websocket url.
	CoinbaseProWebsocketURL = "wss://ws-feed.pro.coinbase.com/"
	// CoinbaseProRESTBaseURL is the coinbase-pro exchange base REST url.
	CoinbaseProRESTBaseURL = "https://api.pro.coinbase.com/"

	// BinanceWebsocketURL is the binance exchange websocket url.
	BinanceWebsocketURL = "wss://stream.binance.com:9443/ws"
	// BinanceRESTBaseURL is the binance exchange base REST url.
	BinanceRESTBaseURL = "https://api.binance.com/api/v3/"

	// BitfinexWebsocketURL is the bitfinex exchange websocket url.
	BitfinexWebsocketURL = "wss://api-pub.bitfinex.com/ws/2"
	// BitfinexRESTBaseURL is the bitfinex exchange base REST url.
	BitfinexRESTBaseURL = "https://api-pub.bitfinex.com/v2/"

	// HbtcWebsocketURL is the hbtc exchange websocket url.
	HbtcWebsocketURL = "wss://wsapi.hbtc.com/openapi/quote/ws/v2"
	// HbtcRESTBaseURL is the hbtc exchange base REST url.
	HbtcRESTBaseURL = "https://api.hbtc.com/"
)

// Config contains config values for the app.
// Struct values are loaded from user defined JSON config file.
type Config struct {
	Exchanges  []Exchange `json:"exchanges"`
	Connection Connection `json:"connection"`
	Log        Log        `json:"log"`
}

// Exchange contains config values for different exchanges.
type Exchange struct {
	Name    string   `json:"name"`
	Markets []Market `json:"markets"`
	Retry   Retry    `json:"retry"`
}

// Market contains config values for different markets.
type Market struct {
	ID         string `json:"id"`
	Info       []Info `json:"info"`
	CommitName string `json:"commit_name"`
}

// Info contains config values for different market channels.
type Info struct {
	Channel          string   `json:"channel"`
	Connector        string   `json:"connector"`
	WsConsiderIntSec int      `json:"websocket_consider_interval_sec"`
	RESTPingIntSec   int      `json:"rest_ping_interval_sec"`
	Storages         []string `json:"storages"`
}

// Retry contains config values for retry process.
type Retry struct {
	Number   int `json:"number"`
	GapSec   int `json:"gap_sec"`
	ResetSec int `json:"reset_sec"`
}

// Connection contains config values for different API and storage connections.
type Connection struct {
	WS       WS       `json:"websocket"`
	REST     REST     `json:"rest"`
	Terminal Terminal `json:"terminal"`
	MySQL    MySQL    `json:"mysql"`
	ES       ES       `json:"elastic_search"`
}

// WS contains config values for websocket connection.
type WS struct {
	ConnTimeoutSec int `json:"conn_timeout_sec"`
	ReadTimeoutSec int `json:"read_timeout_sec"`
}

// REST contains config values for REST API connection.
type REST struct {
	ReqTimeoutSec       int `json:"request_timeout_sec"`
	MaxIdleConns        int `json:"max_idle_conns"`
	MaxIdleConnsPerHost int `json:"max_idle_conns_per_host"`
}

// Terminal contains config values for terminal display.
type Terminal struct {
	TickerCommitBuf int `json:"ticker_commit_buffer"`
	TradeCommitBuf  int `json:"trade_commit_buffer"`
}

// MySQL contains config values for mysql.
type MySQL struct {
	User               string `josn:"user"`
	Password           string `json:"password"`
	URL                string `json:"URL"`
	Schema             string `json:"schema"`
	ReqTimeoutSec      int    `json:"request_timeout_sec"`
	ConnMaxLifetimeSec int    `json:"conn_max_lifetime_sec"`
	MaxOpenConns       int    `json:"max_open_conns"`
	MaxIdleConns       int    `json:"max_idle_conns"`
	TickerCommitBuf    int    `json:"ticker_commit_buffer"`
	TradeCommitBuf     int    `json:"trade_commit_buffer"`
}

// ES contains config values for elastic search.
type ES struct {
	Addresses           []string `json:"addresses"`
	Username            string   `json:"username"`
	Password            string   `json:"password"`
	IndexName           string   `json:"index_name"`
	ReqTimeoutSec       int      `json:"request_timeout_sec"`
	MaxIdleConns        int      `json:"max_idle_conns"`
	MaxIdleConnsPerHost int      `json:"max_idle_conns_per_host"`
	TickerCommitBuf     int      `json:"ticker_commit_buffer"`
	TradeCommitBuf      int      `json:"trade_commit_buffer"`
}

// Log contains config values for logging.
type Log struct {
	Level    string `json:"level"`
	FilePath string `json:"file_path"`
}
