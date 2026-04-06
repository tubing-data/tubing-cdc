package tubing_cdc

import (
	"time"

	"github.com/go-mysql-org/go-mysql/canal"
)

// PositionPersistence configures local Badger storage for binlog position after each committed sync
// (canal OnPosSynced), plus optional periodic snapshots to Redis.
// Set Configs.PositionPersistence with a non-empty BadgerDir to enable; nil disables the feature.
type PositionPersistence struct {
	BadgerDir string
	// BadgerKey is the key inside Badger; empty uses a package default.
	BadgerKey string
	// RedisAddr, when non-empty, enables a background goroutine that SETs the latest JSON state every FlushToRedisInterval.
	RedisAddr     string
	RedisPassword string
	RedisDB       int
	// RedisKey is the Redis key for the JSON blob; empty uses a package default.
	RedisKey string
	// FlushToRedisInterval defaults to 5 minutes when zero or negative.
	FlushToRedisInterval time.Duration
	// GTIDFlavor is stored in the JSON record (e.g. "mysql", "mariadb") for operators parsing GTID on recovery.
	GTIDFlavor string
}

type Configs struct {
	Address  string
	Username string
	Password string
	// Tables lists fully-qualified names as "database.table" for canal IncludeTableRegex.
	Tables []string
	// EventHandler is optional; when nil, MyEventHandler is used.
	// Use NewDynamicTableEventHandler(Tables, tubingcdc.WithRowEventSink(...)) to emit each row as JSON;
	// default sink is LoggerRowSink; use StdoutRowSink, KafkaRowEventSink, or ElasticsearchRowEventSink for other destinations.
	EventHandler canal.EventHandler
	// PositionPersistence is optional; when non-nil and BadgerDir is set, each OnPosSynced updates Badger
	// and optionally mirrors to Redis on a timer. Close the TubingCDC on shutdown to flush Redis and release Badger.
	PositionPersistence *PositionPersistence
}
