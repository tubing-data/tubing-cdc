package tubing_cdc

import (
	"fmt"
	"strings"
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
	// Watermark (P1 / DBLog) identifies a dedicated watermark table on the source. When set, that table is
	// included in replication filters if not already listed in Tables. See WatermarkCreateTableSQL.
	Watermark *WatermarkTableConfig
	// WatermarkNotifier receives parsed row events for the watermark table. When nil, watermark rows are
	// delivered only to EventHandler like any other replicated table (if the handler watches that table).
	WatermarkNotifier WatermarkNotifier
	// ForwardWatermarkRowsToEventHandler, when true with a non-nil WatermarkNotifier, invokes the inner
	// handler's OnRow for watermark events after the notifier. Default false: watermark rows are consumed
	// only by the notifier.
	ForwardWatermarkRowsToEventHandler bool
	// ChunkProgressPersistence (P2 / DBLog) stores PK-chunk cursors in Badger under a namespace separate from
	// binlog PositionPersistence keys. When BadgerDir matches PositionPersistence.BadgerDir, a single Badger
	// database is opened for both.
	ChunkProgressPersistence *ChunkProgressPersistence
	// Algorithm1 (P3 / DBLog) wraps the event handler to record primary-key touches on TargetTableKey
	// while Algorithm1Tracker is in an open watermark window. Wire Tracker.OnWatermark via
	// ChainWatermarkNotifiers (or equivalent) with WatermarkNotifier; nil disables wrapping.
	Algorithm1 *Algorithm1Config
	// ChunkProcessingControl (P4) is optional; when set, TubingCDC exposes it for pause/resume between chunk steps.
	ChunkProcessingControl *ChunkProcessingControl
	// FullStateJobQueue (P4) is optional; when set, TubingCDC exposes a FIFO for PlanFullStateJobs output.
	FullStateJobQueue *FullStateJobQueue
	// LeaderElection (P5 / DBLog HA) configures Redis-backed leader election. Use
	// RunTubingCDCWithLeaderElection or AcquireRedisLeaderSession; nil means single-process Run only.
	LeaderElection *LeaderElectionConfig
}

// Algorithm1Config binds a shared Algorithm1Tracker to the table whose chunked snapshots are reconciled.
type Algorithm1Config struct {
	Tracker *Algorithm1Tracker
	// TargetTableKey is fully qualified "database.table", same convention as Configs.Tables.
	TargetTableKey string
}

func (c *Algorithm1Config) validate(watermark *WatermarkTableConfig) error {
	if c == nil {
		return fmt.Errorf("algorithm1: config is nil")
	}
	if c.Tracker == nil {
		return fmt.Errorf("algorithm1: Tracker is nil")
	}
	tk := strings.TrimSpace(c.TargetTableKey)
	if tk == "" {
		return fmt.Errorf("algorithm1: TargetTableKey is empty")
	}
	if _, err := ParseTableIdentity(tk); err != nil {
		return fmt.Errorf("algorithm1: %w", err)
	}
	if watermark != nil && strings.TrimSpace(watermark.TableKey) != "" {
		if strings.TrimSpace(watermark.TableKey) == tk {
			return fmt.Errorf("algorithm1: TargetTableKey must differ from Watermark.TableKey")
		}
	}
	return nil
}
