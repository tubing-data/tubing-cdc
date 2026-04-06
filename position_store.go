package tubing_cdc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/redis/go-redis/v9"
)

const (
	defaultRedisPositionKey = "tubing-cdc:mysql-binlog-position"
	defaultBadgerStateKey   = "v1/mysql-binlog-state"
	defaultGTIDFlavor       = "mysql"
)

// BinlogStateRecord is the JSON shape stored in Badger and Redis for crash recovery.
type BinlogStateRecord struct {
	File       string `json:"file"`
	Pos        uint32 `json:"pos"`
	GTID       string `json:"gtid,omitempty"`
	GTIDFlavor string `json:"gtid_flavor,omitempty"`
}

type twoTierPositionStore struct {
	db        *badger.DB
	rdb       *redis.Client
	redisKey  string
	badgerKey []byte
	interval  time.Duration
	flavor    string

	mu         sync.Mutex
	latestJSON []byte
	hasLatest  bool

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func newTwoTierPositionStore(cfg *PositionPersistence) (*twoTierPositionStore, error) {
	if cfg == nil || cfg.BadgerDir == "" {
		return nil, fmt.Errorf("position persistence requires non-nil PositionPersistence and BadgerDir")
	}

	bopts := badger.DefaultOptions(cfg.BadgerDir)
	bopts.Logger = nil
	db, err := badger.Open(bopts)
	if err != nil {
		return nil, fmt.Errorf("open badger: %w", err)
	}

	interval := cfg.FlushToRedisInterval
	if interval <= 0 {
		interval = 5 * time.Minute
	}

	redisKey := cfg.RedisKey
	if redisKey == "" {
		redisKey = defaultRedisPositionKey
	}
	bkey := cfg.BadgerKey
	if bkey == "" {
		bkey = defaultBadgerStateKey
	}
	flavor := cfg.GTIDFlavor
	if flavor == "" {
		flavor = defaultGTIDFlavor
	}

	ctx, cancel := context.WithCancel(context.Background())
	s := &twoTierPositionStore{
		db:        db,
		redisKey:  redisKey,
		badgerKey: []byte(bkey),
		interval:  interval,
		flavor:    flavor,
		ctx:       ctx,
		cancel:    cancel,
	}

	if cfg.RedisAddr != "" {
		s.rdb = redis.NewClient(&redis.Options{
			Addr:     cfg.RedisAddr,
			Password: cfg.RedisPassword,
			DB:       cfg.RedisDB,
		})
		s.wg.Add(1)
		go s.redisFlushLoop()
	}

	return s, nil
}

func (s *twoTierPositionStore) onCommitted(pos mysql.Position, gtid mysql.GTIDSet) error {
	rec := BinlogStateRecord{
		File:       pos.Name,
		Pos:        pos.Pos,
		GTIDFlavor: s.flavor,
	}
	if gtid != nil {
		if gs := gtid.String(); gs != "" {
			rec.GTID = gs
		}
	}
	b, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("marshal binlog state: %w", err)
	}
	if err := s.db.Update(func(txn *badger.Txn) error {
		return txn.Set(s.badgerKey, b)
	}); err != nil {
		return fmt.Errorf("badger set position: %w", err)
	}
	s.mu.Lock()
	s.latestJSON = append([]byte(nil), b...)
	s.hasLatest = true
	s.mu.Unlock()
	return nil
}

func (s *twoTierPositionStore) flushRedis(ctx context.Context) error {
	if s.rdb == nil {
		return nil
	}
	s.mu.Lock()
	if !s.hasLatest {
		s.mu.Unlock()
		return nil
	}
	payload := append([]byte(nil), s.latestJSON...)
	s.mu.Unlock()
	if err := s.rdb.Set(ctx, s.redisKey, payload, 0).Err(); err != nil {
		return fmt.Errorf("redis set position: %w", err)
	}
	return nil
}

func (s *twoTierPositionStore) redisFlushLoop() {
	defer s.wg.Done()
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			_ = s.flushRedis(s.ctx)
		}
	}
}

// Close stops the Redis flush loop, performs a final Redis write when configured, then closes Badger and Redis clients.
func (s *twoTierPositionStore) Close() error {
	if s == nil {
		return nil
	}
	s.cancel()
	s.wg.Wait()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = s.flushRedis(ctx)

	var firstErr error
	if err := s.db.Close(); err != nil && firstErr == nil {
		firstErr = fmt.Errorf("close badger: %w", err)
	}
	if s.rdb != nil {
		if err := s.rdb.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("close redis: %w", err)
		}
	}
	return firstErr
}

// ReadBinlogStateFromBadger opens the Badger directory read-only and returns the last saved binlog file/pos and raw GTID string.
func ReadBinlogStateFromBadger(badgerDir, badgerKey string) (mysql.Position, string, error) {
	if badgerDir == "" {
		return mysql.Position{}, "", errors.New("badgerDir is empty")
	}
	if badgerKey == "" {
		badgerKey = defaultBadgerStateKey
	}
	opts := badger.DefaultOptions(badgerDir)
	opts.ReadOnly = true
	opts.Logger = nil
	db, err := badger.Open(opts)
	if err != nil {
		return mysql.Position{}, "", fmt.Errorf("open badger read-only: %w", err)
	}
	defer db.Close()

	var rec BinlogStateRecord
	err = db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(badgerKey))
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			return json.Unmarshal(val, &rec)
		})
	})
	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return mysql.Position{}, "", fmt.Errorf("no saved position at key %q", badgerKey)
		}
		return mysql.Position{}, "", fmt.Errorf("read badger: %w", err)
	}
	return mysql.Position{Name: rec.File, Pos: rec.Pos}, rec.GTID, nil
}
