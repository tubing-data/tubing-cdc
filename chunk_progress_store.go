package tubing_cdc

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/dgraph-io/badger/v4"
)

// ErrChunkProgressNotFound is returned when no chunk cursor exists for the table/run pair.
var ErrChunkProgressNotFound = errors.New("tubing-cdc: chunk progress not found")

// ChunkProgressPersistence configures Badger-backed storage for PK-chunk cursors (namespace separate from binlog position keys).
type ChunkProgressPersistence struct {
	BadgerDir string
	// BadgerKeyPrefix is prepended to each record key; empty uses DefaultChunkProgressKeyPrefix.
	BadgerKeyPrefix string
}

func (c *ChunkProgressPersistence) validate() error {
	if c == nil {
		return fmt.Errorf("chunk progress persistence: config is nil")
	}
	if strings.TrimSpace(c.BadgerDir) == "" {
		return fmt.Errorf("chunk progress persistence: BadgerDir is empty")
	}
	return nil
}

func normalizeChunkKeyPrefix(p string) string {
	p = strings.TrimSpace(p)
	if p == "" {
		p = DefaultChunkProgressKeyPrefix
	}
	if !strings.HasSuffix(p, "/") {
		p += "/"
	}
	return p
}

func chunkProgressStorageKey(prefix, tableKey, runID string) []byte {
	tk := strings.TrimSpace(tableKey)
	rid := strings.TrimSpace(runID)
	if rid == "" {
		rid = DefaultChunkProgressRunID
	}
	p := normalizeChunkKeyPrefix(prefix)
	var b strings.Builder
	b.Grow(len(p) + len(tk) + len(rid) + 8)
	b.WriteString(p)
	b.WriteString("k\x00")
	b.WriteString(tk)
	b.WriteByte('\x00')
	b.WriteString(rid)
	return []byte(b.String())
}

// ChunkProgressStore persists ChunkProgressRecord values under a Badger namespace.
type ChunkProgressStore struct {
	db     *badger.DB
	prefix string
	ownDB  bool
}

func newChunkProgressStore(cfg *ChunkProgressPersistence) (*ChunkProgressStore, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	db, err := openBadgerDB(strings.TrimSpace(cfg.BadgerDir))
	if err != nil {
		return nil, err
	}
	s, err := newChunkProgressStoreWithDB(db, cfg, true)
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	return s, nil
}

func newChunkProgressStoreWithDB(db *badger.DB, cfg *ChunkProgressPersistence, ownDB bool) (*ChunkProgressStore, error) {
	if db == nil {
		return nil, fmt.Errorf("chunk progress store: db is nil")
	}
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return &ChunkProgressStore{
		db:     db,
		prefix: cfg.BadgerKeyPrefix,
		ownDB:  ownDB,
	}, nil
}

// Put upserts validated JSON for the record's table_key and run_id.
func (s *ChunkProgressStore) Put(rec ChunkProgressRecord) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("chunk progress store: nil store")
	}
	if err := rec.Validate(); err != nil {
		return err
	}
	rec.TableKey = strings.TrimSpace(rec.TableKey)
	key := chunkProgressStorageKey(s.prefix, rec.TableKey, rec.normalizedRunID())
	b, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("chunk progress: marshal: %w", err)
	}
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, b)
	})
}

// Get returns the saved record for tableKey and runID (empty runID uses DefaultChunkProgressRunID).
func (s *ChunkProgressStore) Get(tableKey, runID string) (ChunkProgressRecord, error) {
	var zero ChunkProgressRecord
	if s == nil || s.db == nil {
		return zero, fmt.Errorf("chunk progress store: nil store")
	}
	if strings.TrimSpace(tableKey) == "" {
		return zero, fmt.Errorf("chunk progress: tableKey is empty")
	}
	key := chunkProgressStorageKey(s.prefix, tableKey, runID)
	var rec ChunkProgressRecord
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			return json.Unmarshal(val, &rec)
		})
	})
	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return zero, ErrChunkProgressNotFound
		}
		return zero, fmt.Errorf("chunk progress: read: %w", err)
	}
	return rec, nil
}

// Delete removes stored progress for tableKey and runID.
func (s *ChunkProgressStore) Delete(tableKey, runID string) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("chunk progress store: nil store")
	}
	if strings.TrimSpace(tableKey) == "" {
		return fmt.Errorf("chunk progress: tableKey is empty")
	}
	key := chunkProgressStorageKey(s.prefix, tableKey, runID)
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(key)
	})
}

// Close closes the Badger database when this store owns it.
func (s *ChunkProgressStore) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	if !s.ownDB {
		s.db = nil
		return nil
	}
	err := s.db.Close()
	s.db = nil
	return err
}

// ReadChunkProgressFromBadger opens Badger read-only and loads one record (for tooling / recovery).
func ReadChunkProgressFromBadger(badgerDir, badgerKeyPrefix, tableKey, runID string) (ChunkProgressRecord, error) {
	var zero ChunkProgressRecord
	if strings.TrimSpace(badgerDir) == "" {
		return zero, errors.New("ReadChunkProgressFromBadger: badgerDir is empty")
	}
	if strings.TrimSpace(tableKey) == "" {
		return zero, errors.New("ReadChunkProgressFromBadger: tableKey is empty")
	}
	opts := badger.DefaultOptions(badgerDir)
	opts.ReadOnly = true
	opts.Logger = nil
	db, err := badger.Open(opts)
	if err != nil {
		return zero, fmt.Errorf("open badger read-only: %w", err)
	}
	defer db.Close()

	key := chunkProgressStorageKey(badgerKeyPrefix, tableKey, runID)
	var rec ChunkProgressRecord
	err = db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			return json.Unmarshal(val, &rec)
		})
	})
	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return zero, ErrChunkProgressNotFound
		}
		return zero, fmt.Errorf("read chunk progress: %w", err)
	}
	return rec, nil
}
