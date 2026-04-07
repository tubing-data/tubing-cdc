package tubing_cdc

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
)

func TestChunkProgressPersistence_validate(t *testing.T) {
	tests := []struct {
		name    string
		cfg     *ChunkProgressPersistence
		wantErr bool
		errSub  string
	}{
		{name: "nil", cfg: nil, wantErr: true, errSub: "nil"},
		{name: "empty dir", cfg: &ChunkProgressPersistence{BadgerDir: "  "}, wantErr: true, errSub: "BadgerDir"},
		{name: "ok", cfg: &ChunkProgressPersistence{BadgerDir: "/tmp/x"}, wantErr: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.validate()
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error")
				}
				if tt.errSub != "" && !strings.Contains(err.Error(), tt.errSub) {
					t.Fatalf("error %q should mention %q", err.Error(), tt.errSub)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestChunkProgressStore_PutGetRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		rec  ChunkProgressRecord
	}{
		{
			name: "default run id",
			rec: ChunkProgressRecord{
				TableKey:  "cdc.orders",
				RunID:     "",
				ChunkSize: 200,
				AfterPK:   []any{int64(42)},
			},
		},
		{
			name: "named run",
			rec: ChunkProgressRecord{
				TableKey:  "cdc.orders",
				RunID:     "snap-2026-04",
				ChunkSize: 200,
				AfterPK:   []any{float64(1), "k"},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			s, err := newChunkProgressStore(&ChunkProgressPersistence{BadgerDir: dir})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = s.Close() }()

			if err := s.Put(tt.rec); err != nil {
				t.Fatal(err)
			}
			got, err := s.Get(tt.rec.TableKey, tt.rec.RunID)
			if err != nil {
				t.Fatal(err)
			}
			if got.TableKey != strings.TrimSpace(tt.rec.TableKey) || got.ChunkSize != tt.rec.ChunkSize {
				t.Fatalf("got %+v want %+v", got, tt.rec)
			}
			if len(got.AfterPK) != len(tt.rec.AfterPK) {
				t.Fatalf("after pk len got %d want %d", len(got.AfterPK), len(tt.rec.AfterPK))
			}
		})
	}
}

func TestChunkProgressStore_Get_notFound(t *testing.T) {
	dir := t.TempDir()
	s, err := newChunkProgressStore(&ChunkProgressPersistence{BadgerDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()

	_, err = s.Get("db.missing", "")
	if !errors.Is(err, ErrChunkProgressNotFound) {
		t.Fatalf("got %v want ErrChunkProgressNotFound", err)
	}
}

func TestReadChunkProgressFromBadger(t *testing.T) {
	dir := t.TempDir()
	s, err := newChunkProgressStore(&ChunkProgressPersistence{
		BadgerDir:       dir,
		BadgerKeyPrefix: "custom/p2/",
	})
	if err != nil {
		t.Fatal(err)
	}
	want := ChunkProgressRecord{TableKey: "a.b", ChunkSize: 10, AfterPK: []any{"z"}}
	if err := s.Put(want); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	got, err := ReadChunkProgressFromBadger(dir, "custom/p2/", "a.b", "")
	if err != nil {
		t.Fatal(err)
	}
	if got.ChunkSize != want.ChunkSize || got.TableKey != want.TableKey {
		t.Fatalf("got %+v want %+v", got, want)
	}
}

func TestSharedBadger_positionAndChunkStores(t *testing.T) {
	dir := t.TempDir()
	db, err := openBadgerDB(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	pos, err := newTwoTierPositionStoreWithDB(db, &PositionPersistence{
		BadgerDir:            dir,
		FlushToRedisInterval: time.Hour,
	}, false)
	if err != nil {
		t.Fatal(err)
	}
	ch, err := newChunkProgressStoreWithDB(db, &ChunkProgressPersistence{BadgerDir: dir}, false)
	if err != nil {
		t.Fatal(err)
	}

	if err := pos.onCommitted(mysql.Position{Name: "mysql-bin.000011", Pos: 4096}, nil); err != nil {
		t.Fatal(err)
	}
	rec := ChunkProgressRecord{TableKey: "db.t", ChunkSize: 5}
	if err := ch.Put(rec); err != nil {
		t.Fatal(err)
	}

	if err := pos.Close(); err != nil {
		t.Fatal(err)
	}
	if err := ch.Close(); err != nil {
		t.Fatal(err)
	}

	// DB still open — verify both namespaces persisted.
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	_, _, err = ReadBinlogStateFromBadger(dir, "")
	if err != nil {
		t.Fatal(err)
	}
	got, err := ReadChunkProgressFromBadger(dir, "", "db.t", "")
	if err != nil {
		t.Fatal(err)
	}
	if got.ChunkSize != 5 {
		t.Fatalf("chunk: got %+v", got)
	}
}
