package tubing_cdc

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/redis/go-redis/v9"
)

func TestNewTwoTierPositionStore_validation(t *testing.T) {
	tests := []struct {
		name    string
		mkCfg   func(*testing.T) *PositionPersistence
		wantErr bool
	}{
		{
			name: "nil config",
			mkCfg: func(*testing.T) *PositionPersistence {
				return nil
			},
			wantErr: true,
		},
		{
			name: "empty badger dir",
			mkCfg: func(*testing.T) *PositionPersistence {
				return &PositionPersistence{BadgerDir: ""}
			},
			wantErr: true,
		},
		{
			name: "valid badger dir",
			mkCfg: func(t *testing.T) *PositionPersistence {
				return &PositionPersistence{BadgerDir: t.TempDir()}
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, err := newTwoTierPositionStore(tt.mkCfg(t))
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			defer s.Close()
		})
	}
}

func TestTwoTierPositionStore_badgerRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		pos  mysql.Position
	}{
		{name: "first segment", pos: mysql.Position{Name: "mysql-bin.000001", Pos: 1234}},
		{name: "after rotate", pos: mysql.Position{Name: "mysql-bin.000007", Pos: 4}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			s, err := newTwoTierPositionStore(&PositionPersistence{
				BadgerDir:            dir,
				GTIDFlavor:           "mysql",
				FlushToRedisInterval: time.Hour,
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := s.onCommitted(tt.pos, nil); err != nil {
				t.Fatal(err)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			gotPos, gotGTID, err := ReadBinlogStateFromBadger(dir, "")
			if err != nil {
				t.Fatal(err)
			}
			if gotGTID != "" {
				t.Fatalf("gtid: got %q want empty", gotGTID)
			}
			if gotPos != tt.pos {
				t.Fatalf("position: got %+v want %+v", gotPos, tt.pos)
			}
		})
	}
}

func TestTwoTierPositionStore_redisPeriodicFlush(t *testing.T) {
	tests := []struct {
		name    string
		redisKey string
	}{
		{name: "default style key", redisKey: "tubing-cdc:test-periodic"},
		{name: "custom prefix", redisKey: "app:cdc:pos:1"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mr := miniredis.RunT(t)
			defer mr.Close()
			dir := t.TempDir()
			interval := 40 * time.Millisecond
			s, err := newTwoTierPositionStore(&PositionPersistence{
				BadgerDir:             dir,
				RedisAddr:             mr.Addr(),
				RedisKey:              tt.redisKey,
				FlushToRedisInterval:  interval,
			})
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()

			pos := mysql.Position{Name: "bin.003", Pos: 99}
			if err := s.onCommitted(pos, nil); err != nil {
				t.Fatal(err)
			}
			rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
			defer rdb.Close()
			deadline := time.Now().Add(5 * interval)
			var val string
			for time.Now().Before(deadline) {
				v, err := rdb.Get(context.Background(), tt.redisKey).Result()
				if err == nil {
					val = v
					break
				}
				time.Sleep(interval / 4)
			}
			if val == "" {
				t.Fatal("expected redis key to be set by periodic flush")
			}
		})
	}
}

func TestTwoTierPositionStore_CloseFlushesRedis(t *testing.T) {
	mr := miniredis.RunT(t)
	defer mr.Close()
	dir := t.TempDir()
	key := "tubing-cdc:test-close-flush"
	s, err := newTwoTierPositionStore(&PositionPersistence{
		BadgerDir:            dir,
		RedisAddr:            mr.Addr(),
		RedisKey:             key,
		FlushToRedisInterval: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	want := mysql.Position{Name: "bin.close", Pos: 42}
	if err := s.onCommitted(want, nil); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()
	val, err := rdb.Get(context.Background(), key).Result()
	if err != nil {
		t.Fatal(err)
	}
	var rec BinlogStateRecord
	if err := json.Unmarshal([]byte(val), &rec); err != nil {
		t.Fatal(err)
	}
	got := mysql.Position{Name: rec.File, Pos: rec.Pos}
	if got != want {
		t.Fatalf("got %+v want %+v", got, want)
	}
}

func TestReadBinlogStateFromBadger_errors(t *testing.T) {
	tests := []struct {
		name    string
		setup   func(*testing.T) (dir string, badgerKey string)
		wantSub string
	}{
		{
			name:    "empty dir",
			setup:   func(*testing.T) (string, string) { return "", "" },
			wantSub: "badgerDir is empty",
		},
		{
			name: "missing key",
			setup: func(t *testing.T) (string, string) {
				dir := t.TempDir()
				s, err := newTwoTierPositionStore(&PositionPersistence{BadgerDir: dir})
				if err != nil {
					t.Fatal(err)
				}
				if err := s.onCommitted(mysql.Position{Name: "x", Pos: 1}, nil); err != nil {
					t.Fatal(err)
				}
				if err := s.Close(); err != nil {
					t.Fatal(err)
				}
				return dir, "no-such-key"
			},
			wantSub: "no saved position",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir, key := tt.setup(t)
			_, _, err := ReadBinlogStateFromBadger(dir, key)
			if err == nil {
				t.Fatal("expected error")
			}
			if !strings.Contains(err.Error(), tt.wantSub) {
				t.Fatalf("error %q should contain %q", err.Error(), tt.wantSub)
			}
		})
	}
}
