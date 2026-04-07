package tubing_cdc

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"
)

func skipWithoutMySQL(t *testing.T) {
	t.Helper()
	c, err := net.DialTimeout("tcp", "127.0.0.1:3306", 300*time.Millisecond)
	if err != nil {
		t.Skipf("skip: no MySQL at 127.0.0.1:3306: %v", err)
	}
	_ = c.Close()
}

func TestValidateMySQLSourceSpecs(t *testing.T) {
	validCfg := &Configs{
		Address: "127.0.0.1:3306",
		Tables:  []string{"db.t"},
	}
	tests := []struct {
		name    string
		specs   []MySQLSourceSpec
		wantErr string
	}{
		{
			name:    "empty slice",
			specs:   nil,
			wantErr: "no MySQL sources",
		},
		{
			name: "empty id",
			specs: []MySQLSourceSpec{
				{ID: "", Config: validCfg},
			},
			wantErr: "source ID is empty",
		},
		{
			name: "invalid id char",
			specs: []MySQLSourceSpec{
				{ID: "a/b", Config: validCfg},
			},
			wantErr: "invalid character",
		},
		{
			name: "nil config",
			specs: []MySQLSourceSpec{
				{ID: "a", Config: nil},
			},
			wantErr: "Config is nil",
		},
		{
			name: "duplicate id",
			specs: []MySQLSourceSpec{
				{ID: "same", Config: validCfg},
				{ID: "same", Config: validCfg},
			},
			wantErr: "duplicate source ID",
		},
		{
			name: "ok two sources",
			specs: []MySQLSourceSpec{
				{ID: "src1", Config: validCfg},
				{ID: "src2", Config: validCfg},
			},
			wantErr: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateMySQLSourceSpecs(tt.specs)
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("unexpected err: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatal("expected error")
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("err %q should contain %q", err.Error(), tt.wantErr)
			}
		})
	}
}

func TestApplyMySQLSourcePersistenceScope(t *testing.T) {
	tests := []struct {
		name string
		cfg  *Configs
		id   string
		// want* empty means skip check (nil persistence)
		wantBadgerKey   string
		wantRedisKey    string
		wantChunkPrefix string
		wantErr         string
	}{
		{
			name:    "nil cfg",
			cfg:     nil,
			id:      "a",
			wantErr: "Configs is nil",
		},
		{
			name:    "bad id",
			cfg:     &Configs{PositionPersistence: &PositionPersistence{BadgerDir: "/tmp"}},
			id:      "x y",
			wantErr: "invalid character",
		},
		{
			name: "defaults for position and chunk",
			cfg: &Configs{
				PositionPersistence:      &PositionPersistence{BadgerDir: "/badger"},
				ChunkProgressPersistence: &ChunkProgressPersistence{BadgerDir: "/badger"},
			},
			id:              "db1",
			wantBadgerKey:   defaultBadgerStateKey + "/db1",
			wantRedisKey:    defaultRedisPositionKey + ":db1",
			wantChunkPrefix: "v1/chunk-progress/db1/",
		},
		{
			name: "custom base keys extended",
			cfg: &Configs{
				PositionPersistence: &PositionPersistence{
					BadgerDir: "/b",
					BadgerKey: "custom/pos",
					RedisKey:  "my:pos",
				},
				ChunkProgressPersistence: &ChunkProgressPersistence{
					BadgerDir:       "/b",
					BadgerKeyPrefix: "chunk/ns/",
				},
			},
			id:              "s1",
			wantBadgerKey:   "custom/pos/s1",
			wantRedisKey:    "my:pos:s1",
			wantChunkPrefix: "chunk/ns/s1/",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var cfg *Configs
			if tt.cfg != nil {
				cfg = cloneConfigsForScopeTest(tt.cfg)
			}
			err := ApplyMySQLSourcePersistenceScope(cfg, tt.id)
			if tt.wantErr != "" {
				if err == nil {
					t.Fatal("expected error")
				}
				if !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("err %q want substring %q", err.Error(), tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected err: %v", err)
			}
			if tt.wantBadgerKey != "" {
				if got := cfg.PositionPersistence.BadgerKey; got != tt.wantBadgerKey {
					t.Fatalf("BadgerKey got %q want %q", got, tt.wantBadgerKey)
				}
			}
			if tt.wantRedisKey != "" {
				if got := cfg.PositionPersistence.RedisKey; got != tt.wantRedisKey {
					t.Fatalf("RedisKey got %q want %q", got, tt.wantRedisKey)
				}
			}
			if tt.wantChunkPrefix != "" {
				if got := cfg.ChunkProgressPersistence.BadgerKeyPrefix; got != tt.wantChunkPrefix {
					t.Fatalf("BadgerKeyPrefix got %q want %q", got, tt.wantChunkPrefix)
				}
			}
		})
	}
}

func cloneConfigsForScopeTest(c *Configs) *Configs {
	out := *c
	if c.PositionPersistence != nil {
		pp := *c.PositionPersistence
		out.PositionPersistence = &pp
	}
	if c.ChunkProgressPersistence != nil {
		cp := *c.ChunkProgressPersistence
		out.ChunkProgressPersistence = &cp
	}
	return &out
}

func TestNewMultiMySQLCDC_secondInstanceErrorRollsBack(t *testing.T) {
	skipWithoutMySQL(t)
	good := &Configs{Address: "127.0.0.1:3306", Tables: []string{"db.t"}}
	bad := &Configs{
		Address:                  "127.0.0.1:3306",
		Tables:                   []string{"db.t"},
		ChunkProgressPersistence: &ChunkProgressPersistence{BadgerDir: ""},
	}
	_, err := NewMultiMySQLCDC([]MySQLSourceSpec{
		{ID: "a", Config: good},
		{ID: "b", Config: bad},
	})
	if err == nil {
		t.Fatal("expected error from invalid second config")
	}
}

func TestNewMultiMySQLCDC_buildAndClose(t *testing.T) {
	skipWithoutMySQL(t)
	base := &Configs{
		Address: "127.0.0.1:3306",
		Tables:  []string{"db.t"},
	}
	cfg1 := *base
	cfg2 := *base

	m, err := NewMultiMySQLCDC([]MySQLSourceSpec{
		{ID: "a", Config: &cfg1},
		{ID: "b", Config: &cfg2},
	})
	if err != nil {
		t.Fatalf("NewMultiMySQLCDC: %v", err)
	}
	defer m.Close()

	ids := m.SourceIDs()
	if len(ids) != 2 || ids[0] != "a" || ids[1] != "b" {
		t.Fatalf("SourceIDs = %v", ids)
	}
	inst := m.Instances()
	if len(inst) != 2 || inst[0] == nil || inst[1] == nil {
		t.Fatalf("Instances = %v", inst)
	}
}

func TestMultiMySQLCDC_Run_nilContext(t *testing.T) {
	skipWithoutMySQL(t)
	m, err := NewMultiMySQLCDC([]MySQLSourceSpec{
		{ID: "one", Config: &Configs{Address: "127.0.0.1:3306", Tables: []string{"db.t"}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	err = m.Run(nil)
	if err == nil || !strings.Contains(err.Error(), "context is nil") {
		t.Fatalf("got %v", err)
	}
}

func TestMultiMySQLCDC_Run_nilReceiver(t *testing.T) {
	var m *MultiMySQLCDC
	err := m.Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "nil") {
		t.Fatalf("got %v", err)
	}
}

func TestMultiMySQLCDC_Run_contextCancel(t *testing.T) {
	m := &MultiMySQLCDC{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := m.Run(ctx)
	if err != context.Canceled {
		t.Fatalf("got %v want Canceled", err)
	}
}

func TestMultiMySQLCDC_Run_zeroInstances(t *testing.T) {
	m := &MultiMySQLCDC{}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := m.Run(ctx); err != nil {
		t.Fatalf("empty run: %v", err)
	}
}
