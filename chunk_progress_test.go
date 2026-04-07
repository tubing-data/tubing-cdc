package tubing_cdc

import (
	"strings"
	"testing"
)

func TestChunkProgressRecord_Validate(t *testing.T) {
	tests := []struct {
		name    string
		rec     ChunkProgressRecord
		wantErr bool
		errSub  string
	}{
		{
			name:    "valid minimal",
			rec:     ChunkProgressRecord{TableKey: "db.t", ChunkSize: 500},
			wantErr: false,
		},
		{
			name:    "empty table",
			rec:     ChunkProgressRecord{ChunkSize: 1},
			wantErr: true,
			errSub:  "table_key",
		},
		{
			name:    "bad fqn",
			rec:     ChunkProgressRecord{TableKey: "nodb", ChunkSize: 10},
			wantErr: true,
			errSub:  "database.table",
		},
		{
			name:    "zero chunk size",
			rec:     ChunkProgressRecord{TableKey: "a.b", ChunkSize: 0},
			wantErr: true,
			errSub:  "chunk_size",
		},
		{
			name:    "negative chunk size",
			rec:     ChunkProgressRecord{TableKey: "a.b", ChunkSize: -1},
			wantErr: true,
			errSub:  "chunk_size",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.rec.Validate()
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
				t.Fatalf("unexpected: %v", err)
			}
		})
	}
}

func TestBuildPKOrderedChunkSelect(t *testing.T) {
	tests := []struct {
		name       string
		db, tbl    string
		pk         []string
		size       int
		after      []any
		wantQuery  string
		wantArgs   []any
		wantErr    bool
		errSub     string
		skipArgCmp bool
	}{
		{
			name:      "first chunk single pk",
			db:        "demo",
			tbl:       "users",
			pk:        []string{"id"},
			size:      100,
			after:     nil,
			wantQuery: "SELECT * FROM `demo`.`users` ORDER BY `id` LIMIT ?",
			wantArgs:  []any{100},
		},
		{
			name:      "resume composite pk",
			db:        "demo",
			tbl:       "t",
			pk:        []string{"a", "b"},
			size:      50,
			after:     []any{1, "x"},
			wantQuery: "SELECT * FROM `demo`.`t` WHERE (`a`, `b`) > (?,?) ORDER BY `a`, `b` LIMIT ?",
			wantArgs:  []any{1, "x", 50},
		},
		{
			name:    "empty pk",
			db:      "d",
			tbl:     "t",
			pk:      nil,
			size:    1,
			wantErr: true,
			errSub:  "pkColumns",
		},
		{
			name:    "after pk length mismatch",
			db:      "d",
			tbl:     "t",
			pk:      []string{"id"},
			size:    1,
			after:   []any{1, 2},
			wantErr: true,
			errSub:  "afterPK length",
		},
		{
			name:    "empty db",
			db:      "",
			tbl:     "t",
			pk:      []string{"id"},
			size:    1,
			wantErr: true,
			errSub:  "database and table",
		},
		{
			name:    "empty pk column name",
			db:      "d",
			tbl:     "t",
			pk:      []string{"id", ""},
			size:    1,
			wantErr: true,
			errSub:  "empty pk column",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q, args, err := BuildPKOrderedChunkSelect(tt.db, tt.tbl, tt.pk, tt.size, tt.after)
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
			if q != tt.wantQuery {
				t.Fatalf("query\ngot:  %s\nwant: %s", q, tt.wantQuery)
			}
			if !tt.skipArgCmp {
				if len(args) != len(tt.wantArgs) {
					t.Fatalf("args len got %d want %d", len(args), len(tt.wantArgs))
				}
				for i := range args {
					if args[i] != tt.wantArgs[i] {
						t.Fatalf("arg[%d] got %v want %v", i, args[i], tt.wantArgs[i])
					}
				}
			}
		})
	}
}

func TestChunkProgressRecord_normalizedRunID(t *testing.T) {
	r := ChunkProgressRecord{RunID: ""}
	if got := r.normalizedRunID(); got != DefaultChunkProgressRunID {
		t.Fatalf("got %q want %q", got, DefaultChunkProgressRunID)
	}
}

func TestBuildPKRowsInSelect(t *testing.T) {
	tests := []struct {
		name      string
		db, tbl   string
		pk        []string
		rows      [][]any
		wantQuery string
		wantArgs  []any
		wantErr   bool
		errSub    string
	}{
		{
			name:      "single pk two rows",
			db:        "demo",
			tbl:       "users",
			pk:        []string{"id"},
			rows:      [][]any{{int64(1)}, {int64(2)}},
			wantQuery: "SELECT * FROM `demo`.`users` WHERE (`id`) IN ((?), (?))",
			wantArgs:  []any{int64(1), int64(2)},
		},
		{
			name:      "composite pk",
			db:        "d",
			tbl:       "t",
			pk:        []string{"a", "b"},
			rows:      [][]any{{1, "x"}},
			wantQuery: "SELECT * FROM `d`.`t` WHERE (`a`, `b`) IN ((?,?))",
			wantArgs:  []any{1, "x"},
		},
		{
			name:    "empty rows",
			db:      "d",
			tbl:     "t",
			pk:      []string{"id"},
			rows:    nil,
			wantErr: true,
			errSub:  "pkRows is empty",
		},
		{
			name:    "row width mismatch",
			db:      "d",
			tbl:     "t",
			pk:      []string{"id"},
			rows:    [][]any{{1, 2}},
			wantErr: true,
			errSub:  "pkRows[0]",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q, args, err := BuildPKRowsInSelect(tt.db, tt.tbl, tt.pk, tt.rows)
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
			if q != tt.wantQuery {
				t.Fatalf("query\ngot:  %s\nwant: %s", q, tt.wantQuery)
			}
			if len(args) != len(tt.wantArgs) {
				t.Fatalf("args len got %d want %d", len(args), len(tt.wantArgs))
			}
			for i := range args {
				if args[i] != tt.wantArgs[i] {
					t.Fatalf("arg[%d] got %v want %v", i, args[i], tt.wantArgs[i])
				}
			}
		})
	}
}
