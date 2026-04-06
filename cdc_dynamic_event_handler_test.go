package tubing_cdc

import (
	"encoding/json"
	"strings"
	"sync"
	"testing"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/schema"
)

func TestSnakeToExportedGoName(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{name: "empty", in: "", want: "Col"},
		{name: "simple", in: "id", want: "Id"},
		{name: "snake", in: "user_name", want: "UserName"},
		{name: "hyphen", in: "order-id", want: "OrderId"},
		{name: "leading digit column", in: "1st", want: "X1st"},
		{name: "spaces", in: "a b", want: "AB"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := snakeToExportedGoName(tt.in)
			if got != tt.want {
				t.Fatalf("got %q, want %q", got, tt.want)
			}
		})
	}
}

func TestUniqueExportedFieldNames(t *testing.T) {
	tests := []struct {
		name string
		cols []schema.TableColumn
		want []string
	}{
		{
			name: "dedupe",
			cols: []schema.TableColumn{
				{Name: "id"},
				{Name: "ID"},
				{Name: "name"},
			},
			want: []string{"Id", "Id_2", "Name"},
		},
		{
			name: "snake distinct",
			cols: []schema.TableColumn{
				{Name: "foo_bar"},
				{Name: "foo"},
			},
			want: []string{"FooBar", "Foo"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := uniqueExportedFieldNames(tt.cols)
			if len(got) != len(tt.want) {
				t.Fatalf("len got %d want %d: %v vs %v", len(got), len(tt.want), got, tt.want)
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Fatalf("index %d: got %q want %q", i, got[i], tt.want[i])
				}
			}
		})
	}
}

func TestGenerateStructGoSource(t *testing.T) {
	tests := []struct {
		name string
		tbl  *schema.Table
		// substrings we expect in output
		contains []string
	}{
		{
			name: "dem style",
			tbl: &schema.Table{
				Schema: "cdc_test",
				Name:   "dem_test",
				Columns: []schema.TableColumn{
					{Name: "id", Type: schema.TYPE_NUMBER, IsUnsigned: true},
					{Name: "name", Type: schema.TYPE_STRING},
				},
			},
			contains: []string{
				"type CdcTestDemTestRow struct",
				"Id uint64",
				`mysql:"id"`,
				"Name string",
				`mysql:"name"`,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			src := generateStructGoSource(tt.tbl)
			for _, sub := range tt.contains {
				if !strings.Contains(src, sub) {
					t.Fatalf("generated source missing %q:\n%s", sub, src)
				}
			}
		})
	}
}

func TestBuildRuntimeRowStructType_roundTripJSON(t *testing.T) {
	tbl := &schema.Table{
		Schema: "db",
		Name:   "t",
		Columns: []schema.TableColumn{
			{Name: "id", Type: schema.TYPE_NUMBER},
			{Name: "label", Type: schema.TYPE_STRING},
		},
	}
	rt := buildRuntimeRowStructType(tbl)
	row := rowValuesToStruct(rt, []interface{}{int64(7), "x"})
	b, err := json.Marshal(row)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	const want = `{"id":7,"label":"x"}`
	if string(b) != want {
		t.Fatalf("got %s want %s", b, want)
	}
}

func TestDynamicTableEventHandler_allows(t *testing.T) {
	tests := []struct {
		name      string
		tables    []string
		key       string
		wantAllow bool
	}{
		{name: "empty allow all", tables: nil, key: "any.t", wantAllow: true},
		{name: "empty slice allow all", tables: []string{}, key: "db.t", wantAllow: true},
		{name: "whitespace ignored", tables: []string{"  ", ""}, key: "db.t", wantAllow: true},
		{name: "match", tables: []string{"db.t"}, key: "db.t", wantAllow: true},
		{name: "no match", tables: []string{"db.t"}, key: "db.other", wantAllow: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := NewDynamicTableEventHandler(tt.tables)
			if got := h.allows(tt.key); got != tt.wantAllow {
				t.Fatalf("allows(%q) = %v, want %v", tt.key, got, tt.wantAllow)
			}
		})
	}
}

func TestExportedTableRowTypeName(t *testing.T) {
	tbl := &schema.Table{Schema: "cdc_test", Name: "dem_test"}
	got := exportedTableRowTypeName(tbl)
	want := "CdcTestDemTestRow"
	if got != want {
		t.Fatalf("got %q want %q", got, want)
	}
}

type sliceRowSink struct {
	mu   sync.Mutex
	rows []string
}

func (s *sliceRowSink) Emit(tableKey, action string, payloadJSON []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rows = append(s.rows, action+"\t"+tableKey+"\t"+string(payloadJSON))
	return nil
}

func TestDynamicRowStructToMap(t *testing.T) {
	tbl := &schema.Table{
		Schema: "db",
		Name:   "t",
		Columns: []schema.TableColumn{
			{Name: "id", Type: schema.TYPE_NUMBER},
			{Name: "meta", Type: schema.TYPE_STRING},
		},
	}
	rt := buildRuntimeRowStructType(tbl)
	row := rowValuesToStruct(rt, []interface{}{int64(1), "x"})
	tests := []struct {
		name string
		in   any
		want map[string]any
	}{
		{
			name: "dynamic row struct",
			in:   row,
			want: map[string]any{"id": int64(1), "meta": "x"},
		},
		{name: "nil", in: nil, want: nil},
		{name: "not struct", in: 42, want: nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := dynamicRowStructToMap(tt.in)
			if tt.want == nil {
				if got != nil {
					t.Fatalf("got %#v, want nil", got)
				}
				return
			}
			if len(got) != len(tt.want) {
				t.Fatalf("got %#v, want %#v", got, tt.want)
			}
			for k, wv := range tt.want {
				if gv, ok := got[k]; !ok || gv != wv {
					t.Fatalf("key %q: got %v want %v", k, gv, wv)
				}
			}
		})
	}
}

func TestDynamicTableEventHandler_OnRow_fieldTransformRules(t *testing.T) {
	tbl := &schema.Table{
		Schema: "db",
		Name:   "t",
		Columns: []schema.TableColumn{
			{Name: "id", Type: schema.TYPE_NUMBER},
			{Name: "raw", Type: schema.TYPE_STRING},
		},
	}
	key := "db.t"
	tests := []struct {
		name      string
		tables    []string
		rules     []RowFieldTransformRule
		ev        *canal.RowsEvent
		wantJSON  string
		wantEmpty bool
	}{
		{
			name:   "add derived field from raw",
			tables: nil,
			rules: []RowFieldTransformRule{{
				SourceColumn: "raw",
				Transform: func(tableKey, action string, value any) map[string]any {
					return map[string]any{"parsed": map[string]any{"v": value}}
				},
			}},
			ev: &canal.RowsEvent{
				Table:  tbl,
				Action: canal.InsertAction,
				Rows:   [][]interface{}{{int64(1), "hello"}},
			},
			wantJSON: `{"id":1,"parsed":{"v":"hello"},"raw":"hello"}`,
		},
		{
			name:   "overwrite same key",
			tables: nil,
			rules: []RowFieldTransformRule{{
				SourceColumn: "raw",
				Transform: func(tableKey, action string, value any) map[string]any {
					return map[string]any{"raw": "replaced"}
				},
			}},
			ev: &canal.RowsEvent{
				Table:  tbl,
				Action: canal.InsertAction,
				Rows:   [][]interface{}{{int64(1), "hello"}},
			},
			wantJSON: `{"id":1,"raw":"replaced"}`,
		},
		{
			name:   "table key mismatch skips rule",
			tables: nil,
			rules: []RowFieldTransformRule{{
				TableKey:     "other.t",
				SourceColumn: "raw",
				Transform: func(tableKey, action string, value any) map[string]any {
					return map[string]any{"extra": true}
				},
			}},
			ev: &canal.RowsEvent{
				Table:  tbl,
				Action: canal.InsertAction,
				Rows:   [][]interface{}{{int64(1), "hello"}},
			},
			wantJSON: `{"id":1,"raw":"hello"}`,
		},
		{
			name:   "table key match applies",
			tables: nil,
			rules: []RowFieldTransformRule{{
				TableKey:     key,
				SourceColumn: "raw",
				Transform: func(tableKey, action string, value any) map[string]any {
					return map[string]any{"extra": 1}
				},
			}},
			ev: &canal.RowsEvent{
				Table:  tbl,
				Action: canal.InsertAction,
				Rows:   [][]interface{}{{int64(1), "hello"}},
			},
			wantJSON: `{"extra":1,"id":1,"raw":"hello"}`,
		},
		{
			name:   "later rule overwrites same output key",
			tables: nil,
			rules: []RowFieldTransformRule{
				{
					SourceColumn: "id",
					Transform: func(tableKey, action string, value any) map[string]any {
						return map[string]any{"n": 1}
					},
				},
				{
					SourceColumn: "raw",
					Transform: func(tableKey, action string, value any) map[string]any {
						return map[string]any{"n": 2}
					},
				},
			},
			ev: &canal.RowsEvent{
				Table:  tbl,
				Action: canal.InsertAction,
				Rows:   [][]interface{}{{int64(9), "z"}},
			},
			wantJSON: `{"id":9,"n":2,"raw":"z"}`,
		},
		{
			name:   "update transforms before and after",
			tables: nil,
			rules: []RowFieldTransformRule{{
				SourceColumn: "id",
				Transform: func(tableKey, action string, value any) map[string]any {
					return map[string]any{"id_tag": value}
				},
			}},
			ev: &canal.RowsEvent{
				Table:  tbl,
				Action: canal.UpdateAction,
				Rows: [][]interface{}{
					{int64(1), "a"},
					{int64(2), "b"},
				},
			},
			wantJSON: `{"after":{"id":2,"id_tag":2,"raw":"b"},"before":{"id":1,"id_tag":1,"raw":"a"}}`,
		},
		{
			name:   "registered table filter skips transform when not allowed",
			tables: []string{"db.other"},
			rules: []RowFieldTransformRule{{
				SourceColumn: "raw",
				Transform: func(tableKey, action string, value any) map[string]any {
					return map[string]any{"x": 1}
				},
			}},
			ev: &canal.RowsEvent{
				Table:  tbl,
				Action: canal.InsertAction,
				Rows:   [][]interface{}{{int64(1), "hello"}},
			},
			wantEmpty: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sink := &sliceRowSink{}
			h := NewDynamicTableEventHandler(tt.tables,
				WithRowEventSink(sink),
				WithRowFieldTransformRules(tt.rules...),
			)
			if err := h.OnRow(tt.ev); err != nil {
				t.Fatal(err)
			}
			if tt.wantEmpty {
				if len(sink.rows) != 0 {
					t.Fatalf("want no rows, got %v", sink.rows)
				}
				return
			}
			if len(sink.rows) != 1 {
				t.Fatalf("got %d lines, want 1: %v", len(sink.rows), sink.rows)
			}
			parts := strings.SplitN(sink.rows[0], "\t", 3)
			if len(parts) != 3 {
				t.Fatalf("bad line: %q", sink.rows[0])
			}
			gotJSON := parts[2]
			var gotObj, wantObj any
			if err := json.Unmarshal([]byte(gotJSON), &gotObj); err != nil {
				t.Fatalf("unmarshal got: %v", err)
			}
			if err := json.Unmarshal([]byte(tt.wantJSON), &wantObj); err != nil {
				t.Fatalf("unmarshal want: %v", err)
			}
			if !jsonEqual(gotObj, wantObj) {
				t.Fatalf("JSON mismatch\ngot:  %s\nwant: %s", gotJSON, tt.wantJSON)
			}
		})
	}
}

func jsonEqual(a, b any) bool {
	aj, err := json.Marshal(a)
	if err != nil {
		return false
	}
	bj, err := json.Marshal(b)
	if err != nil {
		return false
	}
	return string(aj) == string(bj)
}

func TestDynamicTableEventHandler_OnRow_customSink(t *testing.T) {
	tbl := &schema.Table{
		Schema: "db",
		Name:   "t",
		Columns: []schema.TableColumn{
			{Name: "id", Type: schema.TYPE_NUMBER},
		},
	}
	tests := []struct {
		name      string
		ev        *canal.RowsEvent
		wantLines int
		wantJSON  string
	}{
		{
			name: "insert one row",
			ev: &canal.RowsEvent{
				Table:  tbl,
				Action: canal.InsertAction,
				Rows:   [][]interface{}{{int64(42)}},
			},
			wantLines: 1,
			wantJSON:  `{"id":42}`,
		},
		{
			name: "update before/after",
			ev: &canal.RowsEvent{
				Table:  tbl,
				Action: canal.UpdateAction,
				Rows: [][]interface{}{
					{int64(1)},
					{int64(2)},
				},
			},
			wantLines: 1,
			wantJSON:  `"before"`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sink := &sliceRowSink{}
			h := NewDynamicTableEventHandler(nil, WithRowEventSink(sink))
			if err := h.OnRow(tt.ev); err != nil {
				t.Fatal(err)
			}
			if len(sink.rows) != tt.wantLines {
				t.Fatalf("got %d lines, want %d: %v", len(sink.rows), tt.wantLines, sink.rows)
			}
			if !strings.Contains(sink.rows[0], tt.wantJSON) {
				t.Fatalf("line %q should contain %q", sink.rows[0], tt.wantJSON)
			}
		})
	}
}
