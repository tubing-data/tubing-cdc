package tubing_cdc

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/go-mysql-org/go-mysql/schema"
)

func TestParseTableIdentity(t *testing.T) {
	tests := []struct {
		name    string
		fqn     string
		want    TableIdentity
		wantErr bool
	}{
		{name: "ok", fqn: "mydb.orders", want: TableIdentity{Database: "mydb", Table: "orders"}},
		{name: "table with dot", fqn: "mydb.order.items", want: TableIdentity{Database: "mydb", Table: "order.items"}},
		{name: "no dot", fqn: "bogus", wantErr: true},
		{name: "empty", fqn: "", wantErr: true},
		{name: "leading dot", fqn: ".t", wantErr: true},
		{name: "trailing dot", fqn: "db.", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseTableIdentity(tt.fqn)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got != tt.want {
				t.Fatalf("got %+v want %+v", got, tt.want)
			}
		})
	}
}

func TestJSONFieldNameForColumn(t *testing.T) {
	tests := []struct {
		col  string
		want string
	}{
		{col: "id", want: "id"},
		{col: "", want: "col"},
	}
	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			if got := JSONFieldNameForColumn(tt.col); got != tt.want {
				t.Fatalf("got %q want %q", got, tt.want)
			}
		})
	}
}

func TestPrimaryKeyFromTableRow(t *testing.T) {
	tbl := &schema.Table{
		Columns: []schema.TableColumn{
			{Name: "id"},
			{Name: "name"},
		},
		PKColumns: []int{0},
	}
	row := map[string]any{"id": int64(7), "name": "x"}
	got := PrimaryKeyFromTableRow(tbl, row)
	want := map[string]any{"id": int64(7)}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %#v want %#v", got, want)
	}
	if len(PrimaryKeyFromTableRow(nil, row)) != 0 {
		t.Fatal("nil table")
	}
	if len(PrimaryKeyFromTableRow(tbl, nil)) != 0 {
		t.Fatal("nil row")
	}
}

func TestPrimaryKeyFromResolvedPayload(t *testing.T) {
	tbl := &schema.Table{
		Columns: []schema.TableColumn{{Name: "id"}},
		PKColumns: []int{0},
	}
	tests := []struct {
		name   string
		action string
		res    any
		want   map[string]any
	}{
		{
			name:   "insert map",
			action: canal.InsertAction,
			res:    map[string]any{"id": 1},
			want:   map[string]any{"id": 1},
		},
		{
			name:   "update uses after",
			action: canal.UpdateAction,
			res: map[string]any{
				"before": map[string]any{"id": 1},
				"after":  map[string]any{"id": 2},
			},
			want: map[string]any{"id": 2},
		},
		{
			name:   "non-map",
			action: canal.InsertAction,
			res:    "nope",
			want:   map[string]any{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := PrimaryKeyFromResolvedPayload(tbl, tt.action, tt.res)
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("got %#v want %#v", got, tt.want)
			}
		})
	}
}

func TestBinlogPositionFromSources(t *testing.T) {
	hdr := &replication.EventHeader{LogPos: 99}
	rep := &mysql.Position{Name: "bin.001", Pos: 12}
	tests := []struct {
		name string
		rep  *mysql.Position
		hdr  *replication.EventHeader
		want *BinlogPosition
	}{
		{name: "replication wins", rep: rep, hdr: hdr, want: &BinlogPosition{File: "bin.001", Pos: 12}},
		{name: "header only", rep: nil, hdr: hdr, want: &BinlogPosition{Pos: 99}},
		{name: "rep empty uses header", rep: &mysql.Position{}, hdr: hdr, want: &BinlogPosition{Pos: 99}},
		{name: "nil both", rep: nil, hdr: nil, want: nil},
		{name: "zero header", rep: nil, hdr: &replication.EventHeader{LogPos: 0}, want: nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := BinlogPositionFromSources(tt.rep, tt.hdr)
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("got %#v want %#v", got, tt.want)
			}
		})
	}
}

func TestMarshalCDCEventEnvelope(t *testing.T) {
	tbl := &schema.Table{
		Columns: []schema.TableColumn{{Name: "id"}},
		PKColumns: []int{0},
	}
	inner := []byte(`{"id":1}`)
	hdr := &replication.EventHeader{LogPos: 100}
	tests := []struct {
		name         string
		schemaVer    string
		origin       EventOrigin
		action       string
		tableKey     string
		resolved     any
		inner        []byte
		rep          *mysql.Position
		header       *replication.EventHeader
		wantContains map[string]any
		wantErr      bool
	}{
		{
			name:     "log insert",
			origin:   OriginLog,
			action:   canal.InsertAction,
			tableKey: "acme.users",
			resolved: map[string]any{"id": 1},
			inner:    inner,
			header:   hdr,
			wantContains: map[string]any{
				"schema_version": DefaultEnvelopeSchemaVersion,
				"origin":         string(OriginLog),
				"action":         canal.InsertAction,
			},
		},
		{
			name:     "bad table key",
			origin:   OriginLog,
			action:   canal.InsertAction,
			tableKey: "not_fqn",
			resolved: map[string]any{},
			inner:    []byte(`{}`),
			wantErr:  true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b, err := MarshalCDCEventEnvelope(tt.schemaVer, tt.origin, tt.action, tt.tableKey, tbl, tt.resolved, tt.inner, tt.rep, tt.header)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			var got map[string]json.RawMessage
			if err := json.Unmarshal(b, &got); err != nil {
				t.Fatal(err)
			}
			for k, wantVal := range tt.wantContains {
				wb, _ := json.Marshal(wantVal)
				if string(got[k]) != string(wb) {
					t.Fatalf("field %s: got %s want %s", k, string(got[k]), string(wb))
				}
			}
			var env CDCEventEnvelope
			if err := json.Unmarshal(b, &env); err != nil {
				t.Fatal(err)
			}
			if string(env.Payload) != string(tt.inner) {
				t.Fatalf("payload got %s want %s", env.Payload, tt.inner)
			}
		})
	}
}

func TestMarshalCDCEventEnvelope_defaultSchemaVersion(t *testing.T) {
	b, err := MarshalCDCEventEnvelope("", OriginLog, canal.InsertAction, "a.b", &schema.Table{}, map[string]any{}, []byte(`{}`), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	var env CDCEventEnvelope
	if err := json.Unmarshal(b, &env); err != nil {
		t.Fatal(err)
	}
	if env.SchemaVersion != DefaultEnvelopeSchemaVersion {
		t.Fatalf("schema version %q", env.SchemaVersion)
	}
}
