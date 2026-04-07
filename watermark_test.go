package tubing_cdc

import (
	"errors"
	"strings"
	"testing"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/go-mysql-org/go-mysql/schema"
)

func TestWatermarkTableConfig_Validate(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		cfg     *WatermarkTableConfig
		wantErr bool
	}{
		{name: "nil", cfg: nil, wantErr: true},
		{name: "valid", cfg: &WatermarkTableConfig{TableKey: "dblog.wm"}, wantErr: false},
		{name: "bad_fqn", cfg: &WatermarkTableConfig{TableKey: "nodb"}, wantErr: true},
		{name: "empty_table", cfg: &WatermarkTableConfig{TableKey: "db."}, wantErr: true},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := tt.cfg.Validate()
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestWatermarkCreateTableSQL(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		cfg        *WatermarkTableConfig
		wantErr    bool
		wantSubstr []string
	}{
		{
			name:    "nil",
			cfg:     nil,
			wantErr: true,
		},
		{
			name: "defaults",
			cfg:  &WatermarkTableConfig{TableKey: "dblogctl._wm"},
			wantSubstr: []string{
				"CREATE TABLE `dblogctl`.`_wm`",
				"`id` TINYINT UNSIGNED NOT NULL PRIMARY KEY",
				"`watermark_value` CHAR(36) NOT NULL",
				"INSERT INTO `dblogctl`.`_wm`",
				"00000000-0000-0000-0000-000000000000",
			},
		},
		{
			name: "custom_columns",
			cfg: &WatermarkTableConfig{
				TableKey:         "a.b",
				ValueColumn:      "uuid_col",
				PrimaryKeyColumn: "pk",
			},
			wantSubstr: []string{
				"`pk` TINYINT UNSIGNED NOT NULL PRIMARY KEY",
				"`uuid_col` CHAR(36) NOT NULL",
				"INSERT INTO `a`.`b` (`pk`, `uuid_col`)",
			},
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			createSQL, insertSQL, err := WatermarkCreateTableSQL(tt.cfg)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			combined := createSQL + "\n" + insertSQL
			for _, s := range tt.wantSubstr {
				if !strings.Contains(combined, s) {
					t.Errorf("missing substring %q in:\n%s", s, combined)
				}
			}
		})
	}
}

func TestParseWatermarkBinlogEvent(t *testing.T) {
	t.Parallel()
	tbl := &schema.Table{
		Schema: "dblog",
		Name:   "wm",
		Columns: []schema.TableColumn{
			{Name: "id", Type: schema.TYPE_NUMBER},
			{Name: "watermark_value", Type: schema.TYPE_STRING},
		},
	}
	hdr := &replication.EventHeader{LogPos: 42}
	cfg := &WatermarkTableConfig{TableKey: "dblog.wm"}

	tests := []struct {
		name    string
		e       *canal.RowsEvent
		want    WatermarkBinlogEvent
		wantErr bool
	}{
		{
			name:    "nil_event",
			e:       nil,
			wantErr: true,
		},
		{
			name: "insert",
			e: &canal.RowsEvent{
				Table:  tbl,
				Action: canal.InsertAction,
				Rows:   [][]interface{}{{int64(1), "low-uuid-1"}},
				Header: hdr,
			},
			want: WatermarkBinlogEvent{
				TableKey: "dblog.wm", Action: canal.InsertAction,
				NewValue: "low-uuid-1", Header: hdr,
			},
		},
		{
			name: "update",
			e: &canal.RowsEvent{
				Table:  tbl,
				Action: canal.UpdateAction,
				Rows: [][]interface{}{
					{int64(1), "before-uuid"},
					{int64(1), "after-uuid"},
				},
				Header: hdr,
			},
			want: WatermarkBinlogEvent{
				TableKey: "dblog.wm", Action: canal.UpdateAction,
				OldValue: "before-uuid", NewValue: "after-uuid", Header: hdr,
			},
		},
		{
			name: "delete",
			e: &canal.RowsEvent{
				Table:  tbl,
				Action: canal.DeleteAction,
				Rows:   [][]interface{}{{int64(1), "gone"}},
				Header: hdr,
			},
			want: WatermarkBinlogEvent{
				TableKey: "dblog.wm", Action: canal.DeleteAction,
				OldValue: "gone", Header: hdr,
			},
		},
		{
			name: "wrong_table",
			e: &canal.RowsEvent{
				Table: &schema.Table{
					Schema: "other", Name: "t",
					Columns: tbl.Columns,
				},
				Action: canal.InsertAction,
				Rows:   [][]interface{}{{int64(1), "x"}},
			},
			wantErr: true,
		},
		{
			name: "missing_column",
			e: &canal.RowsEvent{
				Table: &schema.Table{
					Schema: "dblog", Name: "wm",
					Columns: []schema.TableColumn{{Name: "id", Type: schema.TYPE_NUMBER}},
				},
				Action: canal.InsertAction,
				Rows:   [][]interface{}{{int64(1)}},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := ParseWatermarkBinlogEvent(cfg, tt.e)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got.TableKey != tt.want.TableKey || got.Action != tt.want.Action ||
				got.NewValue != tt.want.NewValue || got.OldValue != tt.want.OldValue {
				t.Fatalf("got %+v want %+v", got, tt.want)
			}
			if tt.want.Header != nil && got.Header != tt.want.Header {
				t.Fatalf("header pointer mismatch")
			}
		})
	}
}

func TestParseWatermarkBinlogEvent_customValueColumn(t *testing.T) {
	t.Parallel()
	tbl := &schema.Table{
		Schema: "db", Name: "wm",
		Columns: []schema.TableColumn{
			{Name: "id", Type: schema.TYPE_NUMBER},
			{Name: "uuid_col", Type: schema.TYPE_STRING},
		},
	}
	cfg := &WatermarkTableConfig{TableKey: "db.wm", ValueColumn: "uuid_col"}
	ev, err := ParseWatermarkBinlogEvent(cfg, &canal.RowsEvent{
		Table: tbl, Action: canal.InsertAction,
		Rows: [][]interface{}{{int64(1), "u1"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if ev.NewValue != "u1" {
		t.Fatalf("got %q", ev.NewValue)
	}
}

type countingRowHandler struct {
	canal.DummyEventHandler
	n int
}

func (h *countingRowHandler) OnRow(_ *canal.RowsEvent) error {
	h.n++
	return nil
}

func (h *countingRowHandler) String() string { return "countingRowHandler" }

func TestWrapHandlerWithWatermark_OnRow(t *testing.T) {
	t.Parallel()
	wmTbl := &schema.Table{
		Schema: "dblog",
		Name:   "wm",
		Columns: []schema.TableColumn{
			{Name: "id", Type: schema.TYPE_NUMBER},
			{Name: "watermark_value", Type: schema.TYPE_STRING},
		},
	}
	otherTbl := &schema.Table{
		Schema: "app",
		Name:   "users",
		Columns: []schema.TableColumn{
			{Name: "id", Type: schema.TYPE_NUMBER},
		},
	}
	cfg := &WatermarkTableConfig{TableKey: "dblog.wm"}

	tests := []struct {
		name          string
		notify        WatermarkNotifier
		forward       bool
		event         *canal.RowsEvent
		wantNotify    int
		wantInnerRows int
		notifyErr     error
		wantErr       bool
	}{
		{
			name:   "nil_notify_no_wrap",
			notify: nil,
			event: &canal.RowsEvent{
				Table: wmTbl, Action: canal.InsertAction,
				Rows: [][]interface{}{{int64(1), "a"}},
			},
			wantInnerRows: 1,
		},
		{
			name: "notify_consumes_watermark",
			notify: func(WatermarkBinlogEvent) error {
				return nil
			},
			forward: false,
			event: &canal.RowsEvent{
				Table: wmTbl, Action: canal.InsertAction,
				Rows: [][]interface{}{{int64(1), "w"}},
			},
			wantNotify:    1,
			wantInnerRows: 0,
		},
		{
			name: "notify_and_forward",
			notify: func(WatermarkBinlogEvent) error {
				return nil
			},
			forward: true,
			event: &canal.RowsEvent{
				Table: wmTbl, Action: canal.InsertAction,
				Rows: [][]interface{}{{int64(1), "w"}},
			},
			wantNotify:    1,
			wantInnerRows: 1,
		},
		{
			name: "other_table_passthrough",
			notify: func(WatermarkBinlogEvent) error {
				return nil
			},
			forward: false,
			event: &canal.RowsEvent{
				Table: otherTbl, Action: canal.InsertAction,
				Rows: [][]interface{}{{int64(9)}},
			},
			wantInnerRows: 1,
		},
		{
			name: "notify_error",
			notify: func(WatermarkBinlogEvent) error {
				return errors.New("boom")
			},
			event: &canal.RowsEvent{
				Table: wmTbl, Action: canal.InsertAction,
				Rows: [][]interface{}{{int64(1), "w"}},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			inner := &countingRowHandler{}
			var notifyCount int
			var wrapped canal.EventHandler = inner
			if tt.notify != nil {
				track := func(ev WatermarkBinlogEvent) error {
					notifyCount++
					return tt.notify(ev)
				}
				wrapped = wrapHandlerWithWatermark(inner, cfg, track, tt.forward)
			}
			err := wrapped.OnRow(tt.event)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if notifyCount != tt.wantNotify {
				t.Fatalf("notifyCount=%d want %d", notifyCount, tt.wantNotify)
			}
			if inner.n != tt.wantInnerRows {
				t.Fatalf("inner OnRow=%d want %d", inner.n, tt.wantInnerRows)
			}
		})
	}
}

func TestQuoteMySQLIdent(t *testing.T) {
	t.Parallel()
	bt := string(rune(0x60))
	tests := []struct {
		in, want string
	}{
		{"a", "`a`"},
		{bt + "x" + bt, bt + bt + bt + "x" + bt + bt + bt}, // `x` -> ```x```
		{"db_log", "`db_log`"},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.in, func(t *testing.T) {
			t.Parallel()
			if got := quoteMySQLIdent(tt.in); got != tt.want {
				t.Fatalf("got %q want %q", got, tt.want)
			}
		})
	}
}
