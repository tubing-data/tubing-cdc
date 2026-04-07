package tubing_cdc

import (
	"fmt"
	"strings"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/go-mysql-org/go-mysql/schema"
)

// DefaultWatermarkValueColumn is the DBLog-style UUID column name used in generated DDL and
// when WatermarkTableConfig.ValueColumn is empty.
const DefaultWatermarkValueColumn = "watermark_value"

// DefaultWatermarkPrimaryKeyColumn is the singleton primary key column in generated DDL.
const DefaultWatermarkPrimaryKeyColumn = "id"

// WatermarkTableConfig identifies the source watermark table (DBLog paper: dedicated namespace,
// single row, UUID field updated to place recognizable events in the transaction log).
type WatermarkTableConfig struct {
	// TableKey is fully qualified "database.table", same convention as Configs.Tables.
	TableKey string
	// ValueColumn holds the watermark UUID (or string) written for low/high steps. Empty uses
	// DefaultWatermarkValueColumn.
	ValueColumn string
	// PrimaryKeyColumn is only used by WatermarkCreateTableSQL. Empty uses DefaultWatermarkPrimaryKeyColumn.
	PrimaryKeyColumn string
}

func (c *WatermarkTableConfig) withDefaults() WatermarkTableConfig {
	if c == nil {
		return WatermarkTableConfig{
			ValueColumn:      DefaultWatermarkValueColumn,
			PrimaryKeyColumn: DefaultWatermarkPrimaryKeyColumn,
		}
	}
	out := *c
	if strings.TrimSpace(out.ValueColumn) == "" {
		out.ValueColumn = DefaultWatermarkValueColumn
	}
	if strings.TrimSpace(out.PrimaryKeyColumn) == "" {
		out.PrimaryKeyColumn = DefaultWatermarkPrimaryKeyColumn
	}
	return out
}

// Validate checks TableKey; does not require ValueColumn to be non-empty (defaults apply).
func (c *WatermarkTableConfig) Validate() error {
	if c == nil {
		return fmt.Errorf("watermark: config is nil")
	}
	_, err := ParseTableIdentity(strings.TrimSpace(c.TableKey))
	return err
}

// WatermarkBinlogEvent is a row change on the configured watermark table, mapped from canal's RowsEvent.
type WatermarkBinlogEvent struct {
	TableKey string
	Action   string
	// NewValue is the value column after insert/update; for delete it is empty.
	NewValue string
	// OldValue is the value column before update/delete; empty when unknown.
	OldValue string
	Header   *replication.EventHeader
}

// WatermarkNotifier is invoked for each watermark table row event when configured on Configs.
type WatermarkNotifier func(WatermarkBinlogEvent) error

func quoteMySQLIdent(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}

// WatermarkCreateTableSQL returns CREATE TABLE and seed INSERT matching the DBLog paper layout
// (singleton PK row + string UUID column). Operators run these on the source before enabling CDC.
func WatermarkCreateTableSQL(c *WatermarkTableConfig) (createSQL, insertSQL string, err error) {
	if c == nil {
		return "", "", fmt.Errorf("watermark: config is nil")
	}
	if err := c.Validate(); err != nil {
		return "", "", err
	}
	cfg := c.withDefaults()
	id, err := ParseTableIdentity(strings.TrimSpace(cfg.TableKey))
	if err != nil {
		return "", "", err
	}
	pk := quoteMySQLIdent(cfg.PrimaryKeyColumn)
	valCol := quoteMySQLIdent(cfg.ValueColumn)
	db := quoteMySQLIdent(id.Database)
	tbl := quoteMySQLIdent(id.Table)
	createSQL = fmt.Sprintf(
		"CREATE TABLE %s.%s (\n  %s TINYINT UNSIGNED NOT NULL PRIMARY KEY,\n  %s CHAR(36) NOT NULL\n) ENGINE=InnoDB",
		db, tbl, pk, valCol,
	)
	insertSQL = fmt.Sprintf(
		"INSERT INTO %s.%s (%s, %s) VALUES (1, '00000000-0000-0000-0000-000000000000')",
		db, tbl, pk, valCol,
	)
	return createSQL, insertSQL, nil
}

func watermarkTableKey(e *canal.RowsEvent) string {
	if e == nil || e.Table == nil {
		return ""
	}
	return tableFQN(e.Table.Schema, e.Table.Name)
}

func columnIndex(tbl *schema.Table, colName string) (int, bool) {
	if tbl == nil || colName == "" {
		return 0, false
	}
	for i, c := range tbl.Columns {
		if c.Name == colName {
			return i, true
		}
	}
	return 0, false
}

func cellToWatermarkString(v interface{}) string {
	switch x := v.(type) {
	case nil:
		return ""
	case string:
		return x
	case []byte:
		return string(x)
	default:
		return fmt.Sprint(x)
	}
}

// ParseWatermarkBinlogEvent extracts OldValue/NewValue from a canal RowsEvent for the watermark table.
// cfg must be non-nil; TableKey must match the event's table.
func ParseWatermarkBinlogEvent(cfg *WatermarkTableConfig, e *canal.RowsEvent) (WatermarkBinlogEvent, error) {
	if cfg == nil {
		return WatermarkBinlogEvent{}, fmt.Errorf("watermark: config is nil")
	}
	if err := cfg.Validate(); err != nil {
		return WatermarkBinlogEvent{}, err
	}
	if e == nil || e.Table == nil {
		return WatermarkBinlogEvent{}, fmt.Errorf("watermark: nil rows event")
	}
	key := watermarkTableKey(e)
	if key != strings.TrimSpace(cfg.TableKey) {
		return WatermarkBinlogEvent{}, fmt.Errorf("watermark: event table %q does not match config %q", key, cfg.TableKey)
	}
	wc := cfg.withDefaults()
	idx, ok := columnIndex(e.Table, wc.ValueColumn)
	if !ok {
		return WatermarkBinlogEvent{}, fmt.Errorf("watermark: table %s has no column %q", key, wc.ValueColumn)
	}
	ev := WatermarkBinlogEvent{TableKey: key, Action: e.Action, Header: e.Header}
	switch e.Action {
	case canal.InsertAction:
		if len(e.Rows) < 1 {
			return WatermarkBinlogEvent{}, fmt.Errorf("watermark: insert with no rows")
		}
		if idx < len(e.Rows[0]) {
			ev.NewValue = cellToWatermarkString(e.Rows[0][idx])
		}
	case canal.DeleteAction:
		if len(e.Rows) < 1 {
			return WatermarkBinlogEvent{}, fmt.Errorf("watermark: delete with no rows")
		}
		if idx < len(e.Rows[0]) {
			ev.OldValue = cellToWatermarkString(e.Rows[0][idx])
		}
	case canal.UpdateAction:
		if len(e.Rows) < 2 {
			return WatermarkBinlogEvent{}, fmt.Errorf("watermark: update needs before/after row pair")
		}
		before, after := e.Rows[0], e.Rows[1]
		if idx < len(before) {
			ev.OldValue = cellToWatermarkString(before[idx])
		}
		if idx < len(after) {
			ev.NewValue = cellToWatermarkString(after[idx])
		}
	default:
		return WatermarkBinlogEvent{}, fmt.Errorf("watermark: unsupported action %q", e.Action)
	}
	return ev, nil
}
