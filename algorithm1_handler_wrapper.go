package tubing_cdc

import (
	"strings"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/go-mysql-org/go-mysql/schema"
)

type algorithm1HandlerWrapper struct {
	inner          canal.EventHandler
	tracker        *Algorithm1Tracker
	targetTableKey string
}

func wrapHandlerWithAlgorithm1(inner canal.EventHandler, tracker *Algorithm1Tracker, targetTableKey string) canal.EventHandler {
	if inner == nil || tracker == nil {
		return inner
	}
	tk := strings.TrimSpace(targetTableKey)
	return &algorithm1HandlerWrapper{
		inner:          inner,
		tracker:        tracker,
		targetTableKey: tk,
	}
}

func (w *algorithm1HandlerWrapper) OnRotate(header *replication.EventHeader, rotateEvent *replication.RotateEvent) error {
	return w.inner.OnRotate(header, rotateEvent)
}

func (w *algorithm1HandlerWrapper) OnTableChanged(header *replication.EventHeader, schema, table string) error {
	return w.inner.OnTableChanged(header, schema, table)
}

func (w *algorithm1HandlerWrapper) OnDDL(header *replication.EventHeader, nextPos mysql.Position, queryEvent *replication.QueryEvent) error {
	return w.inner.OnDDL(header, nextPos, queryEvent)
}

func (w *algorithm1HandlerWrapper) OnRow(e *canal.RowsEvent) error {
	if e != nil && e.Table != nil {
		tk := tableFQN(e.Table.Schema, e.Table.Name)
		if w.targetTableKey == "" || tk == w.targetTableKey {
			w.recordRowEvent(e)
		}
	}
	return w.inner.OnRow(e)
}

func (w *algorithm1HandlerWrapper) OnXID(header *replication.EventHeader, nextPos mysql.Position) error {
	return w.inner.OnXID(header, nextPos)
}

func (w *algorithm1HandlerWrapper) OnGTID(header *replication.EventHeader, gtid mysql.GTIDSet) error {
	return w.inner.OnGTID(header, gtid)
}

func (w *algorithm1HandlerWrapper) OnPosSynced(header *replication.EventHeader, pos mysql.Position, set mysql.GTIDSet, force bool) error {
	return w.inner.OnPosSynced(header, pos, set, force)
}

func (w *algorithm1HandlerWrapper) String() string {
	return w.inner.String() + "+Algorithm1"
}

func (w *algorithm1HandlerWrapper) recordRowEvent(e *canal.RowsEvent) {
	tbl := e.Table
	switch e.Action {
	case canal.InsertAction:
		for _, row := range e.Rows {
			m := rowMapFromCanalRow(tbl, row)
			w.tracker.RecordTargetRowChange(tableFQN(tbl.Schema, tbl.Name), m)
		}
	case canal.DeleteAction:
		for _, row := range e.Rows {
			m := rowMapFromCanalRow(tbl, row)
			w.tracker.RecordTargetRowChange(tableFQN(tbl.Schema, tbl.Name), m)
		}
	case canal.UpdateAction:
		tk := tableFQN(tbl.Schema, tbl.Name)
		for i := 0; i+1 < len(e.Rows); i += 2 {
			before := rowMapFromCanalRow(tbl, e.Rows[i])
			after := rowMapFromCanalRow(tbl, e.Rows[i+1])
			w.tracker.RecordTargetRowChange(tk, before)
			w.tracker.RecordTargetRowChange(tk, after)
		}
	default:
		return
	}
}

func rowMapFromCanalRow(tbl *schema.Table, row []interface{}) map[string]any {
	if tbl == nil {
		return map[string]any{}
	}
	out := make(map[string]any, len(tbl.Columns))
	for i, col := range tbl.Columns {
		if i < len(row) {
			out[JSONFieldNameForColumn(col.Name)] = row[i]
		}
	}
	return out
}
