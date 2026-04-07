package tubing_cdc

import (
	"strings"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
)

type watermarkHandlerWrapper struct {
	inner    canal.EventHandler
	cfg      *WatermarkTableConfig
	notify   WatermarkNotifier
	forward  bool
	tableKey string
}

func wrapHandlerWithWatermark(inner canal.EventHandler, cfg *WatermarkTableConfig, notify WatermarkNotifier, forwardToInner bool) canal.EventHandler {
	if inner == nil || cfg == nil || notify == nil {
		return inner
	}
	key := strings.TrimSpace(cfg.TableKey)
	if key == "" {
		return inner
	}
	return &watermarkHandlerWrapper{
		inner:    inner,
		cfg:      cfg,
		notify:   notify,
		forward:  forwardToInner,
		tableKey: key,
	}
}

func (w *watermarkHandlerWrapper) OnRotate(header *replication.EventHeader, rotateEvent *replication.RotateEvent) error {
	return w.inner.OnRotate(header, rotateEvent)
}

func (w *watermarkHandlerWrapper) OnTableChanged(header *replication.EventHeader, schema, table string) error {
	return w.inner.OnTableChanged(header, schema, table)
}

func (w *watermarkHandlerWrapper) OnDDL(header *replication.EventHeader, nextPos mysql.Position, queryEvent *replication.QueryEvent) error {
	return w.inner.OnDDL(header, nextPos, queryEvent)
}

func (w *watermarkHandlerWrapper) OnRow(e *canal.RowsEvent) error {
	if e != nil && watermarkTableKey(e) == w.tableKey {
		ev, err := ParseWatermarkBinlogEvent(w.cfg, e)
		if err != nil {
			return err
		}
		if err := w.notify(ev); err != nil {
			return err
		}
		if !w.forward {
			return nil
		}
	}
	return w.inner.OnRow(e)
}

func (w *watermarkHandlerWrapper) OnXID(header *replication.EventHeader, nextPos mysql.Position) error {
	return w.inner.OnXID(header, nextPos)
}

func (w *watermarkHandlerWrapper) OnGTID(header *replication.EventHeader, gtid mysql.GTIDSet) error {
	return w.inner.OnGTID(header, gtid)
}

func (w *watermarkHandlerWrapper) OnPosSynced(header *replication.EventHeader, pos mysql.Position, set mysql.GTIDSet, force bool) error {
	return w.inner.OnPosSynced(header, pos, set, force)
}

func (w *watermarkHandlerWrapper) String() string {
	return w.inner.String() + "+Watermark"
}
