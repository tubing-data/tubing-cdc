package tubing_cdc

import (
	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
)

type positionHandlerWrapper struct {
	inner canal.EventHandler
	store *twoTierPositionStore
}

func wrapHandlerWithPositionStore(inner canal.EventHandler, store *twoTierPositionStore) canal.EventHandler {
	if store == nil {
		return inner
	}
	return &positionHandlerWrapper{inner: inner, store: store}
}

func (w *positionHandlerWrapper) OnRotate(header *replication.EventHeader, rotateEvent *replication.RotateEvent) error {
	return w.inner.OnRotate(header, rotateEvent)
}

func (w *positionHandlerWrapper) OnTableChanged(header *replication.EventHeader, schema, table string) error {
	return w.inner.OnTableChanged(header, schema, table)
}

func (w *positionHandlerWrapper) OnDDL(header *replication.EventHeader, nextPos mysql.Position, queryEvent *replication.QueryEvent) error {
	return w.inner.OnDDL(header, nextPos, queryEvent)
}

func (w *positionHandlerWrapper) OnRow(e *canal.RowsEvent) error {
	return w.inner.OnRow(e)
}

func (w *positionHandlerWrapper) OnXID(header *replication.EventHeader, nextPos mysql.Position) error {
	return w.inner.OnXID(header, nextPos)
}

func (w *positionHandlerWrapper) OnGTID(header *replication.EventHeader, gtid mysql.GTIDSet) error {
	return w.inner.OnGTID(header, gtid)
}

func (w *positionHandlerWrapper) OnPosSynced(header *replication.EventHeader, pos mysql.Position, set mysql.GTIDSet, force bool) error {
	if err := w.inner.OnPosSynced(header, pos, set, force); err != nil {
		return err
	}
	return w.store.onCommitted(pos, set)
}

func (w *positionHandlerWrapper) String() string {
	return w.inner.String() + "+PositionPersistence"
}
