package tubing_cdc

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/go-mysql-org/go-mysql/schema"

	"github.com/go-mysql-org/go-mysql/canal"
)

// DefaultEnvelopeSchemaVersion is the JSON envelope schema_version for P0 (DBLog-aligned shape).
const DefaultEnvelopeSchemaVersion = "tubing-cdc-envelope-v0"

// EventOrigin distinguishes replication-log rows from future chunked snapshot rows (DBLog).
type EventOrigin string

const (
	OriginLog      EventOrigin = "log"
	OriginSnapshot EventOrigin = "snapshot"
)

// TableIdentity is a fully qualified MySQL table (database + table name).
type TableIdentity struct {
	Database string `json:"database"`
	Table    string `json:"table"`
}

// ParseTableIdentity splits "database.table" into TableIdentity. The table part may contain
// dots if the physical table name includes them (rare); callers should use the same FQN as
// Configs.Tables and RowEventSink tableKey.
func ParseTableIdentity(fqn string) (TableIdentity, error) {
	fqn = strings.TrimSpace(fqn)
	dot := strings.IndexByte(fqn, '.')
	if dot <= 0 || dot == len(fqn)-1 {
		return TableIdentity{}, fmt.Errorf("table identity: want database.table, got %q", fqn)
	}
	return TableIdentity{Database: fqn[:dot], Table: fqn[dot+1:]}, nil
}

// BinlogPosition is the MySQL analog to an LSN: binlog file plus byte offset. File may be
// omitted when unknown at emit time (e.g. only the replication event header offset is available).
type BinlogPosition struct {
	File string `json:"file,omitempty"`
	Pos  uint32 `json:"pos"`
}

// CDCEventEnvelope is the DBLog-style unified row event shape: same outer fields for log- and
// (future) snapshot-origin rows. Payload holds the existing canal-oriented JSON (row object or
// update before/after) for compatibility with current sinks and consumers.
type CDCEventEnvelope struct {
	SchemaVersion string            `json:"schema_version"`
	Origin        EventOrigin       `json:"origin"`
	Action        string            `json:"action"`
	Table         TableIdentity     `json:"table"`
	PrimaryKey    map[string]any    `json:"primary_key,omitempty"`
	Position      *BinlogPosition   `json:"position,omitempty"`
	Payload       json.RawMessage   `json:"payload"`
}

// JSONFieldNameForColumn matches DynamicTableEventHandler row JSON keys (MySQL column name, or
// "col" when the name is empty).
func JSONFieldNameForColumn(columnName string) string {
	if columnName == "" {
		return "col"
	}
	return columnName
}

// PrimaryKeyFromTableRow returns PK column names (JSON field names) to values from a row map.
// It uses schema.Table.PKColumns; if the table has no PK metadata, it returns an empty map.
func PrimaryKeyFromTableRow(tbl *schema.Table, row map[string]any) map[string]any {
	if tbl == nil || len(tbl.PKColumns) == 0 || row == nil {
		return map[string]any{}
	}
	out := make(map[string]any, len(tbl.PKColumns))
	for _, idx := range tbl.PKColumns {
		if idx < 0 || idx >= len(tbl.Columns) {
			continue
		}
		name := JSONFieldNameForColumn(tbl.Columns[idx].Name)
		out[name] = row[name]
	}
	return out
}

// PrimaryKeyFromResolvedPayload extracts the primary key from the same structure that was
// marshaled into envelope payload (single row map, or update with before/after).
func PrimaryKeyFromResolvedPayload(tbl *schema.Table, action string, resolved any) map[string]any {
	m, ok := resolved.(map[string]any)
	if !ok {
		return map[string]any{}
	}
	if action == canal.UpdateAction {
		if after, ok := m["after"].(map[string]any); ok {
			return PrimaryKeyFromTableRow(tbl, after)
		}
	}
	return PrimaryKeyFromTableRow(tbl, m)
}

// BinlogPositionFromSources builds an optional position for the envelope. If replicationPos is
// non-nil and has a file name or non-zero pos, that wins. Otherwise, if header is non-nil and
// LogPos is non-zero, Pos is taken from the event header (end offset of the event; file name
// still unknown until the syncer exposes it).
func BinlogPositionFromSources(replicationPos *mysql.Position, header *replication.EventHeader) *BinlogPosition {
	if replicationPos != nil && (replicationPos.Name != "" || replicationPos.Pos != 0) {
		return &BinlogPosition{File: replicationPos.Name, Pos: replicationPos.Pos}
	}
	if header != nil && header.LogPos != 0 {
		return &BinlogPosition{Pos: header.LogPos}
	}
	return nil
}

// MarshalCDCEventEnvelope builds JSON for one sink message. innerPayloadJSON must be the
// marshaled row payload (legacy shape). resolvedPayload is used only for primary_key extraction
// and should match innerPayloadJSON semantically (post–field-transform map).
func MarshalCDCEventEnvelope(
	schemaVersion string,
	origin EventOrigin,
	action, tableKey string,
	tbl *schema.Table,
	resolvedPayload any,
	innerPayloadJSON []byte,
	replicationPos *mysql.Position,
	header *replication.EventHeader,
) ([]byte, error) {
	if schemaVersion == "" {
		schemaVersion = DefaultEnvelopeSchemaVersion
	}
	id, err := ParseTableIdentity(tableKey)
	if err != nil {
		return nil, err
	}
	env := CDCEventEnvelope{
		SchemaVersion: schemaVersion,
		Origin:        origin,
		Action:        action,
		Table:         id,
		PrimaryKey:    PrimaryKeyFromResolvedPayload(tbl, action, resolvedPayload),
		Position:      BinlogPositionFromSources(replicationPos, header),
		Payload:       json.RawMessage(append([]byte(nil), innerPayloadJSON...)),
	}
	return json.Marshal(env)
}
