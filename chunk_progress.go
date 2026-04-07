package tubing_cdc

import (
	"fmt"
	"strings"
)

const (
	// DefaultChunkProgressRunID is stored when ChunkProgressRecord.RunID is empty.
	DefaultChunkProgressRunID = "default"
	// DefaultChunkProgressKeyPrefix is the Badger key prefix namespace for chunk state (separate from binlog position keys).
	DefaultChunkProgressKeyPrefix = "v1/chunk-progress/"
)

// ChunkProgressRecord is restart-safe cursor state for primary-key-ordered full-state chunks (DBLog-style).
// AfterPK holds the last primary key from the previous chunk; the next chunk query uses rows strictly greater
// than this tuple in PK column order. Empty AfterPK means start from the beginning of the table.
type ChunkProgressRecord struct {
	TableKey  string `json:"table_key"`
	RunID     string `json:"run_id,omitempty"`
	ChunkSize int    `json:"chunk_size"`
	AfterPK   []any  `json:"after_pk,omitempty"`
}

func (r ChunkProgressRecord) normalizedRunID() string {
	if strings.TrimSpace(r.RunID) == "" {
		return DefaultChunkProgressRunID
	}
	return strings.TrimSpace(r.RunID)
}

// Validate checks table key and chunk size. It does not require AfterPK to match a particular PK width;
// callers building SQL should ensure len(AfterPK) is 0 or equals len(pkColumns).
func (r ChunkProgressRecord) Validate() error {
	if strings.TrimSpace(r.TableKey) == "" {
		return fmt.Errorf("chunk progress: table_key is empty")
	}
	if _, err := ParseTableIdentity(strings.TrimSpace(r.TableKey)); err != nil {
		return fmt.Errorf("chunk progress: %w", err)
	}
	if r.ChunkSize <= 0 {
		return fmt.Errorf("chunk progress: chunk_size must be positive")
	}
	return nil
}

// BuildPKOrderedChunkSelect returns a MySQL SELECT ordered by primary key with optional strict lower bound.
// pkColumns must be non-empty and list PK columns in ascending order used for chunking. afterPK must be nil/empty
// for the first chunk, or have the same length as pkColumns for subsequent chunks.
func BuildPKOrderedChunkSelect(
	database, table string,
	pkColumns []string,
	chunkSize int,
	afterPK []any,
) (query string, args []any, err error) {
	database = strings.TrimSpace(database)
	table = strings.TrimSpace(table)
	if database == "" || table == "" {
		return "", nil, fmt.Errorf("chunk select: database and table are required")
	}
	if len(pkColumns) == 0 {
		return "", nil, fmt.Errorf("chunk select: pkColumns is empty")
	}
	for _, c := range pkColumns {
		if strings.TrimSpace(c) == "" {
			return "", nil, fmt.Errorf("chunk select: empty pk column name")
		}
	}
	if chunkSize <= 0 {
		return "", nil, fmt.Errorf("chunk select: chunkSize must be positive")
	}
	if len(afterPK) != 0 && len(afterPK) != len(pkColumns) {
		return "", nil, fmt.Errorf("chunk select: afterPK length %d must equal pkColumns length %d", len(afterPK), len(pkColumns))
	}

	quotedDB := quoteMySQLIdent(database)
	quotedTbl := quoteMySQLIdent(table)
	quotedPK := make([]string, len(pkColumns))
	for i, c := range pkColumns {
		quotedPK[i] = quoteMySQLIdent(strings.TrimSpace(c))
	}
	orderBy := strings.Join(quotedPK, ", ")

	fqn := quotedDB + "." + quotedTbl
	var sb strings.Builder
	sb.WriteString("SELECT * FROM ")
	sb.WriteString(fqn)

	if len(afterPK) > 0 {
		sb.WriteString(" WHERE (")
		sb.WriteString(orderBy)
		sb.WriteString(") > (")
		sb.WriteString(strings.TrimRight(strings.Repeat("?,", len(pkColumns)), ","))
		sb.WriteString(")")
	}

	sb.WriteString(" ORDER BY ")
	sb.WriteString(orderBy)
	sb.WriteString(" LIMIT ?")

	query = sb.String()
	args = make([]any, 0, len(afterPK)+1)
	args = append(args, afterPK...)
	args = append(args, chunkSize)
	return query, args, nil
}

// BuildPKRowsInSelect returns a MySQL SELECT that fetches rows whose primary key tuple is in pkRows.
// pkRows must be non-empty; each row must have the same length as pkColumns.
func BuildPKRowsInSelect(
	database, table string,
	pkColumns []string,
	pkRows [][]any,
) (query string, args []any, err error) {
	database = strings.TrimSpace(database)
	table = strings.TrimSpace(table)
	if database == "" || table == "" {
		return "", nil, fmt.Errorf("chunk select: database and table are required")
	}
	if len(pkColumns) == 0 {
		return "", nil, fmt.Errorf("chunk select: pkColumns is empty")
	}
	for _, c := range pkColumns {
		if strings.TrimSpace(c) == "" {
			return "", nil, fmt.Errorf("chunk select: empty pk column name")
		}
	}
	if len(pkRows) == 0 {
		return "", nil, fmt.Errorf("chunk select: pkRows is empty")
	}
	for i, row := range pkRows {
		if len(row) != len(pkColumns) {
			return "", nil, fmt.Errorf("chunk select: pkRows[%d] length %d must equal pkColumns length %d", i, len(row), len(pkColumns))
		}
	}

	quotedDB := quoteMySQLIdent(database)
	quotedTbl := quoteMySQLIdent(table)
	quotedPK := make([]string, len(pkColumns))
	for i, c := range pkColumns {
		quotedPK[i] = quoteMySQLIdent(strings.TrimSpace(c))
	}
	orderBy := strings.Join(quotedPK, ", ")
	fqn := quotedDB + "." + quotedTbl

	var sb strings.Builder
	sb.WriteString("SELECT * FROM ")
	sb.WriteString(fqn)
	sb.WriteString(" WHERE (")
	sb.WriteString(orderBy)
	sb.WriteString(") IN (")
	for r := range pkRows {
		if r > 0 {
			sb.WriteString(", ")
		}
		sb.WriteByte('(')
		sb.WriteString(strings.TrimRight(strings.Repeat("?,", len(pkColumns)), ","))
		sb.WriteByte(')')
	}
	sb.WriteByte(')')

	query = sb.String()
	args = make([]any, 0, len(pkRows)*len(pkColumns))
	for _, row := range pkRows {
		args = append(args, row...)
	}
	return query, args, nil
}
