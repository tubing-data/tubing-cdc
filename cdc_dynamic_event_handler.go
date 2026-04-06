package tubing_cdc

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"unicode"
	"unicode/utf8"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/go-mysql-org/go-mysql/schema"
	"github.com/siddontang/go-log/log"
)

// DynamicTableEventHandler maps each watched table's binlog row to map[string]any using canal
// column order and names (JSON keys match MySQL column names). It prints a Go struct snippet
// once per table for documentation; runtime rows avoid reflect.StructOf per event.
type DynamicTableEventHandler struct {
	canal.DummyEventHandler

	allow map[string]struct{}
	sink  RowEventSink

	fieldRules []RowFieldTransformRule

	mu            sync.Mutex
	rowLayout     map[string]rowColumnLayout
	printedSource map[string]bool
}

// rowColumnLayout holds per-table JSON field names in column order (aligned with canal row slices).
type rowColumnLayout struct {
	jsonKeys []string
}

// RowFieldTransformRule applies a custom function to one source column (JSON key matches the
// MySQL column name as emitted by the handler) and merges the returned map into the row map
// sent to the sink. Later rules overwrite earlier values when keys collide; transform output
// overwrites existing row keys with the same name.
type RowFieldTransformRule struct {
	TableKey     string
	SourceColumn string
	Transform    func(tableKey, action string, value any) map[string]any
}

// DynamicHandlerOption configures NewDynamicTableEventHandler.
type DynamicHandlerOption func(*DynamicTableEventHandler)

// WithRowEventSink sets where row JSON is delivered. When unset, LoggerRowSink is used.
func WithRowEventSink(s RowEventSink) DynamicHandlerOption {
	return func(h *DynamicTableEventHandler) {
		h.sink = s
	}
}

// WithRowFieldTransformRules registers destination-side field transforms. TableKey empty means
// the rule applies to every table. SourceColumn is the JSON field name (same as the column name
// in the default mapping). Transform receives the current cell value; returned entries are merged
// into the outgoing row, overwriting keys that already exist.
func WithRowFieldTransformRules(rules ...RowFieldTransformRule) DynamicHandlerOption {
	return func(h *DynamicTableEventHandler) {
		h.fieldRules = append(h.fieldRules, rules...)
	}
}

// NewDynamicTableEventHandler returns a handler that maps CDC rows into a per-table runtime
// struct type. If registeredTables is empty, all tables seen in OnRow are handled; otherwise
// only fully-qualified names "database.table" listed here are processed.
func NewDynamicTableEventHandler(registeredTables []string, opts ...DynamicHandlerOption) *DynamicTableEventHandler {
	allow := make(map[string]struct{}, len(registeredTables))
	for _, t := range registeredTables {
		t = strings.TrimSpace(t)
		if t == "" {
			continue
		}
		allow[t] = struct{}{}
	}
	h := &DynamicTableEventHandler{
		allow:         allow,
		rowLayout:     make(map[string]rowColumnLayout),
		printedSource: make(map[string]bool),
	}
	for _, o := range opts {
		o(h)
	}
	if h.sink == nil {
		h.sink = LoggerRowSink{}
	}
	return h
}

func (h *DynamicTableEventHandler) String() string {
	return "DynamicTableEventHandler"
}

func (h *DynamicTableEventHandler) OnTableChanged(_ *replication.EventHeader, schema, table string) error {
	key := tableFQN(schema, table)
	h.mu.Lock()
	delete(h.rowLayout, key)
	delete(h.printedSource, key)
	h.mu.Unlock()
	log.Infof("[CDC] table changed, cleared dynamic struct cache: %s", key)
	return nil
}

func (h *DynamicTableEventHandler) OnRow(e *canal.RowsEvent) error {
	if e == nil || e.Table == nil {
		return nil
	}
	key := tableFQN(e.Table.Schema, e.Table.Name)
	if !h.allows(key) {
		return nil
	}

	h.mu.Lock()
	layout, ok := h.rowLayout[key]
	if !ok {
		layout = rowColumnLayoutFromTable(e.Table)
		h.rowLayout[key] = layout
	}
	if !h.printedSource[key] {
		h.printedSource[key] = true
		log.Infof("[CDC] generated Go row struct for %s:\n%s", key, generateStructGoSource(e.Table))
	}
	h.mu.Unlock()

	switch e.Action {
	case canal.UpdateAction:
		for i := 0; i+1 < len(e.Rows); i += 2 {
			h.logRowJSON(key, e.Action, map[string]any{
				"before": rowValuesToMap(layout, e.Rows[i]),
				"after":  rowValuesToMap(layout, e.Rows[i+1]),
			})
		}
	default:
		for _, row := range e.Rows {
			h.logRowJSON(key, e.Action, rowValuesToMap(layout, row))
		}
	}
	return nil
}

func (h *DynamicTableEventHandler) allows(key string) bool {
	if len(h.allow) == 0 {
		return true
	}
	_, ok := h.allow[key]
	return ok
}

func (h *DynamicTableEventHandler) logRowJSON(tableKey, action string, payload any) {
	var b []byte
	var err error
	if len(h.fieldRules) == 0 {
		b, err = json.Marshal(payload)
	} else {
		out, prepErr := h.applyFieldRulesToPayload(tableKey, action, payload)
		if prepErr != nil {
			log.Infof("[CDC] %s %s field transform error: %v payload=%#v", action, tableKey, prepErr, payload)
			return
		}
		b, err = json.Marshal(out)
	}
	if err != nil {
		log.Infof("[CDC] %s %s marshal error: %v payload=%#v", action, tableKey, err, payload)
		return
	}
	if h.sink == nil {
		h.sink = LoggerRowSink{}
	}
	if err := h.sink.Emit(tableKey, action, b); err != nil {
		log.Infof("[CDC] %s %s sink error: %v", action, tableKey, err)
	}
}

func (h *DynamicTableEventHandler) applyFieldRulesToPayload(tableKey, action string, payload any) (any, error) {
	if m, ok := payload.(map[string]any); ok {
		if _, hasBefore := m["before"]; hasBefore {
			if _, hasAfter := m["after"]; hasAfter {
				beforeMap, err := h.applyFieldRulesToSingleRow(tableKey, action, m["before"])
				if err != nil {
					return nil, err
				}
				afterMap, err := h.applyFieldRulesToSingleRow(tableKey, action, m["after"])
				if err != nil {
					return nil, err
				}
				return map[string]any{"before": beforeMap, "after": afterMap}, nil
			}
		}
	}
	return h.applyFieldRulesToSingleRow(tableKey, action, payload)
}

func (h *DynamicTableEventHandler) applyFieldRulesToSingleRow(tableKey, action string, row any) (map[string]any, error) {
	if m, ok := row.(map[string]any); ok {
		h.applyFieldRules(tableKey, action, m)
		return m, nil
	}
	m := dynamicRowStructToMap(row)
	if m == nil {
		b, err := json.Marshal(row)
		if err != nil {
			return nil, err
		}
		var mm map[string]any
		if err := json.Unmarshal(b, &mm); err != nil {
			return nil, err
		}
		m = mm
	}
	h.applyFieldRules(tableKey, action, m)
	return m, nil
}

func (h *DynamicTableEventHandler) applyFieldRules(tableKey, action string, row map[string]any) {
	for _, rule := range h.fieldRules {
		if rule.TableKey != "" && rule.TableKey != tableKey {
			continue
		}
		if rule.SourceColumn == "" || rule.Transform == nil {
			continue
		}
		val, ok := row[rule.SourceColumn]
		if !ok {
			continue
		}
		for k, v := range rule.Transform(tableKey, action, val) {
			row[k] = v
		}
	}
}

func dynamicRowStructToMap(v any) map[string]any {
	if v == nil {
		return nil
	}
	rv := reflect.ValueOf(v)
	if rv.Kind() == reflect.Ptr {
		if rv.IsNil() {
			return nil
		}
		rv = rv.Elem()
	}
	if rv.Kind() != reflect.Struct {
		return nil
	}
	rt := rv.Type()
	out := make(map[string]any, rt.NumField())
	for i := 0; i < rt.NumField(); i++ {
		sf := rt.Field(i)
		name := strings.Split(sf.Tag.Get("json"), ",")[0]
		if name == "" || name == "-" {
			continue
		}
		fv := rv.Field(i)
		if !fv.CanInterface() {
			continue
		}
		out[name] = fv.Interface()
	}
	return out
}

func tableFQN(schemaName, table string) string {
	return schemaName + "." + table
}

func rowColumnLayoutFromTable(t *schema.Table) rowColumnLayout {
	if t == nil || len(t.Columns) == 0 {
		return rowColumnLayout{}
	}
	keys := make([]string, len(t.Columns))
	for i, c := range t.Columns {
		keys[i] = jsonNameFromColumn(c.Name)
	}
	return rowColumnLayout{jsonKeys: keys}
}

// rowValuesToMap builds one JSON object per binlog row without reflection. Keys match
// jsonNameFromColumn(c.Name); nil cells and missing trailing values become JSON null like the
// previous reflect.StructOf + any-field representation.
func rowValuesToMap(layout rowColumnLayout, vals []interface{}) map[string]any {
	n := len(layout.jsonKeys)
	if n == 0 {
		return map[string]any{}
	}
	out := make(map[string]any, n)
	for i := 0; i < n; i++ {
		var v any
		if i < len(vals) {
			v = vals[i]
		}
		out[layout.jsonKeys[i]] = v
	}
	return out
}

func jsonNameFromColumn(col string) string {
	if col == "" {
		return "col"
	}
	return col
}

func uniqueExportedFieldNames(columns []schema.TableColumn) []string {
	seen := make(map[string]int, len(columns))
	out := make([]string, len(columns))
	for i, c := range columns {
		base := snakeToExportedGoName(c.Name)
		n := seen[base]
		seen[base]++
		if n > 0 {
			out[i] = fmt.Sprintf("%s_%d", base, n+1)
		} else {
			out[i] = base
		}
	}
	return out
}

func snakeToExportedGoName(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return "Col"
	}
	var b strings.Builder
	start := 0
	for i := 0; i <= len(s); i++ {
		if i < len(s) && s[i] != '_' && s[i] != '-' && s[i] != ' ' {
			continue
		}
		if start < i {
			part := s[start:i]
			r0, w := utf8.DecodeRuneInString(part)
			if r0 == utf8.RuneError {
				b.WriteString("X")
			} else {
				b.WriteRune(unicode.ToUpper(r0))
				if w < len(part) {
					b.WriteString(strings.ToLower(part[w:]))
				}
			}
		}
		start = i + 1
	}
	name := b.String()
	if name == "" {
		return "Col"
	}
	r0, _ := utf8.DecodeRuneInString(name)
	if r0 != utf8.RuneError && !unicode.IsLetter(r0) {
		name = "X" + name
	}
	return name
}

func exportedTableRowTypeName(tbl *schema.Table) string {
	return snakeToExportedGoName(tbl.Schema) + snakeToExportedGoName(tbl.Name) + "Row"
}

func generateStructGoSource(tbl *schema.Table) string {
	typeName := exportedTableRowTypeName(tbl)
	names := uniqueExportedFieldNames(tbl.Columns)
	var b strings.Builder
	b.WriteString(fmt.Sprintf("// %s approximates binlog row shape for %s.%s (fields are any at runtime).\n",
		typeName, tbl.Schema, tbl.Name))
	b.WriteString(fmt.Sprintf("type %s struct {\n", typeName))
	for i, col := range tbl.Columns {
		goType := schemaColumnToGoTypeString(col)
		tag := fmt.Sprintf("`mysql:\"%s\" json:\"%s\"`", col.Name, jsonNameFromColumn(col.Name))
		b.WriteString(fmt.Sprintf("\t%s %s %s\n", names[i], goType, tag))
	}
	b.WriteString("}\n")
	return b.String()
}

func schemaColumnToGoTypeString(c schema.TableColumn) string {
	switch c.Type {
	case schema.TYPE_NUMBER:
		if c.IsUnsigned {
			return "uint64"
		}
		return "int64"
	case schema.TYPE_FLOAT:
		return "float64"
	case schema.TYPE_DECIMAL:
		return "string"
	case schema.TYPE_ENUM, schema.TYPE_SET:
		return "string"
	case schema.TYPE_STRING:
		return "string"
	case schema.TYPE_DATETIME, schema.TYPE_TIMESTAMP, schema.TYPE_DATE, schema.TYPE_TIME:
		return "time.Time"
	case schema.TYPE_JSON:
		return "json.RawMessage"
	case schema.TYPE_BIT, schema.TYPE_BINARY:
		return "[]byte"
	case schema.TYPE_POINT:
		return "string"
	case schema.TYPE_MEDIUM_INT:
		if c.IsUnsigned {
			return "uint32"
		}
		return "int32"
	default:
		return "any"
	}
}
