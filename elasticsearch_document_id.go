package tubing_cdc

import (
	"encoding/json"
	"fmt"
	"strings"
)

// ElasticsearchDocumentIDPart is one ordered fragment of the Elasticsearch document _id. Build parts
// with ElasticsearchDocumentIDLiteral, ElasticsearchDocumentIDField, ElasticsearchDocumentIDTableKey,
// and ElasticsearchDocumentIDAction, then pass them to JoinElasticsearchDocumentID.
type ElasticsearchDocumentIDPart struct {
	kind          esDocIDPartKind
	pathOrLiteral string
}

type esDocIDPartKind uint8

const (
	esDocIDPartLiteral esDocIDPartKind = iota
	esDocIDPartJSONPath
	esDocIDPartTableKey
	esDocIDPartAction
)

// ElasticsearchDocumentIDLiteral appends fixed text (for example a tenant slug or environment prefix).
func ElasticsearchDocumentIDLiteral(text string) ElasticsearchDocumentIDPart {
	return ElasticsearchDocumentIDPart{kind: esDocIDPartLiteral, pathOrLiteral: text}
}

// ElasticsearchDocumentIDField reads one value from the CDC JSON payload using a dotted path from the
// root object (e.g. "id", "sku", "after.id", "before.tenant_id"). The value is formatted with fmt.Sprint.
// If the path is missing or null, the whole document id is considered unavailable (false).
func ElasticsearchDocumentIDField(path string) ElasticsearchDocumentIDPart {
	return ElasticsearchDocumentIDPart{kind: esDocIDPartJSONPath, pathOrLiteral: path}
}

// ElasticsearchDocumentIDTableKey appends the fully qualified table key passed to Emit (e.g. "db.orders").
func ElasticsearchDocumentIDTableKey() ElasticsearchDocumentIDPart {
	return ElasticsearchDocumentIDPart{kind: esDocIDPartTableKey}
}

// ElasticsearchDocumentIDAction appends the canal action name passed to Emit (insert, update, delete, ...).
func ElasticsearchDocumentIDAction() ElasticsearchDocumentIDPart {
	return ElasticsearchDocumentIDPart{kind: esDocIDPartAction}
}

// JoinElasticsearchDocumentID returns a callback suitable for ElasticsearchSinkConfig.DocumentID. Fragments
// are concatenated in order with separator between each pair. JSON field lookups use the raw CDC payload
// (the same bytes passed to Emit), so update events can reference "after.id" or "before.id" as needed.
func JoinElasticsearchDocumentID(separator string, parts ...ElasticsearchDocumentIDPart) (func(tableKey, action string, payloadJSON []byte) (string, bool), error) {
	if len(parts) == 0 {
		return nil, fmt.Errorf("elasticsearch document id: at least one part is required")
	}
	for i := range parts {
		if parts[i].kind == esDocIDPartJSONPath && strings.TrimSpace(parts[i].pathOrLiteral) == "" {
			return nil, fmt.Errorf("elasticsearch document id: field path must be non-empty")
		}
	}
	return func(tableKey, action string, payloadJSON []byte) (string, bool) {
		var root any
		if err := json.Unmarshal(payloadJSON, &root); err != nil {
			return "", false
		}
		var b strings.Builder
		for i := range parts {
			if i > 0 {
				b.WriteString(separator)
			}
			switch parts[i].kind {
			case esDocIDPartLiteral:
				b.WriteString(parts[i].pathOrLiteral)
			case esDocIDPartJSONPath:
				v, ok := jsonPathGet(root, parts[i].pathOrLiteral)
				if !ok || v == nil {
					return "", false
				}
				b.WriteString(fmt.Sprint(v))
			case esDocIDPartTableKey:
				b.WriteString(tableKey)
			case esDocIDPartAction:
				b.WriteString(action)
			}
		}
		out := b.String()
		if out == "" {
			return "", false
		}
		return out, true
	}, nil
}

func jsonPathGet(root any, path string) (any, bool) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, false
	}
	cur := root
	for _, p := range strings.Split(path, ".") {
		p = strings.TrimSpace(p)
		if p == "" {
			return nil, false
		}
		m, ok := cur.(map[string]any)
		if !ok {
			return nil, false
		}
		cur, ok = m[p]
		if !ok {
			return nil, false
		}
	}
	return cur, true
}
