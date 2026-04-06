package tubing_cdc

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// ElasticsearchRowEventSink indexes each CDC row as a JSON document via the Elasticsearch HTTP API.
// Document body is {"cdc_table","cdc_action","data": <original payload>}. When a document id can be
// derived (default: top-level "id", "_id", or "pk"; for updates, "after" then "before"), insert/update
// use PUT .../_doc/{id}. Otherwise POST .../_doc is used (Elasticsearch assigns _id). Delete requires
// a derivable id.
type ElasticsearchRowEventSink struct {
	baseURL       string
	defaultIndex  string
	indexResolver func(tableKey string) string
	client        *http.Client
	refreshParam  string
	getDocumentID func(tableKey, action string, payload []byte) (string, bool)
	basicUser     string
	basicPass     string
	apiKey        string
}

// ElasticsearchSinkConfig holds cluster address and index routing for NewElasticsearchRowEventSink.
type ElasticsearchSinkConfig struct {
	Addresses []string
	// Index is used when IndexResolver is nil; required in that case.
	Index string
	// IndexResolver, when set, maps fully-qualified table key "db.table" to an index name (sanitized).
	IndexResolver func(tableKey string) string
	Username string
	Password string
	// APIKey is sent as Authorization: ApiKey <value> (Elastic Cloud style). Ignored if empty.
	APIKey string
	// Refresh is passed as the refresh query parameter when non-empty (e.g. "true", "wait_for").
	Refresh string
	HTTPClient *http.Client
	// DocumentID overrides default id extraction from payloadJSON. Use JoinElasticsearchDocumentID to build
	// an id from literals, JSON field paths (e.g. "after.id"), table key, and action.
	DocumentID func(tableKey, action string, payloadJSON []byte) (string, bool)
}

// NewElasticsearchRowEventSink builds a sink. Only the first entry in Addresses is used. Callers should
// use a load balancer URL or a single coordinating node when high availability is required.
func NewElasticsearchRowEventSink(cfg ElasticsearchSinkConfig) (*ElasticsearchRowEventSink, error) {
	if len(cfg.Addresses) == 0 {
		return nil, fmt.Errorf("elasticsearch sink: at least one address is required")
	}
	base := normalizeElasticsearchBaseURL(cfg.Addresses[0])
	if base == "" {
		return nil, fmt.Errorf("elasticsearch sink: invalid address")
	}
	if cfg.IndexResolver == nil && strings.TrimSpace(cfg.Index) == "" {
		return nil, fmt.Errorf("elasticsearch sink: index or IndexResolver is required")
	}
	client := cfg.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}
	idFn := cfg.DocumentID
	if idFn == nil {
		idFn = defaultElasticsearchDocumentID
	}
	return &ElasticsearchRowEventSink{
		baseURL:       base,
		defaultIndex:  strings.TrimSpace(cfg.Index),
		indexResolver: cfg.IndexResolver,
		client:        client,
		refreshParam:  cfg.Refresh,
		getDocumentID: idFn,
		basicUser:     cfg.Username,
		basicPass:     cfg.Password,
		apiKey:        strings.TrimSpace(cfg.APIKey),
	}, nil
}

func normalizeElasticsearchBaseURL(addr string) string {
	addr = strings.TrimSpace(addr)
	addr = strings.TrimRight(addr, "/")
	if addr == "" {
		return ""
	}
	if !strings.HasPrefix(addr, "http://") && !strings.HasPrefix(addr, "https://") {
		addr = "http://" + addr
	}
	return addr
}

func (s *ElasticsearchRowEventSink) resolveIndex(tableKey string) string {
	if s.indexResolver != nil {
		if idx := strings.TrimSpace(s.indexResolver(tableKey)); idx != "" {
			return sanitizeElasticsearchIndexName(idx)
		}
	}
	if s.defaultIndex != "" {
		return sanitizeElasticsearchIndexName(s.defaultIndex)
	}
	return sanitizeElasticsearchIndexName(tableKey)
}

func sanitizeElasticsearchIndexName(name string) string {
	name = strings.ToLower(strings.TrimSpace(name))
	if name == "" {
		return "cdc"
	}
	var b strings.Builder
	for _, r := range name {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
			b.WriteRune(r)
		case r == '-' || r == '_':
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
	}
	out := b.String()
	if out == "" || out[0] == '-' || out[0] == '_' || out[0] == '+' {
		out = "i" + out
	}
	if len(out) > 255 {
		out = out[:255]
	}
	return out
}

func elasticsearchEnvelopeJSON(tableKey, action string, payloadJSON []byte) ([]byte, error) {
	if !json.Valid(payloadJSON) {
		return nil, fmt.Errorf("elasticsearch sink: payload is not valid JSON")
	}
	doc := struct {
		CdcTable  string          `json:"cdc_table"`
		CdcAction string          `json:"cdc_action"`
		Data      json.RawMessage `json:"data"`
	}{
		CdcTable:  tableKey,
		CdcAction: action,
		Data:      json.RawMessage(payloadJSON),
	}
	return json.Marshal(doc)
}

func defaultElasticsearchDocumentID(tableKey, action string, payloadJSON []byte) (string, bool) {
	_ = tableKey
	var v any
	if err := json.Unmarshal(payloadJSON, &v); err != nil {
		return "", false
	}
	if action == "update" {
		m, ok := v.(map[string]any)
		if !ok {
			return "", false
		}
		if after, ok := m["after"].(map[string]any); ok {
			if id, ok := pickElasticsearchRowID(after); ok {
				return id, true
			}
		}
		if before, ok := m["before"].(map[string]any); ok {
			return pickElasticsearchRowID(before)
		}
		return "", false
	}
	m, ok := v.(map[string]any)
	if !ok {
		return "", false
	}
	return pickElasticsearchRowID(m)
}

func pickElasticsearchRowID(m map[string]any) (string, bool) {
	for _, key := range []string{"id", "_id", "pk"} {
		if val, ok := m[key]; ok && val != nil {
			return fmt.Sprint(val), true
		}
	}
	return "", false
}

func (s *ElasticsearchRowEventSink) applyAuth(req *http.Request) {
	if s.apiKey != "" {
		req.Header.Set("Authorization", "ApiKey "+s.apiKey)
		return
	}
	if s.basicUser != "" || s.basicPass != "" {
		req.SetBasicAuth(s.basicUser, s.basicPass)
	}
}

func (s *ElasticsearchRowEventSink) appendRefresh(u string) string {
	if s.refreshParam == "" {
		return u
	}
	sep := "?"
	if strings.Contains(u, "?") {
		sep = "&"
	}
	return u + sep + "refresh=" + url.QueryEscape(s.refreshParam)
}

// Emit sends one CDC event to Elasticsearch.
func (s *ElasticsearchRowEventSink) Emit(tableKey, action string, payloadJSON []byte) error {
	if s == nil || s.client == nil {
		return fmt.Errorf("elasticsearch sink: nil sink")
	}
	if len(payloadJSON) == 0 {
		return fmt.Errorf("elasticsearch sink: empty payload")
	}
	index := s.resolveIndex(tableKey)
	id, hasID := s.getDocumentID(tableKey, action, payloadJSON)

	if action == "delete" {
		if !hasID {
			return fmt.Errorf("elasticsearch sink: delete requires document id in payload")
		}
		u := fmt.Sprintf("%s/%s/_doc/%s", s.baseURL, url.PathEscape(index), url.PathEscape(id))
		u = s.appendRefresh(u)
		req, err := http.NewRequest(http.MethodDelete, u, nil)
		if err != nil {
			return err
		}
		s.applyAuth(req)
		return s.doElasticsearchRequest(req)
	}

	body, err := elasticsearchEnvelopeJSON(tableKey, action, payloadJSON)
	if err != nil {
		return err
	}
	var u string
	method := http.MethodPost
	if hasID {
		method = http.MethodPut
		u = fmt.Sprintf("%s/%s/_doc/%s", s.baseURL, url.PathEscape(index), url.PathEscape(id))
	} else {
		u = fmt.Sprintf("%s/%s/_doc", s.baseURL, url.PathEscape(index))
	}
	u = s.appendRefresh(u)
	req, err := http.NewRequest(method, u, bytes.NewReader(body))
	if err != nil {
		return err
	}
	s.applyAuth(req)
	req.Header.Set("Content-Type", "application/json")
	return s.doElasticsearchRequest(req)
}

func (s *ElasticsearchRowEventSink) doElasticsearchRequest(req *http.Request) error {
	resp, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("elasticsearch sink: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	slurp, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}
	return fmt.Errorf("elasticsearch sink: %s: %s", resp.Status, bytes.TrimSpace(slurp))
}
