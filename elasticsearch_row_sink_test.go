package tubing_cdc

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestNewElasticsearchRowEventSink_validation(t *testing.T) {
	tests := []struct {
		name    string
		cfg     ElasticsearchSinkConfig
		wantErr string
	}{
		{
			name:    "no addresses",
			cfg:     ElasticsearchSinkConfig{Index: "cdc"},
			wantErr: "at least one address",
		},
		{
			name:    "empty address string",
			cfg:     ElasticsearchSinkConfig{Addresses: []string{"   "}, Index: "cdc"},
			wantErr: "invalid address",
		},
		{
			name:    "no index and no resolver",
			cfg:     ElasticsearchSinkConfig{Addresses: []string{"http://localhost:9200"}},
			wantErr: "index or IndexResolver",
		},
		{
			name:    "whitespace-only index without resolver",
			cfg:     ElasticsearchSinkConfig{Addresses: []string{"http://localhost:9200"}, Index: "  "},
			wantErr: "index or IndexResolver",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewElasticsearchRowEventSink(tt.cfg)
			if err == nil {
				t.Fatal("expected error")
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("err %q should contain %q", err.Error(), tt.wantErr)
			}
		})
	}
}

func TestNewElasticsearchRowEventSink_ok(t *testing.T) {
	sink, err := NewElasticsearchRowEventSink(ElasticsearchSinkConfig{
		Addresses: []string{"127.0.0.1:9200"},
		Index:     "cdc_events",
	})
	if err != nil {
		t.Fatal(err)
	}
	if sink == nil || sink.client == nil {
		t.Fatal("sink not initialized")
	}
	if !strings.HasPrefix(sink.baseURL, "http://") {
		t.Fatalf("baseURL %q", sink.baseURL)
	}
}

func TestElasticsearchRowEventSink_Emit_nil(t *testing.T) {
	var s *ElasticsearchRowEventSink
	if err := s.Emit("a.b", "insert", []byte(`{}`)); err == nil {
		t.Fatal("expected error")
	}
}

func TestElasticsearchRowEventSink_Emit_emptyPayload(t *testing.T) {
	s := &ElasticsearchRowEventSink{client: http.DefaultClient}
	if err := s.Emit("a.b", "insert", nil); err == nil {
		t.Fatal("expected error")
	}
}

func TestElasticsearchRowEventSink_Emit_invalidJSON(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	t.Cleanup(srv.Close)
	sink, err := NewElasticsearchRowEventSink(ElasticsearchSinkConfig{
		Addresses:  []string{srv.URL},
		Index:      "t",
		HTTPClient: srv.Client(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := sink.Emit("db.t", "insert", []byte(`not json`)); err == nil {
		t.Fatal("expected error")
	} else if !strings.Contains(err.Error(), "valid JSON") {
		t.Fatalf("got %v", err)
	}
}

func TestElasticsearchRowEventSink_Emit_deleteWithoutID(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	t.Cleanup(srv.Close)
	sink, err := NewElasticsearchRowEventSink(ElasticsearchSinkConfig{
		Addresses:  []string{srv.URL},
		Index:      "t",
		HTTPClient: srv.Client(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := sink.Emit("db.t", "delete", []byte(`{"name":"x"}`)); err == nil {
		t.Fatal("expected error")
	} else if !strings.Contains(err.Error(), "delete requires document id") {
		t.Fatalf("got %v", err)
	}
}

func TestElasticsearchRowEventSink_Emit_httpFlow(t *testing.T) {
	tests := []struct {
		name           string
		tableKey       string
		action         string
		payload        []byte
		cfg            ElasticsearchSinkConfig
		wantMethod     string
		wantPathSuffix string // suffix after index, e.g. "_doc/42" or "_doc"
		checkBody      func(t *testing.T, body []byte)
	}{
		{
			name:           "put with top-level id",
			tableKey:       "db.users",
			action:         "insert",
			payload:        []byte(`{"id":42,"name":"a"}`),
			cfg:            ElasticsearchSinkConfig{Index: "my_cdc"},
			wantMethod:     http.MethodPut,
			wantPathSuffix: "_doc/42",
			checkBody: func(t *testing.T, body []byte) {
				t.Helper()
				var m map[string]json.RawMessage
				if err := json.Unmarshal(body, &m); err != nil {
					t.Fatal(err)
				}
				if string(m["cdc_table"]) != `"db.users"` {
					t.Fatalf("cdc_table: %s", m["cdc_table"])
				}
				if string(m["cdc_action"]) != `"insert"` {
					t.Fatalf("cdc_action: %s", m["cdc_action"])
				}
			},
		},
		{
			name:           "post without id",
			tableKey:       "db.users",
			action:         "insert",
			payload:        []byte(`{"name":"only"}`),
			cfg:            ElasticsearchSinkConfig{Index: "my_cdc"},
			wantMethod:     http.MethodPost,
			wantPathSuffix: "_doc",
			checkBody:      func(t *testing.T, body []byte) { t.Helper() },
		},
		{
			name:           "update uses after id",
			tableKey:       "db.users",
			action:         "update",
			payload:        []byte(`{"before":{"id":9},"after":{"id":9,"k":2}}`),
			cfg:            ElasticsearchSinkConfig{Index: "my_cdc"},
			wantMethod:     http.MethodPut,
			wantPathSuffix: "_doc/9",
			checkBody:      func(t *testing.T, body []byte) { t.Helper() },
		},
		{
			name:           "delete",
			tableKey:       "db.users",
			action:         "delete",
			payload:        []byte(`{"id":99}`),
			cfg:            ElasticsearchSinkConfig{Index: "my_cdc"},
			wantMethod:     http.MethodDelete,
			wantPathSuffix: "_doc/99",
			checkBody:      nil,
		},
		{
			name:     "index resolver",
			tableKey: "Acme.Orders",
			action:   "insert",
			payload:  []byte(`{"id":1}`),
			cfg: ElasticsearchSinkConfig{
				Index: "ignored",
				IndexResolver: func(tableKey string) string {
					if tableKey != "Acme.Orders" {
						t.Fatalf("unexpected key %q", tableKey)
					}
					return "custom_IDX"
				},
			},
			wantMethod:     http.MethodPut,
			wantPathSuffix: "_doc/1",
			checkBody:      func(t *testing.T, body []byte) { t.Helper() },
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var gotMethod string
			var gotPath string
			var gotBody []byte
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				gotMethod = r.Method
				gotPath = r.URL.Path
				if r.Body != nil {
					gotBody, _ = io.ReadAll(r.Body)
					_ = r.Body.Close()
				}
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(`{}`))
			}))
			t.Cleanup(srv.Close)

			cfg := tt.cfg
			cfg.Addresses = []string{srv.URL}
			if cfg.HTTPClient == nil {
				cfg.HTTPClient = srv.Client()
			}
			sink, err := NewElasticsearchRowEventSink(cfg)
			if err != nil {
				t.Fatal(err)
			}
			if err := sink.Emit(tt.tableKey, tt.action, tt.payload); err != nil {
				t.Fatal(err)
			}
			if gotMethod != tt.wantMethod {
				t.Fatalf("method got %q want %q", gotMethod, tt.wantMethod)
			}
			wantIndex := sanitizeElasticsearchIndexName(tt.cfg.Index)
			if tt.cfg.IndexResolver != nil {
				wantIndex = sanitizeElasticsearchIndexName(tt.cfg.IndexResolver(tt.tableKey))
			}
			wantPath := "/" + wantIndex + "/" + tt.wantPathSuffix
			if gotPath != wantPath {
				t.Fatalf("path got %q want %q", gotPath, wantPath)
			}
			if tt.checkBody != nil {
				tt.checkBody(t, gotBody)
			}
		})
	}
}

func TestElasticsearchRowEventSink_Emit_refreshQuery(t *testing.T) {
	var rawQuery string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rawQuery = r.URL.RawQuery
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)
	sink, err := NewElasticsearchRowEventSink(ElasticsearchSinkConfig{
		Addresses:  []string{srv.URL},
		Index:      "i",
		Refresh:    "wait_for",
		HTTPClient: srv.Client(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := sink.Emit("d.t", "insert", []byte(`{"id":1}`)); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(rawQuery, "refresh=") {
		t.Fatalf("query %q", rawQuery)
	}
}

func TestElasticsearchRowEventSink_Emit_apiKeyAuth(t *testing.T) {
	var auth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)
	sink, err := NewElasticsearchRowEventSink(ElasticsearchSinkConfig{
		Addresses:  []string{srv.URL},
		Index:      "i",
		APIKey:     "c2VjcmV0",
		HTTPClient: srv.Client(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := sink.Emit("d.t", "insert", []byte(`{"id":1}`)); err != nil {
		t.Fatal(err)
	}
	if auth != "ApiKey c2VjcmV0" {
		t.Fatalf("Authorization %q", auth)
	}
}

func TestElasticsearchRowEventSink_Emit_httpError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "nope", http.StatusBadRequest)
	}))
	t.Cleanup(srv.Close)
	sink, err := NewElasticsearchRowEventSink(ElasticsearchSinkConfig{
		Addresses:  []string{srv.URL},
		Index:      "i",
		HTTPClient: srv.Client(),
	})
	if err != nil {
		t.Fatal(err)
	}
	err = sink.Emit("d.t", "insert", []byte(`{"id":1}`))
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "400") {
		t.Fatalf("got %v", err)
	}
}

func TestElasticsearchRowEventSink_Emit_customDocumentID(t *testing.T) {
	var gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)
	sink, err := NewElasticsearchRowEventSink(ElasticsearchSinkConfig{
		Addresses: []string{srv.URL},
		Index:     "i",
		DocumentID: func(tableKey, action string, payloadJSON []byte) (string, bool) {
			return "custom-key", true
		},
		HTTPClient: srv.Client(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := sink.Emit("d.t", "insert", []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(gotPath, "/_doc/custom-key") {
		t.Fatalf("path %q", gotPath)
	}
}

func TestSanitizeElasticsearchIndexName_viaResolver(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.URL.Path, "/db_schema_orders/") {
			t.Errorf("path %q", r.URL.Path)
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)
	sink, err := NewElasticsearchRowEventSink(ElasticsearchSinkConfig{
		Addresses: []string{srv.URL},
		IndexResolver: func(string) string {
			return "DB.Schema.Orders"
		},
		HTTPClient: srv.Client(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := sink.Emit("x.y", "insert", []byte(`{"id":1}`)); err != nil {
		t.Fatal(err)
	}
}
