package tubing_cdc

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

func TestJoinElasticsearchDocumentID_validation(t *testing.T) {
	tests := []struct {
		name    string
		parts   []ElasticsearchDocumentIDPart
		wantErr string
	}{
		{
			name:    "no parts",
			parts:   nil,
			wantErr: "at least one part",
		},
		{
			name:    "empty field path",
			parts:   []ElasticsearchDocumentIDPart{ElasticsearchDocumentIDField("  ")},
			wantErr: "field path must be non-empty",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := JoinElasticsearchDocumentID(":", tt.parts...)
			if err == nil {
				t.Fatal("expected error")
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("err %q should contain %q", err.Error(), tt.wantErr)
			}
		})
	}
}

func TestJoinElasticsearchDocumentID_ok(t *testing.T) {
	tests := []struct {
		name     string
		sep      string
		parts    []ElasticsearchDocumentIDPart
		tableKey string
		action   string
		payload  []byte
		wantID   string
		wantOK   bool
	}{
		{
			name:     "literal and field",
			sep:      ":",
			parts:    []ElasticsearchDocumentIDPart{ElasticsearchDocumentIDLiteral("prod"), ElasticsearchDocumentIDField("id")},
			tableKey: "db.t",
			action:   "insert",
			payload:  []byte(`{"id":42,"name":"x"}`),
			wantID:   "prod:42",
			wantOK:   true,
		},
		{
			name: "table key action and nested field",
			sep:  "|",
			parts: []ElasticsearchDocumentIDPart{
				ElasticsearchDocumentIDTableKey(),
				ElasticsearchDocumentIDAction(),
				ElasticsearchDocumentIDField("after.order_id"),
			},
			tableKey: "shop.orders",
			action:   "update",
			payload:  []byte(`{"before":{"order_id":1},"after":{"order_id":99}}`),
			wantID:   "shop.orders|update|99",
			wantOK:   true,
		},
		{
			name:     "before path when after unused",
			sep:      "-",
			parts:    []ElasticsearchDocumentIDPart{ElasticsearchDocumentIDField("before.sku")},
			tableKey: "db.items",
			action:   "update",
			payload:  []byte(`{"before":{"sku":"A1"},"after":{"sku":"A2"}}`),
			wantID:   "A1",
			wantOK:   true,
		},
		{
			name:     "missing field",
			sep:      ":",
			parts:    []ElasticsearchDocumentIDPart{ElasticsearchDocumentIDField("missing")},
			tableKey: "db.t",
			action:   "insert",
			payload:  []byte(`{"id":1}`),
			wantOK:   false,
		},
		{
			name:     "invalid json",
			sep:      ":",
			parts:    []ElasticsearchDocumentIDPart{ElasticsearchDocumentIDField("id")},
			tableKey: "db.t",
			action:   "insert",
			payload:  []byte(`not-json`),
			wantOK:   false,
		},
		{
			name:     "only literals",
			sep:      "",
			parts:    []ElasticsearchDocumentIDPart{ElasticsearchDocumentIDLiteral("fix"), ElasticsearchDocumentIDLiteral("ed")},
			tableKey: "db.t",
			action:   "insert",
			payload:  []byte(`{}`),
			wantID:   "fixed",
			wantOK:   true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fn, err := JoinElasticsearchDocumentID(tt.sep, tt.parts...)
			if err != nil {
				t.Fatal(err)
			}
			got, ok := fn(tt.tableKey, tt.action, tt.payload)
			if ok != tt.wantOK {
				t.Fatalf("ok got %v want %v (id=%q)", ok, tt.wantOK, got)
			}
			if tt.wantOK && got != tt.wantID {
				t.Fatalf("id got %q want %q", got, tt.wantID)
			}
		})
	}
}

func TestJoinElasticsearchDocumentID_singleLiteralEmptyYieldsFalse(t *testing.T) {
	fn, err := JoinElasticsearchDocumentID("-", ElasticsearchDocumentIDLiteral(""))
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := fn("db.t", "insert", []byte(`{}`)); ok {
		t.Fatal("expected false for empty composed id")
	}
}

func TestElasticsearchSink_JoinDocumentID_emit(t *testing.T) {
	docID, err := JoinElasticsearchDocumentID(":",
		ElasticsearchDocumentIDLiteral("v1"),
		ElasticsearchDocumentIDTableKey(),
		ElasticsearchDocumentIDField("id"),
	)
	if err != nil {
		t.Fatal(err)
	}
	var gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)
	sink, err := NewElasticsearchRowEventSink(ElasticsearchSinkConfig{
		Addresses:  []string{srv.URL},
		Index:      "idx",
		DocumentID: docID,
		HTTPClient: srv.Client(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := sink.Emit("acme.users", "insert", []byte(`{"id":7}`)); err != nil {
		t.Fatal(err)
	}
	wantID := "v1:acme.users:7"
	wantPath := "/" + sanitizeElasticsearchIndexName("idx") + "/_doc/" + url.PathEscape(wantID)
	if gotPath != wantPath {
		t.Fatalf("path got %q want %q", gotPath, wantPath)
	}
}
