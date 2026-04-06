package tubing_cdc

import (
	"bytes"
	"strings"
	"testing"
)

func TestNewKafkaRowEventSink_validation(t *testing.T) {
	tests := []struct {
		name    string
		cfg     KafkaSinkConfig
		wantErr string
	}{
		{
			name:    "empty topic",
			cfg:     KafkaSinkConfig{Brokers: []string{"localhost:9092"}, Topic: ""},
			wantErr: "topic is required",
		},
		{
			name:    "no brokers",
			cfg:     KafkaSinkConfig{Brokers: nil, Topic: "cdc"},
			wantErr: "at least one broker",
		},
		{
			name:    "empty brokers slice",
			cfg:     KafkaSinkConfig{Brokers: []string{}, Topic: "cdc"},
			wantErr: "at least one broker",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewKafkaRowEventSink(tt.cfg)
			if err == nil {
				t.Fatal("expected error")
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("err %q should contain %q", err.Error(), tt.wantErr)
			}
		})
	}
}

func TestNewKafkaRowEventSink_ok(t *testing.T) {
	sink, err := NewKafkaRowEventSink(KafkaSinkConfig{
		Brokers: []string{"127.0.0.1:9092"},
		Topic:   "cdc_events",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sink.Close() }()
	if sink.writer == nil {
		t.Fatal("writer is nil")
	}
}

func TestStdoutRowSink_Emit(t *testing.T) {
	tests := []struct {
		name       string
		writer     *bytes.Buffer
		tableKey   string
		action     string
		payload    []byte
		wantSuffix string
	}{
		{
			name:       "insert line",
			writer:     &bytes.Buffer{},
			tableKey:   "db.users",
			action:     "insert",
			payload:    []byte(`{"id":1}`),
			wantSuffix: "[CDC] insert db.users {\"id\":1}\n",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := StdoutRowSink{Writer: tt.writer}
			if err := s.Emit(tt.tableKey, tt.action, tt.payload); err != nil {
				t.Fatal(err)
			}
			if got := tt.writer.String(); got != tt.wantSuffix {
				t.Fatalf("got %q want %q", got, tt.wantSuffix)
			}
		})
	}
}

func TestKafkaRowEventSink_Emit_nil(t *testing.T) {
	var k *KafkaRowEventSink
	if err := k.Emit("a.b", "insert", []byte(`{}`)); err == nil {
		t.Fatal("expected error")
	}
}

func TestKafkaRowEventSink_Close_nil(t *testing.T) {
	var k *KafkaRowEventSink
	if err := k.Close(); err != nil {
		t.Fatal(err)
	}
}
