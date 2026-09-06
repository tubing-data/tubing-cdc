package tubing_cdc

import (
	"errors"
	"sync"
	"testing"
	"time"
)

type flakySink struct {
	mu              sync.Mutex
	failures, calls int
}

func (s *flakySink) Emit(_, _ string, _ []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	if s.calls <= s.failures {
		return errors.New("temporary")
	}
	return nil
}

func TestReliableRowEventSink_retriesAndMetrics(t *testing.T) {
	inner := &flakySink{failures: 2}
	m := &RuntimeMetrics{}
	s, err := NewReliableRowEventSink(ReliableSinkConfig{Sink: inner, MaxAttempts: 3, RetryBackoff: time.Nanosecond, Metrics: m})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Emit("db.t", "insert", []byte(`{"id":1}`)); err != nil {
		t.Fatal(err)
	}
	got := m.Snapshot()
	if got.Emitted != 1 || got.Retries != 2 || got.Failed != 0 {
		t.Fatalf("metrics=%+v", got)
	}
}

func TestReliableRowEventSink_deadLetter(t *testing.T) {
	inner := &flakySink{failures: 10}
	dlq := &flakySink{}
	m := &RuntimeMetrics{}
	s, _ := NewReliableRowEventSink(ReliableSinkConfig{Sink: inner, DeadLetter: dlq, MaxAttempts: 2, RetryBackoff: time.Nanosecond, Metrics: m})
	if err := s.Emit("db.t", "insert", []byte(`{"id":1}`)); err == nil {
		t.Fatal("expected error")
	}
	got := m.Snapshot()
	if got.Failed != 1 || got.Retries != 1 || got.DLQ != 1 {
		t.Fatalf("metrics=%+v", got)
	}
}
