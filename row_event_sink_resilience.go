package tubing_cdc

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// RuntimeMetrics contains dependency-free counters suitable for exporting through an application's
// existing metrics system.
type RuntimeMetrics struct {
	emitted atomic.Uint64
	failed  atomic.Uint64
	retries atomic.Uint64
	dlq     atomic.Uint64
}

type RuntimeMetricsSnapshot struct {
	Emitted uint64 `json:"emitted"`
	Failed  uint64 `json:"failed"`
	Retries uint64 `json:"retries"`
	DLQ     uint64 `json:"dlq"`
}

func (m *RuntimeMetrics) Snapshot() RuntimeMetricsSnapshot {
	if m == nil {
		return RuntimeMetricsSnapshot{}
	}
	return RuntimeMetricsSnapshot{m.emitted.Load(), m.failed.Load(), m.retries.Load(), m.dlq.Load()}
}

// ReliableSinkConfig adds serialized delivery, bounded retries, counters, and an optional dead-letter sink.
type ReliableSinkConfig struct {
	Sink         RowEventSink
	DeadLetter   RowEventSink
	MaxAttempts  int
	RetryBackoff time.Duration
	Metrics      *RuntimeMetrics
}

type reliableRowEventSink struct {
	cfg ReliableSinkConfig
	mu  sync.Mutex
}

// NewReliableRowEventSink returns a concurrency-safe sink wrapper. Calls are serialized to preserve
// input order. MaxAttempts defaults to 3 and RetryBackoff defaults to 100ms.
func NewReliableRowEventSink(cfg ReliableSinkConfig) (RowEventSink, error) {
	if cfg.Sink == nil {
		return nil, fmt.Errorf("reliable sink: Sink is nil")
	}
	if cfg.MaxAttempts < 0 {
		return nil, fmt.Errorf("reliable sink: MaxAttempts cannot be negative")
	}
	if cfg.MaxAttempts == 0 {
		cfg.MaxAttempts = 3
	}
	if cfg.RetryBackoff <= 0 {
		cfg.RetryBackoff = 100 * time.Millisecond
	}
	if cfg.Metrics == nil {
		cfg.Metrics = &RuntimeMetrics{}
	}
	return &reliableRowEventSink{cfg: cfg}, nil
}

type DeadLetterEvent struct {
	Table    string          `json:"table"`
	Action   string          `json:"action"`
	Payload  json.RawMessage `json:"payload"`
	Error    string          `json:"error"`
	Attempts int             `json:"attempts"`
	FailedAt time.Time       `json:"failed_at"`
}

func (s *reliableRowEventSink) Emit(tableKey, action string, payloadJSON []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var last error
	for attempt := 1; attempt <= s.cfg.MaxAttempts; attempt++ {
		if err := s.cfg.Sink.Emit(tableKey, action, payloadJSON); err == nil {
			s.cfg.Metrics.emitted.Add(1)
			return nil
		} else {
			last = err
		}
		if attempt < s.cfg.MaxAttempts {
			s.cfg.Metrics.retries.Add(1)
			time.Sleep(s.cfg.RetryBackoff)
		}
	}
	s.cfg.Metrics.failed.Add(1)
	if s.cfg.DeadLetter != nil {
		b, err := json.Marshal(DeadLetterEvent{Table: tableKey, Action: action, Payload: append([]byte(nil), payloadJSON...), Error: last.Error(), Attempts: s.cfg.MaxAttempts, FailedAt: time.Now().UTC()})
		if err == nil {
			if dlqErr := s.cfg.DeadLetter.Emit(tableKey, "dead_letter", b); dlqErr == nil {
				s.cfg.Metrics.dlq.Add(1)
			} else {
				last = errors.Join(last, fmt.Errorf("reliable sink: dead letter: %w", dlqErr))
			}
		}
	}
	return fmt.Errorf("reliable sink: delivery failed after %d attempts: %w", s.cfg.MaxAttempts, last)
}
