package tubing_cdc

import (
	"fmt"
	"io"
	"os"

	"github.com/siddontang/go-log/log"
)

// RowEventSink receives serialized CDC row payloads (JSON). Implementations decide how to
// persist or stream them (stdout, log, Kafka, etc.).
type RowEventSink interface {
	Emit(tableKey, action string, payloadJSON []byte) error
}

// LoggerRowSink writes each event with the same format as the historical DynamicTableEventHandler
// behavior (via go-log).
type LoggerRowSink struct{}

func (LoggerRowSink) Emit(tableKey, action string, payloadJSON []byte) error {
	log.Infof("[CDC] %s %s %s", action, tableKey, string(payloadJSON))
	return nil
}

// StdoutRowSink writes one line per event to Writer (default os.Stdout when Writer is nil).
type StdoutRowSink struct {
	Writer io.Writer
}

func (s StdoutRowSink) Emit(tableKey, action string, payloadJSON []byte) error {
	w := s.Writer
	if w == nil {
		w = os.Stdout
	}
	_, err := fmt.Fprintf(w, "[CDC] %s %s %s\n", action, tableKey, payloadJSON)
	return err
}
