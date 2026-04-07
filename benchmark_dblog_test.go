package tubing_cdc

import (
	"encoding/json"
	"testing"

	"github.com/go-mysql-org/go-mysql/canal"
)

func BenchmarkMarshalCDCEventEnvelope(b *testing.B) {
	row := map[string]any{"id": int64(1), "name": "alice", "bio": "x"}
	inner, err := json.Marshal(row)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := MarshalCDCEventEnvelope(
			DefaultEnvelopeSchemaVersion,
			OriginLog,
			canal.InsertAction,
			"app.users",
			nil,
			row,
			inner,
			nil,
			nil,
		)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkReconcileChunkRows(b *testing.B) {
	tests := []struct {
		name          string
		nRows         int
		conflictEvery int // 0 = none; k>0 marks every k-th PK as seen in window
	}{
		{name: "1k_no_conflict", nRows: 1000, conflictEvery: 0},
		{name: "1k_half_conflict", nRows: 1000, conflictEvery: 2},
		{name: "10k_no_conflict", nRows: 10000, conflictEvery: 0},
	}
	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			rows := make([]map[string]any, tt.nRows)
			for i := 0; i < tt.nRows; i++ {
				rows[i] = map[string]any{"id": int64(i + 1)}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				tr := NewAlgorithm1Tracker()
				if err := tr.BeginCapture("db.t", "low-u", "high-u", []string{"id"}); err != nil {
					b.Fatal(err)
				}
				if err := tr.OnWatermark(WatermarkBinlogEvent{NewValue: "low-u"}); err != nil {
					b.Fatal(err)
				}
				if tt.conflictEvery > 0 {
					for j := 0; j < tt.nRows; j += tt.conflictEvery {
						tr.RecordTargetRowChange("db.t", rows[j])
					}
				}
				if err := tr.OnWatermark(WatermarkBinlogEvent{NewValue: "high-u"}); err != nil {
					b.Fatal(err)
				}
				_, err := tr.ReconcileChunkRows(rows)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkBuildPKOrderedChunkSelect(b *testing.B) {
	afterPK := []any{int64(1000), "cursor"}
	tests := []struct {
		name      string
		chunkSize int
	}{
		{name: "chunk_100", chunkSize: 100},
		{name: "chunk_1000", chunkSize: 1000},
	}
	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, _, err := BuildPKOrderedChunkSelect("app", "orders", []string{"id", "shard"}, tt.chunkSize, afterPK)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
