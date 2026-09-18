package tubing_cdc

import (
	"testing"

	"github.com/segmentio/kafka-go"
)

func TestNewKafkaRowEventSink_defaultsToHashBalancer(t *testing.T) {
	sink, err := NewKafkaRowEventSink(KafkaSinkConfig{Brokers: []string{"127.0.0.1:9092"}, Topic: "cdc"})
	if err != nil {
		t.Fatal(err)
	}
	defer sink.Close()
	if _, ok := sink.writer.Balancer.(*kafka.Hash); !ok {
		t.Fatalf("balancer = %T, want *kafka.Hash", sink.writer.Balancer)
	}
}

func TestNewKafkaRowEventSink_customMessageKey(t *testing.T) {
	sink, err := NewKafkaRowEventSink(KafkaSinkConfig{
		Brokers: []string{"127.0.0.1:9092"},
		Topic:   "cdc",
		MessageKey: func(tableKey, action string, payloadJSON []byte) []byte {
			return []byte(tableKey + ":42")
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sink.Close()
	if got := string(sink.messageKey("db.t", "insert", nil)); got != "db.t:42" {
		t.Fatalf("key = %q", got)
	}
}
