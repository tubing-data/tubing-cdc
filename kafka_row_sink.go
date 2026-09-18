package tubing_cdc

import (
	"context"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
)

// KafkaRowEventSink publishes each row event as a Kafka message. Value is the JSON payload;
// message key is tableKey (e.g. "db.table"); a header "cdc_action" carries the canal action name.
type KafkaRowEventSink struct {
	writer     *kafka.Writer
	messageKey func(tableKey, action string, payloadJSON []byte) []byte
}

// KafkaSinkConfig holds broker addresses and topic for NewKafkaRowEventSink.
type KafkaSinkConfig struct {
	Brokers []string
	Topic   string
	// Optional kafka.Writer fields; zero values use sensible defaults for streaming CDC.
	BatchTimeout time.Duration
	RequiredAcks kafka.RequiredAcks
	// Balancer defaults to kafka.Hash so equal message keys remain on one partition.
	Balancer kafka.Balancer
	// MessageKey overrides the default tableKey message key. Returning a stable
	// primary-key-derived value preserves per-entity ordering with more parallelism.
	MessageKey func(tableKey, action string, payloadJSON []byte) []byte
}

// NewKafkaRowEventSink builds a sink backed by a kafka.Writer. Call Close when shutting down.
func NewKafkaRowEventSink(cfg KafkaSinkConfig) (*KafkaRowEventSink, error) {
	if cfg.Topic == "" {
		return nil, fmt.Errorf("kafka sink: topic is required")
	}
	if len(cfg.Brokers) == 0 {
		return nil, fmt.Errorf("kafka sink: at least one broker address is required")
	}
	batch := cfg.BatchTimeout
	if batch == 0 {
		batch = 10 * time.Millisecond
	}
	acks := cfg.RequiredAcks
	if acks == 0 {
		acks = kafka.RequireOne
	}
	balancer := cfg.Balancer
	if balancer == nil {
		balancer = &kafka.Hash{}
	}
	w := &kafka.Writer{
		Addr:         kafka.TCP(cfg.Brokers...),
		Topic:        cfg.Topic,
		BatchTimeout: batch,
		RequiredAcks: acks,
		Balancer:     balancer,
	}
	return &KafkaRowEventSink{writer: w, messageKey: cfg.MessageKey}, nil
}

func (k *KafkaRowEventSink) Emit(tableKey, action string, payloadJSON []byte) error {
	if k == nil || k.writer == nil {
		return fmt.Errorf("kafka sink: nil writer")
	}
	key := []byte(tableKey)
	if k.messageKey != nil {
		key = k.messageKey(tableKey, action, payloadJSON)
	}
	msg := kafka.Message{
		Key:   append([]byte(nil), key...),
		Value: append([]byte(nil), payloadJSON...),
		Headers: []kafka.Header{
			{Key: "cdc_action", Value: []byte(action)},
		},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return k.writer.WriteMessages(ctx, msg)
}

// Close releases the Kafka writer.
func (k *KafkaRowEventSink) Close() error {
	if k == nil || k.writer == nil {
		return nil
	}
	return k.writer.Close()
}
