package sender

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/theRTima/lab14_energy/pkg/aggregator"
)

type KafkaSender struct {
	writer *kafka.Writer
	topic  string
}

func NewKafkaSender(brokers []string, topic string) (*KafkaSender, error) {
	writer := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafka.Hash{},
		BatchTimeout: 100 * time.Millisecond,
		RequiredAcks: kafka.RequireOne,
		Async:        false,
	}

	return &KafkaSender{writer: writer, topic: topic}, nil
}

func (ks *KafkaSender) SendAggregatedData(ctx context.Context, data aggregator.AggregatedData) error {
	payload := struct {
		WindowStart int64              `json:"window_start"`
		WindowEnd   int64              `json:"window_end"`
		ShardID     int                `json:"shard_id"`
		CollectorID string             `json:"collector_id"`
		TotalCount  int                `json:"total_count"`
		Metrics     []aggregatedMetric `json:"metrics"`
	}{
		WindowStart: data.WindowStart.UnixMicro(),
		WindowEnd:   data.WindowEnd.UnixMicro(),
		ShardID:     data.ShardID,
		CollectorID: data.CollectorID,
		TotalCount:  data.TotalCount,
	}

	for _, m := range data.Metrics {
		payload.Metrics = append(payload.Metrics, aggregatedMetric{
			MeterID:      m.MeterID,
			Count:        m.Count,
			SumKWh:       m.SumKWh,
			AvgKWh:       m.AvgKWh,
			MinKWh:       m.MinKWh,
			MaxKWh:       m.MaxKWh,
			AvgVoltage:   m.AvgVoltage,
			AvgCurrent:   m.AvgCurrent,
			FirstReading: m.FirstReading.UnixMicro(),
			LastReading:  m.LastReading.UnixMicro(),
		})
	}

	value, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal aggregated data: %w", err)
	}

	msg := kafka.Message{
		Key:   []byte(data.CollectorID),
		Value: value,
	}

	if err := ks.writer.WriteMessages(ctx, msg); err != nil {
		return fmt.Errorf("failed to write to Kafka: %w", err)
	}

	log.Printf("Sent aggregated data to Kafka topic %s: %d readings from %d meters",
		ks.topic, data.TotalCount, len(data.Metrics))
	return nil
}

func (ks *KafkaSender) Close() error {
	return ks.writer.Close()
}

type aggregatedMetric struct {
	MeterID      string  `json:"meter_id"`
	Count        int     `json:"count"`
	SumKWh       float64 `json:"sum_kwh"`
	AvgKWh       float64 `json:"avg_kwh"`
	MinKWh       float64 `json:"min_kwh"`
	MaxKWh       float64 `json:"max_kwh"`
	AvgVoltage   float64 `json:"avg_voltage"`
	AvgCurrent   float64 `json:"avg_current"`
	FirstReading int64   `json:"first_reading"`
	LastReading  int64   `json:"last_reading"`
}
