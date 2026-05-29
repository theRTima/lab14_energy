package sender

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/IBM/sarama"
	"github.com/theRTima/lab14_energy/pkg/aggregator"
)

type KafkaSender struct {
	producer sarama.SyncProducer
	topic    string
}

func NewKafkaSender(brokers []string, topic string) (*KafkaSender, error) {
	log.Printf("Kafka sender initializing brokers=%v topic=%s", brokers, topic)
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForLocal
	config.Producer.Timeout = 5 * time.Second
	config.Net.DialTimeout = 5 * time.Second
	config.Net.ReadTimeout = 5 * time.Second
	config.Net.WriteTimeout = 5 * time.Second
	config.Version = sarama.V0_10_2_0

	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka producer: %w", err)
	}

	return &KafkaSender{producer: producer, topic: topic}, nil
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
		return fmt.Errorf("failed to marshal: %w", err)
	}

	msg := &sarama.ProducerMessage{
		Topic: ks.topic,
		Key:   sarama.StringEncoder(data.CollectorID),
		Value: sarama.ByteEncoder(value),
	}

	partition, offset, err := ks.producer.SendMessage(msg)
	if err != nil {
		return fmt.Errorf("failed to send message to Kafka: %w", err)
	}

	log.Printf("Sent aggregated data to Kafka topic %s (partition=%d offset=%d): %d readings from %d meters",
		ks.topic, partition, offset, data.TotalCount, len(data.Metrics))
	return nil
}

func (ks *KafkaSender) Close() error {
	return ks.producer.Close()
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