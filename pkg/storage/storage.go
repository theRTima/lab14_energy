package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/theRTima/lab14_energy/pkg/meter"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type Storage struct {
	client *clientv3.Client
	prefix string
}

func NewStorage(endpoints []string, prefix string) (*Storage, error) {
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create etcd client: %w", err)
	}

	return &Storage{
		client: client,
		prefix: prefix,
	}, nil
}

func (s *Storage) StoreReading(ctx context.Context, reading meter.Reading) error {
	key := fmt.Sprintf("%s/readings/%s/%d", s.prefix, reading.MeterID, reading.Timestamp.Unix())
	data, err := json.Marshal(reading)
	if err != nil {
		return fmt.Errorf("failed to marshal reading: %w", err)
	}

	_, err = s.client.Put(ctx, key, string(data))
	if err != nil {
		return fmt.Errorf("failed to store reading: %w", err)
	}

	return nil
}

func (s *Storage) GetReadings(ctx context.Context, meterID string, from, to time.Time) ([]meter.Reading, error) {
	startKey := fmt.Sprintf("%s/readings/%s/%d", s.prefix, meterID, from.Unix())
	endKey := fmt.Sprintf("%s/readings/%s/%d", s.prefix, meterID, to.Unix())

	resp, err := s.client.Get(ctx, startKey, clientv3.WithRange(endKey))
	if err != nil {
		return nil, fmt.Errorf("failed to get readings: %w", err)
	}

	readings := make([]meter.Reading, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		var reading meter.Reading
		if err := json.Unmarshal(kv.Value, &reading); err != nil {
			continue
		}
		readings = append(readings, reading)
	}

	return readings, nil
}

func (s *Storage) GetAllReadings(ctx context.Context, from, to time.Time) ([]meter.Reading, error) {
	startKey := fmt.Sprintf("%s/readings/", s.prefix)

	resp, err := s.client.Get(ctx, startKey, clientv3.WithPrefix())
	if err != nil {
		return nil, fmt.Errorf("failed to get all readings: %w", err)
	}

	readings := make([]meter.Reading, 0)
	for _, kv := range resp.Kvs {
		var reading meter.Reading
		if err := json.Unmarshal(kv.Value, &reading); err != nil {
			continue
		}
		if reading.Timestamp.After(from) && reading.Timestamp.Before(to) {
			readings = append(readings, reading)
		}
	}

	return readings, nil
}

func (s *Storage) Close() error {
	if s.client != nil {
		return s.client.Close()
	}
	return nil
}
