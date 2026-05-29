package sender

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/theRTima/lab14_energy/pkg/aggregator"
)

type PythonSender struct {
	endpoint string
	client   *http.Client
}

func NewPythonSender(endpoint string) *PythonSender {
	return &PythonSender{
		endpoint: endpoint,
		client: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

func (ps *PythonSender) SendAggregatedData(ctx context.Context, data aggregator.AggregatedData) error {
	jsonData, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to marshal data: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", ps.endpoint+"/aggregate", bytes.NewBuffer(jsonData))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := ps.client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	log.Printf("Successfully sent aggregated data: %d readings from %d meters",
		data.TotalCount, len(data.Metrics))

	return nil
}
