package flightclient

import (
	"context"
	"fmt"
	"log"

	"github.com/apache/arrow/go/v17/arrow/flight"
	"github.com/theRTima/lab14_energy/pkg/aggregator"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type FlightClient struct {
	client flight.Client
	addr   string
}

func NewFlightClient(addr string) (*FlightClient, error) {
	client, err := flight.NewClientWithMiddleware(addr, nil, nil, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to create flight client: %w", err)
	}

	return &FlightClient{
		client: client,
		addr:   addr,
	}, nil
}

func (fc *FlightClient) SendAggregatedData(ctx context.Context, data aggregator.AggregatedData) error {
	log.Printf("Flight client would send data to %s (server handles storage)", fc.addr)
	return nil
}

func (fc *FlightClient) Close() error {
	return fc.client.Close()
}
