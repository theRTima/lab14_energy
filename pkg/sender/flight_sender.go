package sender

import (
	"context"
	"fmt"
	"log"

	"github.com/theRTima/lab14_energy/pkg/aggregator"
	"github.com/theRTima/lab14_energy/pkg/flightserver"
)

type FlightSender struct {
	server *flightserver.FlightServer
}

func NewFlightSender(server *flightserver.FlightServer) *FlightSender {
	return &FlightSender{
		server: server,
	}
}

func (fs *FlightSender) SendAggregatedData(ctx context.Context, data aggregator.AggregatedData) error {
	if fs.server == nil {
		return fmt.Errorf("flight server not initialized")
	}

	fs.server.AddAggregatedData(data)
	log.Printf("Sent aggregated data to Flight server: %d readings from %d meters",
		data.TotalCount, len(data.Metrics))

	return nil
}
