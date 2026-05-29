package sender

import (
	"context"
	"fmt"
	"log"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/flight"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/theRTima/lab14_energy/pkg/aggregator"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type FlightClientSender struct {
	client flight.Client
	alloc  memory.Allocator
	addr   string
}

func NewFlightClientSender(addr string) (*FlightClientSender, error) {
	client, err := flight.NewClientWithMiddleware(addr, nil, nil, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to create flight client: %w", err)
	}

	return &FlightClientSender{
		client: client,
		alloc:  memory.NewGoAllocator(),
		addr:   addr,
	}, nil
}

func (fcs *FlightClientSender) SendAggregatedData(ctx context.Context, data aggregator.AggregatedData) error {
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "window_start", Type: arrow.FixedWidthTypes.Timestamp_us},
			{Name: "window_end", Type: arrow.FixedWidthTypes.Timestamp_us},
			{Name: "collector_id", Type: arrow.BinaryTypes.String},
			{Name: "shard_id", Type: arrow.PrimitiveTypes.Int32},
			{Name: "meter_id", Type: arrow.BinaryTypes.String},
			{Name: "count", Type: arrow.PrimitiveTypes.Int32},
			{Name: "sum_kwh", Type: arrow.PrimitiveTypes.Float64},
			{Name: "avg_kwh", Type: arrow.PrimitiveTypes.Float64},
			{Name: "min_kwh", Type: arrow.PrimitiveTypes.Float64},
			{Name: "max_kwh", Type: arrow.PrimitiveTypes.Float64},
			{Name: "avg_voltage", Type: arrow.PrimitiveTypes.Float64},
			{Name: "avg_current", Type: arrow.PrimitiveTypes.Float64},
		},
		nil,
	)

	builder := array.NewRecordBuilder(fcs.alloc, schema)
	defer builder.Release()

	for _, metric := range data.Metrics {
		builder.Field(0).(*array.TimestampBuilder).Append(arrow.Timestamp(data.WindowStart.UnixMicro()))
		builder.Field(1).(*array.TimestampBuilder).Append(arrow.Timestamp(data.WindowEnd.UnixMicro()))
		builder.Field(2).(*array.StringBuilder).Append(data.CollectorID)
		builder.Field(3).(*array.Int32Builder).Append(int32(data.ShardID))
		builder.Field(4).(*array.StringBuilder).Append(metric.MeterID)
		builder.Field(5).(*array.Int32Builder).Append(int32(metric.Count))
		builder.Field(6).(*array.Float64Builder).Append(metric.SumKWh)
		builder.Field(7).(*array.Float64Builder).Append(metric.AvgKWh)
		builder.Field(8).(*array.Float64Builder).Append(metric.MinKWh)
		builder.Field(9).(*array.Float64Builder).Append(metric.MaxKWh)
		builder.Field(10).(*array.Float64Builder).Append(metric.AvgVoltage)
		builder.Field(11).(*array.Float64Builder).Append(metric.AvgCurrent)
	}

	rec := builder.NewRecord()
	defer rec.Release()

	descriptor := &flight.FlightDescriptor{
		Type: flight.FlightDescriptor_CMD,
		Cmd:  []byte("put_data"),
	}

	stream, err := fcs.client.DoPut(ctx)
	if err != nil {
		return fmt.Errorf("failed to create DoPut stream: %w", err)
	}

	writer := flight.NewRecordWriter(stream, flight.WithSchema(schema))
	writer.Write(rec)
	writer.Close()

	_, err = stream.Recv()
	if err != nil {
		return fmt.Errorf("failed to receive response: %w", err)
	}

	log.Printf("Sent %d rows via Flight to %s", rec.NumRows(), fcs.addr)
	return nil
}

func (fcs *FlightClientSender) Close() error {
	return fcs.client.Close()
}
