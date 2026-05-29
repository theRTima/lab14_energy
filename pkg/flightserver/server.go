package flightserver

import (
	"fmt"
	"log"
	"sync"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/flight"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/theRTima/lab14_energy/pkg/aggregator"
)

type FlightServer struct {
	flight.BaseFlightServer
	mu     sync.RWMutex
	data   []aggregator.AggregatedData
	alloc  memory.Allocator
}

func NewFlightServer() *FlightServer {
	return &FlightServer{
		data:  make([]aggregator.AggregatedData, 0),
		alloc: memory.NewGoAllocator(),
	}
}

func (fs *FlightServer) AddAggregatedData(data aggregator.AggregatedData) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	fs.data = append(fs.data, data)
	log.Printf("Flight server received aggregated data: %d readings from collector %s",
		data.TotalCount, data.CollectorID)
}

func (fs *FlightServer) DoPut(stream flight.FlightService_DoPutServer) error {
	reader, err := flight.NewRecordReader(stream)
	if err != nil {
		return fmt.Errorf("failed to create reader: %w", err)
	}
	defer reader.Release()

	for reader.Next() {
		rec := reader.Record()
		log.Printf("Received %d rows via DoPut", rec.NumRows())
	}

	if err := reader.Err(); err != nil {
		return fmt.Errorf("reader error: %w", err)
	}

	return stream.Send(&flight.PutResult{})
}

func (fs *FlightServer) DoGet(tkt *flight.Ticket, stream flight.FlightService_DoGetServer) error {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	if len(fs.data) == 0 {
		return fmt.Errorf("no data available")
	}

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

	builder := array.NewRecordBuilder(fs.alloc, schema)
	defer builder.Release()

	for _, aggData := range fs.data {
		for _, metric := range aggData.Metrics {
			builder.Field(0).(*array.TimestampBuilder).Append(arrow.Timestamp(aggData.WindowStart.UnixMicro()))
			builder.Field(1).(*array.TimestampBuilder).Append(arrow.Timestamp(aggData.WindowEnd.UnixMicro()))
			builder.Field(2).(*array.StringBuilder).Append(aggData.CollectorID)
			builder.Field(3).(*array.Int32Builder).Append(int32(aggData.ShardID))
			builder.Field(4).(*array.StringBuilder).Append(metric.MeterID)
			builder.Field(5).(*array.Int32Builder).Append(int32(metric.Count))
			builder.Field(6).(*array.Float64Builder).Append(metric.SumKWh)
			builder.Field(7).(*array.Float64Builder).Append(metric.AvgKWh)
			builder.Field(8).(*array.Float64Builder).Append(metric.MinKWh)
			builder.Field(9).(*array.Float64Builder).Append(metric.MaxKWh)
			builder.Field(10).(*array.Float64Builder).Append(metric.AvgVoltage)
			builder.Field(11).(*array.Float64Builder).Append(metric.AvgCurrent)
		}
	}

	rec := builder.NewRecord()
	defer rec.Release()

	writer := flight.NewRecordWriter(stream)
	defer writer.Close()

	if err := writer.Write(rec); err != nil {
		return fmt.Errorf("failed to write record: %w", err)
	}

	log.Printf("Sent %d rows via Flight", rec.NumRows())
	return nil
}

func (fs *FlightServer) ListFlights(criteria *flight.Criteria, stream flight.FlightService_ListFlightsServer) error {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	info := &flight.FlightInfo{
		Schema: []byte{},
		FlightDescriptor: &flight.FlightDescriptor{
			Type: flight.DescriptorPATH,
			Path: []string{"aggregated_data"},
		},
		Endpoint: []*flight.FlightEndpoint{
			{
				Ticket: &flight.Ticket{Ticket: []byte("aggregated_data")},
			},
		},
		TotalRecords: int64(len(fs.data)),
		TotalBytes:   -1,
	}

	return stream.Send(info)
}

func StartFlightServer(addr string) (*FlightServer, error) {
	fs := NewFlightServer()

	server := flight.NewServerWithMiddleware(nil)
	server.RegisterFlightService(fs)
	server.Init(addr)

	go func() {
		if err := server.Serve(); err != nil {
			log.Fatalf("Flight server failed: %v", err)
		}
	}()

	log.Printf("Flight server started on %s", addr)
	return fs, nil
}
