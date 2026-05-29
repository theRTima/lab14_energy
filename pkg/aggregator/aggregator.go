package aggregator

import (
	"sync"
	"time"

	"github.com/theRTima/lab14_energy/pkg/meter"
)

type AggregatedData struct {
	WindowStart time.Time              `json:"window_start"`
	WindowEnd   time.Time              `json:"window_end"`
	ShardID     int                    `json:"shard_id"`
	CollectorID string                 `json:"collector_id"`
	TotalCount  int                    `json:"total_count"`
	Metrics     map[string]MeterMetric `json:"metrics"`
}

type MeterMetric struct {
	MeterID      string  `json:"meter_id"`
	Count        int     `json:"count"`
	SumKWh       float64 `json:"sum_kwh"`
	AvgKWh       float64 `json:"avg_kwh"`
	MinKWh       float64 `json:"min_kwh"`
	MaxKWh       float64 `json:"max_kwh"`
	AvgVoltage   float64 `json:"avg_voltage"`
	AvgCurrent   float64 `json:"avg_current"`
	FirstReading time.Time `json:"first_reading"`
	LastReading  time.Time `json:"last_reading"`
}

type TumblingWindow struct {
	timeWindow  time.Duration
	countWindow int
	shardID     int
	collectorID string

	mu            sync.Mutex
	currentWindow *windowState
	flushCallback func(AggregatedData)
	ticker        *time.Ticker
	stopChan      chan struct{}
}

type windowState struct {
	start   time.Time
	metrics map[string]*meterAccumulator
	count   int
}

type meterAccumulator struct {
	meterID      string
	count        int
	sumKWh       float64
	sumVoltage   float64
	sumCurrent   float64
	minKWh       float64
	maxKWh       float64
	firstReading time.Time
	lastReading  time.Time
}

func NewTumblingWindow(timeWindow time.Duration, countWindow int, shardID int, collectorID string, flushCallback func(AggregatedData)) *TumblingWindow {
	tw := &TumblingWindow{
		timeWindow:    timeWindow,
		countWindow:   countWindow,
		shardID:       shardID,
		collectorID:   collectorID,
		flushCallback: flushCallback,
		stopChan:      make(chan struct{}),
	}

	tw.currentWindow = &windowState{
		start:   time.Now(),
		metrics: make(map[string]*meterAccumulator),
	}

	if timeWindow > 0 {
		tw.ticker = time.NewTicker(timeWindow)
		go tw.timeBasedFlusher()
	}

	return tw
}

func (tw *TumblingWindow) Add(reading meter.Reading) {
	tw.mu.Lock()
	defer tw.mu.Unlock()

	acc, exists := tw.currentWindow.metrics[reading.MeterID]
	if !exists {
		acc = &meterAccumulator{
			meterID:      reading.MeterID,
			minKWh:       reading.KWh,
			maxKWh:       reading.KWh,
			firstReading: reading.Timestamp,
		}
		tw.currentWindow.metrics[reading.MeterID] = acc
	}

	acc.count++
	acc.sumKWh += reading.KWh
	acc.sumVoltage += reading.Voltage
	acc.sumCurrent += reading.Current
	acc.lastReading = reading.Timestamp

	if reading.KWh < acc.minKWh {
		acc.minKWh = reading.KWh
	}
	if reading.KWh > acc.maxKWh {
		acc.maxKWh = reading.KWh
	}

	tw.currentWindow.count++

	if tw.countWindow > 0 && tw.currentWindow.count >= tw.countWindow {
		tw.flushLocked()
	}
}

func (tw *TumblingWindow) timeBasedFlusher() {
	for {
		select {
		case <-tw.ticker.C:
			tw.mu.Lock()
			tw.flushLocked()
			tw.mu.Unlock()
		case <-tw.stopChan:
			return
		}
	}
}

func (tw *TumblingWindow) flushLocked() {
	if tw.currentWindow.count == 0 {
		return
	}

	aggregated := AggregatedData{
		WindowStart: tw.currentWindow.start,
		WindowEnd:   time.Now(),
		ShardID:     tw.shardID,
		CollectorID: tw.collectorID,
		TotalCount:  tw.currentWindow.count,
		Metrics:     make(map[string]MeterMetric),
	}

	for meterID, acc := range tw.currentWindow.metrics {
		aggregated.Metrics[meterID] = MeterMetric{
			MeterID:      acc.meterID,
			Count:        acc.count,
			SumKWh:       acc.sumKWh,
			AvgKWh:       acc.sumKWh / float64(acc.count),
			MinKWh:       acc.minKWh,
			MaxKWh:       acc.maxKWh,
			AvgVoltage:   acc.sumVoltage / float64(acc.count),
			AvgCurrent:   acc.sumCurrent / float64(acc.count),
			FirstReading: acc.firstReading,
			LastReading:  acc.lastReading,
		}
	}

	tw.currentWindow = &windowState{
		start:   time.Now(),
		metrics: make(map[string]*meterAccumulator),
	}

	if tw.flushCallback != nil {
		go tw.flushCallback(aggregated)
	}
}

func (tw *TumblingWindow) Flush() {
	tw.mu.Lock()
	defer tw.mu.Unlock()
	tw.flushLocked()
}

func (tw *TumblingWindow) Stop() {
	if tw.ticker != nil {
		tw.ticker.Stop()
	}
	close(tw.stopChan)
	tw.Flush()
}
