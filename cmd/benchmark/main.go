package main

import (
	"encoding/json"
	"fmt"
	"os"
	"runtime"
	"runtime/pprof"
	"time"

	"github.com/theRTima/lab14_energy/pkg/aggregator"
	"github.com/theRTima/lab14_energy/pkg/meter"
	"github.com/theRTima/lab14_energy/pkg/validator"
)

type BenchResult struct {
	Language       string  `json:"language"`
	Meters         int     `json:"meters"`
	Iterations     int     `json:"iterations"`
	TotalReadings  int     `json:"total_readings"`
	DurationMs     float64 `json:"duration_ms"`
	ReadingsPerSec float64 `json:"readings_per_sec"`
	AvgUsPerRead   float64 `json:"avg_us_per_read"`
	PeakMemMB      float64 `json:"peak_mem_mb"`
	AvgCPUPercent  float64 `json:"avg_cpu_percent"`
	WindowFlushes  int     `json:"window_flushes"`
}

func main() {
	meters := 100
	if len(os.Args) > 1 {
		fmt.Sscanf(os.Args[1], "%d", &meters)
	}
	iterations := 10
	if len(os.Args) > 2 {
		fmt.Sscanf(os.Args[2], "%d", &iterations)
	}
	windowTime := 30 * time.Second

	var m1, m2 runtime.MemStats

	runtime.ReadMemStats(&m1)

	start := time.Now()

	pool := meter.NewMeterPool(meters)
	meterList := pool.GetMetersByRange(0, meters)

	flushCount := 0
	flushCallback := func(data aggregator.AggregatedData) {
		flushCount++
		_ = data
	}

	window := aggregator.NewTumblingWindow(windowTime, 0, 0, "bench-collector", flushCallback)
	defer window.Stop()

	totalReadings := 0

	for iter := 0; iter < iterations; iter++ {
		for _, m := range meterList {
			reading := m.GenerateReading()
			if err := validator.ValidateReading(reading); err != nil {
				continue
			}
			window.Add(reading)
			totalReadings++
		}
	}

	window.Flush()
	elapsed := time.Since(start)

	runtime.ReadMemStats(&m2)

	var memStatsToUse runtime.MemStats
	if m2.Alloc > m1.Alloc {
		memStatsToUse = m2
	} else {
		memStatsToUse = m1
	}

	peakMem := float64(memStatsToUse.Alloc) / 1024 / 1024

	result := BenchResult{
		Language:       "Go",
		Meters:         meters,
		Iterations:     iterations,
		TotalReadings:  totalReadings,
		DurationMs:     float64(elapsed.Microseconds()) / 1000.0,
		ReadingsPerSec: float64(totalReadings) / elapsed.Seconds(),
		AvgUsPerRead:   float64(elapsed.Microseconds()) / float64(totalReadings),
		PeakMemMB:      peakMem,
		AvgCPUPercent:  0,
		WindowFlushes:  flushCount,
	}

	if cpuFile := os.Getenv("CPU_PROFILE"); cpuFile != "" {
		f, _ := os.Create(cpuFile)
		if f != nil {
			pprof.StartCPUProfile(f)
			time.Sleep(2 * time.Second)
			pprof.StopCPUProfile()
			f.Close()
		}
	}

	out, _ := json.Marshal(result)
	fmt.Println(string(out))
}
