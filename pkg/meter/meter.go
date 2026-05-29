package meter

import (
	"fmt"
	"math"
	"math/rand"
	"time"
)

type Reading struct {
	MeterID   string    `json:"meter_id"`
	Timestamp time.Time `json:"timestamp"`
	KWh       float64   `json:"kwh"`
	Voltage   float64   `json:"voltage"`
	Current   float64   `json:"current"`
}

type Meter struct {
	ID       string
	BaseLoad float64
	rand     *rand.Rand
}

func NewMeter(id string, baseLoad float64) *Meter {
	return &Meter{
		ID:       id,
		BaseLoad: baseLoad,
		rand:     rand.New(rand.NewSource(time.Now().UnixNano() + int64(len(id)))),
	}
}

func (m *Meter) GenerateReading() Reading {
	hour := time.Now().Hour()

	timeFactor := 1.0
	if hour >= 6 && hour < 9 {
		timeFactor = 1.3
	} else if hour >= 18 && hour < 22 {
		timeFactor = 1.5
	} else if hour >= 0 && hour < 6 {
		timeFactor = 0.6
	}

	noise := m.rand.Float64()*0.2 - 0.1
	kwh := m.BaseLoad * timeFactor * (1 + noise)

	voltage := 220.0 + m.rand.Float64()*10 - 5
	current := (kwh * 1000) / voltage

	return Reading{
		MeterID:   m.ID,
		Timestamp: time.Now(),
		KWh:       math.Round(kwh*100) / 100,
		Voltage:   math.Round(voltage*10) / 10,
		Current:   math.Round(current*100) / 100,
	}
}

type MeterPool struct {
	meters []*Meter
}

func NewMeterPool(count int) *MeterPool {
	meters := make([]*Meter, count)
	for i := 0; i < count; i++ {
		baseLoad := 2.0 + rand.Float64()*8.0
		meters[i] = NewMeter(fmt.Sprintf("METER-%04d", i), baseLoad)
	}
	return &MeterPool{meters: meters}
}

func (mp *MeterPool) GetMeter(id string) *Meter {
	for _, m := range mp.meters {
		if m.ID == id {
			return m
		}
	}
	return nil
}

func (mp *MeterPool) GetMetersByRange(start, end int) []*Meter {
	if start < 0 {
		start = 0
	}
	if end > len(mp.meters) {
		end = len(mp.meters)
	}
	return mp.meters[start:end]
}

func (mp *MeterPool) TotalCount() int {
	return len(mp.meters)
}
