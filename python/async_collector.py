import asyncio
import math
import random
import time
import logging
from dataclasses import dataclass, field
from typing import Optional

logger = logging.getLogger(__name__)

# --- Validation (pure Python, matching Rust logic) ---

ERROR_NONE = 0
ERROR_KWH_OUT_OF_RANGE = 1
ERROR_VOLTAGE_OUT_OF_RANGE = 2
ERROR_CURRENT_OUT_OF_RANGE = 3
ERROR_POWER_MISMATCH = 4

MIN_KWH = 0.0
MAX_KWH = 50.0
MIN_VOLTAGE = 200.0
MAX_VOLTAGE = 240.0
MIN_CURRENT = 0.0
MAX_CURRENT = 100.0

ERROR_MESSAGES = {
    ERROR_NONE: "No error",
    ERROR_KWH_OUT_OF_RANGE: "Power consumption out of valid range",
    ERROR_VOLTAGE_OUT_OF_RANGE: "Voltage out of valid range",
    ERROR_CURRENT_OUT_OF_RANGE: "Current out of valid range",
    ERROR_POWER_MISMATCH: "Power calculation mismatch",
}


@dataclass
class MeterReading:
    meter_id: str
    timestamp: float
    kwh: float
    voltage: float
    current: float


class ValidationError(Exception):
    def __init__(self, code: int, message: str):
        self.code = code
        self.message = message
        super().__init__(f"validation error [{code}]: {message}")


def validate_reading(reading: MeterReading) -> None:
    if reading.kwh < MIN_KWH or reading.kwh > MAX_KWH:
        raise ValidationError(ERROR_KWH_OUT_OF_RANGE, ERROR_MESSAGES[ERROR_KWH_OUT_OF_RANGE])
    if reading.voltage < MIN_VOLTAGE or reading.voltage > MAX_VOLTAGE:
        raise ValidationError(ERROR_VOLTAGE_OUT_OF_RANGE, ERROR_MESSAGES[ERROR_VOLTAGE_OUT_OF_RANGE])
    if reading.current < MIN_CURRENT or reading.current > MAX_CURRENT:
        raise ValidationError(ERROR_CURRENT_OUT_OF_RANGE, ERROR_MESSAGES[ERROR_CURRENT_OUT_OF_RANGE])
    calculated_power = (reading.voltage * reading.current) / 1000.0
    power_diff = abs(calculated_power - reading.kwh)
    tolerance = reading.kwh * 0.15
    if power_diff > tolerance:
        raise ValidationError(ERROR_POWER_MISMATCH, ERROR_MESSAGES[ERROR_POWER_MISMATCH])


# --- Meter ---

class Meter:
    def __init__(self, meter_id: str, base_load: float):
        self.id = meter_id
        self.base_load = base_load
        self._rand = random.Random(time.time_ns() + len(meter_id))

    def generate_reading(self) -> MeterReading:
        hour = time.localtime().tm_hour
        if 6 <= hour < 9:
            time_factor = 1.3
        elif 18 <= hour < 22:
            time_factor = 1.5
        elif 0 <= hour < 6:
            time_factor = 0.6
        else:
            time_factor = 1.0

        noise = self._rand.random() * 0.2 - 0.1
        kwh = self.base_load * time_factor * (1 + noise)

        voltage = 220.0 + self._rand.random() * 10 - 5
        current = (kwh * 1000) / voltage

        return MeterReading(
            meter_id=self.id,
            timestamp=time.time(),
            kwh=round(kwh, 2),
            voltage=round(voltage, 1),
            current=round(current, 2),
        )


class MeterPool:
    def __init__(self, count: int):
        self.meters = []
        for i in range(count):
            base_load = 2.0 + random.random() * 8.0
            self.meters.append(Meter(f"METER-{i:04d}", base_load))

    def get_meters_by_range(self, start: int, end: int):
        return self.meters[start:end]


# --- Aggregator ---

@dataclass
class MeterMetric:
    meter_id: str
    count: int = 0
    sum_kwh: float = 0.0
    sum_voltage: float = 0.0
    sum_current: float = 0.0
    avg_kwh: float = 0.0
    min_kwh: float = 0.0
    max_kwh: float = 0.0
    avg_voltage: float = 0.0
    avg_current: float = 0.0
    first_reading: float = 0.0
    last_reading: float = 0.0


@dataclass
class AggregatedData:
    window_start: float
    window_end: float
    shard_id: int
    collector_id: str
    total_count: int = 0
    metrics: dict = field(default_factory=dict)


class TumblingWindow:
    def __init__(self, time_window: float, count_window: int,
                 shard_id: int, collector_id: str,
                 flush_callback):
        self.time_window = time_window
        self.count_window = count_window
        self.shard_id = shard_id
        self.collector_id = collector_id
        self.flush_callback = flush_callback

        self._lock = asyncio.Lock()
        self._current_window = self._new_window()
        self._stop_event = asyncio.Event()
        self._flush_task: Optional[asyncio.Task] = None
        self._started = False

    def start(self):
        if self.time_window > 0 and not self._started:
            self._flush_task = asyncio.create_task(self._time_based_flusher())
            self._started = True

    def _new_window(self):
        return {
            "start": time.time(),
            "metrics": {},
            "count": 0,
        }

    async def _time_based_flusher(self):
        try:
            while not self._stop_event.is_set():
                await asyncio.sleep(self.time_window)
                async with self._lock:
                    await self._flush_locked()
        except asyncio.CancelledError:
            pass

    async def add(self, reading: MeterReading):
        async with self._lock:
            acc = self._current_window["metrics"].get(reading.meter_id)
            if acc is None:
                acc = MeterMetric(
                    meter_id=reading.meter_id,
                    min_kwh=reading.kwh,
                    max_kwh=reading.kwh,
                    first_reading=reading.timestamp,
                )
                self._current_window["metrics"][reading.meter_id] = acc

            acc.count += 1
            acc.sum_kwh += reading.kwh
            acc.sum_voltage += reading.voltage
            acc.sum_current += reading.current
            acc.last_reading = reading.timestamp

            if reading.kwh < acc.min_kwh:
                acc.min_kwh = reading.kwh
            if reading.kwh > acc.max_kwh:
                acc.max_kwh = reading.kwh

            self._current_window["count"] += 1

            if self.count_window > 0 and self._current_window["count"] >= self.count_window:
                await self._flush_locked()

    async def _flush_locked(self):
        if self._current_window["count"] == 0:
            return

        window_end = time.time()
        metrics = {}
        for mid, acc in self._current_window["metrics"].items():
            metrics[mid] = MeterMetric(
                meter_id=acc.meter_id,
                count=acc.count,
                sum_kwh=acc.sum_kwh,
                avg_kwh=acc.sum_kwh / acc.count,
                min_kwh=acc.min_kwh,
                max_kwh=acc.max_kwh,
                avg_voltage=acc.sum_voltage / acc.count,
                avg_current=acc.sum_current / acc.count,
                first_reading=acc.first_reading,
                last_reading=acc.last_reading,
            )

        data = AggregatedData(
            window_start=self._current_window["start"],
            window_end=window_end,
            shard_id=self.shard_id,
            collector_id=self.collector_id,
            total_count=self._current_window["count"],
            metrics=metrics,
        )

        self._current_window = self._new_window()

        if self.flush_callback:
            await self.flush_callback(data)

    async def flush(self):
        async with self._lock:
            await self._flush_locked()

    async def stop(self):
        self._stop_event.set()
        if self._flush_task:
            self._flush_task.cancel()
            try:
                await self._flush_task
            except asyncio.CancelledError:
                pass
        await self.flush()


# --- Async Collector ---

class AsyncCollector:
    def __init__(self, *, collector_id: str, total_meters: int,
                 num_shards: int, shard_id: int,
                 meters_per_shard_start: int, meters_per_shard_end: int,
                 interval: float, window_time: float,
                 window_count: int = 0,
                 flush_callback=None):
        self.collector_id = collector_id
        self.total_meters = total_meters
        self.num_shards = num_shards
        self.shard_id = shard_id
        self.interval = interval
        self.window_time = window_time
        self.window_count = window_count

        self.meter_pool = MeterPool(total_meters)
        self.meters = self.meter_pool.get_meters_by_range(
            meters_per_shard_start, meters_per_shard_end)

        self.window = TumblingWindow(
            time_window=window_time,
            count_window=window_count,
            shard_id=shard_id,
            collector_id=collector_id,
            flush_callback=flush_callback,
        )

        self.reading_count = 0
        self.valid_count = 0
        self.invalid_count = 0
        self._stop_event = asyncio.Event()

    async def run(self):
        self.window.start()
        logger.info("Collector %s managing %d meters (window: %ss, count: %d)",
                     self.collector_id, len(self.meters), self.window_time, self.window_count)

        while not self._stop_event.is_set():
            await asyncio.sleep(self.interval)

            for m in self.meters:
                reading = m.generate_reading()

                try:
                    validate_reading(reading)
                except ValidationError as e:
                    logger.debug("Invalid reading from %s: %s", m.id, e)
                    self.invalid_count += 1
                    continue

                await self.window.add(reading)
                self.reading_count += 1
                self.valid_count += 1

            logger.info("Collector %s: collected %d readings (valid: %d, invalid: %d, total: %d)",
                         self.collector_id, len(self.meters), self.valid_count,
                         self.invalid_count, self.reading_count)

    async def stop(self):
        self._stop_event.set()
        await self.window.stop()
