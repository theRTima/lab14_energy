#!/usr/bin/env python3
import asyncio
import json
import logging
import os
import signal
import time
from collections import defaultdict

from aiokafka import AIOKafkaConsumer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("kafka-analyzer")

TOPIC = os.environ.get("KAFKA_TOPIC", "energy-aggregated")
BROKERS = os.environ.get("KAFKA_BROKERS", "localhost:9092").split(",")
REPORT_INTERVAL = int(os.environ.get("REPORT_INTERVAL", "30"))

meter_stats = defaultdict(lambda: {
    "count": 0, "sum_kwh": 0.0,
    "min_kwh": float("inf"), "max_kwh": float("-inf"),
    "sum_voltage": 0.0, "sum_current": 0.0,
})
window_count = 0
last_report = time.time()


def print_report():
    global last_report, window_count
    elapsed = time.time() - last_report
    if not meter_stats:
        logger.info("No data received in the last %ds", REPORT_INTERVAL)
        last_report = time.time()
        return

    total_readings = sum(s["count"] for s in meter_stats.values())
    total_kwh = sum(s["sum_kwh"] for s in meter_stats.values())
    total_voltage = sum(s["sum_voltage"] for s in meter_stats.values())
    total_current = sum(s["sum_current"] for s in meter_stats.values())
    min_kwh = min(s["min_kwh"] for s in meter_stats.values())
    max_kwh = max(s["max_kwh"] for s in meter_stats.values())

    logger.info("=" * 50)
    logger.info("Real-Time Energy Analysis")
    logger.info("-" * 50)
    logger.info("Time Window: last %ds", elapsed)
    logger.info("Windows Received: %d", window_count)
    logger.info("Total Readings: %d", total_readings)
    logger.info("Active Meters: %d", len(meter_stats))
    logger.info("Total Energy: %.2f kWh", total_kwh)
    logger.info("Average Power: %.4f kWh", total_kwh / total_readings if total_readings else 0)
    logger.info("Min Power: %.2f kWh", min_kwh if min_kwh != float("inf") else 0)
    logger.info("Max Power: %.2f kWh", max_kwh if max_kwh != float("-inf") else 0)
    logger.info("Average Voltage: %.1f V", total_voltage / total_readings if total_readings else 0)
    logger.info("Average Current: %.2f A", total_current / total_readings if total_readings else 0)
    logger.info("Rate: %.0f readings/sec", total_readings / elapsed if elapsed else 0)
    logger.info("=" * 50)

    meter_stats.clear()
    window_count = 0
    last_report = time.time()


async def main():
    global window_count
    logger.info("Starting Kafka analyzer: brokers=%s topic=%s", BROKERS, TOPIC)

    consumer = AIOKafkaConsumer(
        TOPIC,
        bootstrap_servers=BROKERS,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id="energy-analyzer",
    )

    stop = asyncio.Event()

    def _shutdown():
        stop.set()

    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _shutdown)

    await consumer.start()
    try:
        while not stop.is_set():
            get_msg = asyncio.create_task(consumer.getone())
            wait_stop = asyncio.create_task(stop.wait())
            done, _ = await asyncio.wait(
                [get_msg, wait_stop],
                return_when=asyncio.FIRST_COMPLETED,
            )

            if wait_stop in done:
                get_msg.cancel()
                break

            msg = get_msg.result()
            try:
                data = json.loads(msg.value.decode("utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError, AttributeError):
                logger.warning("Skipping malformed message (offset=%d)", msg.offset)
                continue

            window_count += 1

            for m in data.get("metrics", []):
                mid = m.get("meter_id")
                if not mid:
                    continue
                s = meter_stats[mid]
                s["count"] += m.get("count", 0)
                s["sum_kwh"] += m.get("sum_kwh", 0.0)
                s["min_kwh"] = min(s["min_kwh"], m.get("min_kwh", float("inf")))
                s["max_kwh"] = max(s["max_kwh"], m.get("max_kwh", float("-inf")))
                s["sum_voltage"] += m.get("avg_voltage", 0.0) * m.get("count", 0)
                s["sum_current"] += m.get("avg_current", 0.0) * m.get("count", 0)

            logger.debug("Processed window from %s: %d readings from %d meters",
                         data.get("collector_id", "?"), data.get("total_count", 0),
                         len(data.get("metrics", [])))

            if time.time() - last_report >= REPORT_INTERVAL:
                print_report()

    except asyncio.CancelledError:
        pass
    finally:
        await consumer.stop()
        if meter_stats:
            print_report()


if __name__ == "__main__":
    asyncio.run(main())
