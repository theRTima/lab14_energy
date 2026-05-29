#!/usr/bin/env python3
"""
Real-time Kafka consumer for aggregated energy data.
Reads from the 'energy-aggregated' topic and computes live statistics.
"""
import json
import logging
import os
import signal
import sys
import time
from collections import defaultdict

from kafka import KafkaConsumer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("kafka-analyzer")

TOPIC = os.environ.get("KAFKA_TOPIC", "energy-aggregated")
BROKERS = os.environ.get("KAFKA_BROKERS", "localhost:9092").split(",")

accumulated_readings = 0
accumulated_kwh = 0.0
meter_stats = defaultdict(lambda: {
    "count": 0,
    "sum_kwh": 0.0,
    "min_kwh": float("inf"),
    "max_kwh": float("-inf"),
    "sum_voltage": 0.0,
    "sum_current": 0.0,
})
window_count = 0
last_report = time.time()
REPORT_INTERVAL = int(os.environ.get("REPORT_INTERVAL", "30"))


def print_report():
    global last_report, accumulated_readings, accumulated_kwh, window_count

    elapsed = time.time() - last_report
    if accumulated_readings == 0:
        logger.info("No data received in the last %ds", REPORT_INTERVAL)
        last_report = time.time()
        return

    active_meters = len(meter_stats)
    total_kwh = sum(s["sum_kwh"] for s in meter_stats.values())
    total_readings = sum(s["count"] for s in meter_stats.values())
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
    logger.info("Active Meters: %d", active_meters)
    logger.info("Total Energy: %.2f kWh", total_kwh)
    logger.info("Average Power: %.4f kWh", total_kwh / total_readings if total_readings else 0)
    logger.info("Min Power: %.2f kWh", min_kwh if min_kwh != float("inf") else 0)
    logger.info("Max Power: %.2f kWh", max_kwh if max_kwh != float("-inf") else 0)
    logger.info("Average Voltage: %.1f V", total_voltage / total_readings if total_readings else 0)
    logger.info("Average Current: %.2f A", total_current / total_readings if total_readings else 0)
    logger.info("Rate: %.0f readings/sec", total_readings / elapsed if elapsed else 0)
    logger.info("=" * 50)

    if os.environ.get("PER_METER_BREAKDOWN"):
        logger.info("Per-Meter Breakdown:")
        for mid, s in sorted(meter_stats.items()):
            logger.info("  %s: %d readings, %.2f kWh total, %.2f avg",
                        mid, s["count"], s["sum_kwh"],
                        s["sum_kwh"] / s["count"] if s["count"] else 0)

    accumulated_readings = 0
    accumulated_kwh = 0.0
    meter_stats.clear()
    window_count = 0
    last_report = time.time()


def process_message(msg):
    global accumulated_readings, accumulated_kwh, window_count

    try:
        data = json.loads(msg.value)
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        logger.warning("Failed to decode message: %s", e)
        return

    collector_id = data.get("collector_id", "?")
    total_count = data.get("total_count", 0)
    metrics = data.get("metrics", [])

    window_count += 1
    accumulated_readings += total_count

    for m in metrics:
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
                 collector_id, total_count, len(metrics))


def main():
    logger.info("Starting Kafka analyzer: brokers=%s topic=%s report_interval=%ds",
                BROKERS, TOPIC, REPORT_INTERVAL)

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BROKERS,
        value_deserializer=lambda v: v.decode("utf-8"),
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id="energy-analyzer",
    )

    def shutdown(sig, frame):
        logger.info("Shutting down...")
        consumer.close()
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    try:
        for msg in consumer:
            process_message(msg)

            if time.time() - last_report >= REPORT_INTERVAL:
                print_report()
    except KeyboardInterrupt:
        pass
    finally:
        if accumulated_readings > 0:
            print_report()
        consumer.close()


if __name__ == "__main__":
    main()
