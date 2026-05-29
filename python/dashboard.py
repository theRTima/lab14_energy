#!/usr/bin/env python3
"""
Streamlit dashboard for real-time energy monitoring.
Consumes aggregated data from Kafka and displays live charts + metrics.
"""
import asyncio
import json
import logging
import os
import threading
import time
from collections import deque
from datetime import datetime

import pandas as pd
import plotly.express as px
import streamlit as st

KAFKA_BROKERS = os.environ.get("KAFKA_BROKERS", "localhost:9092").split(",")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "energy-aggregated")
MAX_POINTS = int(os.environ.get("DASHBOARD_MAX_POINTS", "500"))

logger = logging.getLogger("dashboard")
logging.basicConfig(level=logging.WARNING)

data_lock = threading.Lock()
time_series = deque(maxlen=MAX_POINTS)
collector_stats = {}
meter_metrics = {}
total_readings = 0
window_count = 0
last_update = time.time()


def kafka_worker():
    global total_readings, window_count, last_update

    async def consume():
        global total_readings, window_count, last_update
        from aiokafka import AIOKafkaConsumer
        consumer = AIOKafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BROKERS,
            auto_offset_reset="latest",
            enable_auto_commit=True,
            group_id="energy-dashboard",
        )
        await consumer.start()
        try:
            async for msg in consumer:
                try:
                    data = json.loads(msg.value.decode("utf-8"))
                except (json.JSONDecodeError, UnicodeDecodeError, AttributeError):
                    logger.warning("Skipping malformed message (offset=%d)", msg.offset)
                    continue
                now = time.time()
                with data_lock:
                    total_readings += data.get("total_count", 0)
                    window_count += 1
                    last_update = now

                    ts = data.get("window_end", now * 1e6) / 1e6
                    collector = data.get("collector_id", "?")
                    total_kwh = sum(m.get("sum_kwh", 0) for m in data.get("metrics", []))

                    time_series.append({
                        "time": datetime.fromtimestamp(ts),
                        "collector": collector,
                        "readings": data.get("total_count", 0),
                        "total_kwh": total_kwh,
                        "num_meters": len(data.get("metrics", [])),
                    })

                    cid = data.get("collector_id", "?")
                    if cid not in collector_stats:
                        collector_stats[cid] = {"windows": 0, "readings": 0, "total_kwh": 0.0}
                    collector_stats[cid]["windows"] += 1
                    collector_stats[cid]["readings"] += data.get("total_count", 0)
                    collector_stats[cid]["total_kwh"] += total_kwh

                    for m in data.get("metrics", []):
                        mid = m.get("meter_id")
                        if not mid:
                            continue
                        if mid not in meter_metrics:
                            meter_metrics[mid] = {
                                "count": 0, "sum_kwh": 0.0,
                                "min_kwh": float("inf"), "max_kwh": float("-inf"),
                            }
                        mm = meter_metrics[mid]
                        mm["count"] += m.get("count", 0)
                        mm["sum_kwh"] += m.get("sum_kwh", 0.0)
                        mm["min_kwh"] = min(mm["min_kwh"], m.get("min_kwh", float("inf")))
                        mm["max_kwh"] = max(mm["max_kwh"], m.get("max_kwh", float("-inf")))
        finally:
            await consumer.stop()

    asyncio.run(consume())


if "kafka_thread_started" not in st.session_state:
    st.session_state.kafka_thread_started = True
    thread = threading.Thread(target=kafka_worker, daemon=True)
    thread.start()

st.set_page_config(page_title="Energy Dashboard", layout="wide")
st.title("⚡ Energy Monitoring Dashboard")

placeholder = st.empty()

while True:
    with data_lock:
        ts_copy = list(time_series)
        cs_copy = dict(collector_stats)
        mm_copy = dict(meter_metrics)
        tr = total_readings
        wc = window_count

    with placeholder.container():
        if not ts_copy:
            st.info("Waiting for data from Kafka...")
            time.sleep(1)
            st.rerun()

        latest = ts_copy[-1]
        total_kwh = sum(p["total_kwh"] for p in ts_copy)
        active_meters = len(mm_copy)

        c1, c2, c3, c4 = st.columns(4)
        with c1:
            st.metric("Total Readings", f"{tr:,}", help="Cumulative readings processed")
        with c2:
            st.metric("Active Meters", active_meters, help="Unique meters seen")
        with c3:
            st.metric("Total Energy", f"{total_kwh:,.2f} kWh", help="Cumulative energy")
        with c4:
            rps = tr / (time.time() - (ts_copy[0]["time"].timestamp() if ts_copy else time.time()))
            st.metric("Throughput", f"{rps:,.0f} r/s", help="Readings per second")

        df = pd.DataFrame(ts_copy)

        st.subheader("Energy Readings Over Time")
        if not df.empty:
            fig1 = px.line(df, x="time", y="total_kwh", color="collector",
                           title="Aggregated Energy by Collector",
                           labels={"total_kwh": "kWh", "time": "Time"})
            fig1.update_layout(legend=dict(orientation="h", y=-0.2))
            st.plotly_chart(fig1, use_container_width=True)

        col_left, col_right = st.columns(2)

        with col_left:
            st.subheader("Per-Collector Stats")
            if cs_copy:
                cs_df = pd.DataFrame([
                    {"Collector": cid, "Windows": v["windows"],
                     "Readings": v["readings"], "Total kWh": round(v["total_kwh"], 2)}
                    for cid, v in cs_copy.items()
                ])
                st.dataframe(cs_df, use_container_width=True, hide_index=True)

        with col_right:
            st.subheader("Reading Rate")
            if not df.empty:
                rate_df = df.groupby("collector").agg(
                    readings=("readings", "sum"),
                    windows=("readings", "count"),
                ).reset_index()
                fig2 = px.bar(rate_df, x="collector", y="readings",
                              title="Readings per Collector",
                              labels={"readings": "Readings", "collector": ""})
                st.plotly_chart(fig2, use_container_width=True)

        st.subheader("Meter Energy Distribution")
        if mm_copy:
            meter_df = pd.DataFrame([
                {"Meter": mid, "Avg kWh": round(v["sum_kwh"] / v["count"], 4),
                 "Total kWh": round(v["sum_kwh"], 2), "Readings": v["count"]}
                for mid, v in mm_copy.items()
            ])
            fig3 = px.histogram(meter_df, x="Avg kWh", nbins=30,
                                title="Distribution of Average Meter Energy",
                                labels={"Avg kWh": "kWh"})
            st.plotly_chart(fig3, use_container_width=True)

            with st.expander("Show All Meter Data"):
                st.dataframe(
                    meter_df.sort_values("Total kWh", ascending=False),
                    use_container_width=True, hide_index=True,
                    height=400,
                )

        st.caption(f"Last update: {datetime.fromtimestamp(last_update).strftime('%H:%M:%S')} "
                   f"| Windows received: {wc} | Data points: {len(ts_copy)}")

    time.sleep(2)
    st.rerun()
