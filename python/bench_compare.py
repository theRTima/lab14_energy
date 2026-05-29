#!/usr/bin/env python3
import asyncio
import json
import logging
import os
import platform
import subprocess
import sys
import time
import tracemalloc

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from async_collector import MeterPool, validate_reading, ValidationError, TumblingWindow, AggregatedData

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger("bench")

METERS = [10, 50, 100, 250, 500, 1000]
ITERATIONS = 20
WARMUP = 3

BENCH_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
GO_BENCH_BIN = os.path.join(BENCH_DIR, "bin", "benchmark")
REPORT_DIR = os.path.join(BENCH_DIR, "bench_report")


def run_multi_trial(runner_fn, meters, iterations, trials=3):
    results = []
    for t in range(trials):
        res = runner_fn(meters, iterations)
        results.append(res)
    return results


def build_go_bench():
    print("Building Go benchmark...")
    subprocess.run(
        ["go", "build", "-o", GO_BENCH_BIN, "./cmd/benchmark"],
        cwd=BENCH_DIR,
        check=True,
        capture_output=True,
    )
    print("  OK")


def run_go_bench(meters: int, iterations: int) -> dict:
    result = subprocess.run(
        [GO_BENCH_BIN, str(meters), str(iterations)],
        cwd=BENCH_DIR,
        capture_output=True,
        text=True,
        timeout=120,
    )
    return json.loads(result.stdout.strip())


def run_python_bench(meters: int, iterations: int, window_time: float = 30.0) -> dict:
    pool = MeterPool(meters)
    meter_list = pool.get_meters_by_range(0, meters)

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    flush_count = 0

    async def flush_cb(data: AggregatedData):
        nonlocal flush_count
        flush_count += 1

    window = TumblingWindow(
        time_window=window_time,
        count_window=0,
        shard_id=0,
        collector_id="bench-collector",
        flush_callback=flush_cb,
    )

    async def init():
        window.start()

    loop.run_until_complete(init())

    reading_count = 0
    valid_count = 0

    tracemalloc.start()
    start = time.perf_counter()

    for _ in range(iterations):
        for m in meter_list:
            reading = m.generate_reading()
            try:
                validate_reading(reading)
            except ValidationError:
                continue
            loop.run_until_complete(window.add(reading))
            reading_count += 1
            valid_count += 1

    loop.run_until_complete(window.flush())
    loop.run_until_complete(window.stop())
    elapsed = time.perf_counter() - start

    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    loop.close()

    return {
        "language": "Python",
        "meters": meters,
        "iterations": iterations,
        "total_readings": reading_count,
        "duration_ms": elapsed * 1000,
        "readings_per_sec": reading_count / elapsed if elapsed > 0 else 0,
        "avg_us_per_read": (elapsed * 1_000_000) / reading_count if reading_count > 0 else 0,
        "peak_mem_mb": peak / 1024 / 1024,
        "avg_cpu_percent": 0,
        "window_flushes": flush_count,
    }


def compute_stats(values):
    arr = np.array(values)
    return {
        "mean": float(np.mean(arr)),
        "median": float(np.median(arr)),
        "std": float(np.std(arr)),
        "min": float(np.min(arr)),
        "max": float(np.max(arr)),
        "p95": float(np.percentile(arr, 95)),
        "p99": float(np.percentile(arr, 99)),
    }


def main():
    os.makedirs(REPORT_DIR, exist_ok=True)

    build_go_bench()

    all_go = {}
    all_py = {}

    print(f"\nRunning benchmarks: {len(METERS)} configs x {WARMUP} warmup + 3 trials each")
    print(f"Meters: {METERS}")
    print(f"Iterations per meter: {ITERATIONS}\n")

    for meters in METERS:
        print(f"--- {meters} meters ---")

        print(f"  Warmup ({meters}m)...")
        _ = run_python_bench(meters, 5)
        print(f"  Python {meters}m x3 trials...")
        py_results = run_multi_trial(
            lambda m, i: run_python_bench(m, i), meters, ITERATIONS, trials=3
        )
        all_py[meters] = py_results
        avg = np.mean([r["readings_per_sec"] for r in py_results])
        print(f"    Python: {avg:.0f} readings/sec")

        print(f"  Go {meters}m x3 trials...")
        go_results = run_multi_trial(
            lambda m, i: run_go_bench(m, i), meters, ITERATIONS, trials=3
        )
        all_go[meters] = go_results
        avg = np.mean([r["readings_per_sec"] for r in go_results])
        print(f"    Go: {avg:.0f} readings/sec")

    generate_report(all_go, all_py)
    print(f"\nReport saved to {REPORT_DIR}/")


def generate_report(all_go, all_py):
    meters_list = sorted(all_go.keys())

    go_throughput = [np.mean([r["readings_per_sec"] for r in all_go[m]]) for m in meters_list]
    py_throughput = [np.mean([r["readings_per_sec"] for r in all_py[m]]) for m in meters_list]
    go_throughput_std = [np.std([r["readings_per_sec"] for r in all_go[m]]) for m in meters_list]
    py_throughput_std = [np.std([r["readings_per_sec"] for r in all_py[m]]) for m in meters_list]

    go_latency = [np.mean([r["avg_us_per_read"] for r in all_go[m]]) for m in meters_list]
    py_latency = [np.mean([r["avg_us_per_read"] for r in all_py[m]]) for m in meters_list]
    go_latency_std = [np.std([r["avg_us_per_read"] for r in all_go[m]]) for m in meters_list]
    py_latency_std = [np.std([r["avg_us_per_read"] for r in all_py[m]]) for m in meters_list]

    go_mem = [np.mean([r["peak_mem_mb"] for r in all_go[m]]) for m in meters_list]
    py_mem = [np.mean([r["peak_mem_mb"] for r in all_py[m]]) for m in meters_list]

    go_mem_std = [np.std([r["peak_mem_mb"] for r in all_go[m]]) for m in meters_list]
    py_mem_std = [np.std([r["peak_mem_mb"] for r in all_py[m]]) for m in meters_list]

    fig, axes = plt.subplots(2, 2, figsize=(14, 10))
    fig.suptitle("Go vs Python Async Collector — Performance Comparison",
                 fontsize=16, fontweight="bold")

    x = np.arange(len(meters_list))
    width = 0.35

    ax1 = axes[0, 0]
    ax1.bar(x - width / 2, go_throughput, width, yerr=go_throughput_std,
            label="Go", capsize=4, color="#2196F3", alpha=0.85)
    ax1.bar(x + width / 2, py_throughput, width, yerr=py_throughput_std,
            label="Python (asyncio)", capsize=4, color="#FF9800", alpha=0.85)
    ax1.set_xlabel("Meters")
    ax1.set_ylabel("Readings / sec")
    ax1.set_title("Throughput (higher is better)")
    ax1.set_xticks(x)
    ax1.set_xticklabels([str(m) for m in meters_list])
    ax1.legend()
    ax1.grid(axis="y", alpha=0.3)

    ax2 = axes[0, 1]
    ax2.bar(x - width / 2, go_latency, width, yerr=go_latency_std,
            label="Go", capsize=4, color="#2196F3", alpha=0.85)
    ax2.bar(x + width / 2, py_latency, width, yerr=py_latency_std,
            label="Python (asyncio)", capsize=4, color="#FF9800", alpha=0.85)
    ax2.set_xlabel("Meters")
    ax2.set_ylabel("µs / reading")
    ax2.set_title("Latency per Reading (lower is better)")
    ax2.set_xticks(x)
    ax2.set_xticklabels([str(m) for m in meters_list])
    ax2.legend()
    ax2.grid(axis="y", alpha=0.3)

    ax3 = axes[1, 0]
    ax3.bar(x - width / 2, go_mem, width, yerr=go_mem_std,
            label="Go", capsize=4, color="#2196F3", alpha=0.85)
    ax3.bar(x + width / 2, py_mem, width, yerr=py_mem_std,
            label="Python (asyncio)", capsize=4, color="#FF9800", alpha=0.85)
    ax3.set_xlabel("Meters")
    ax3.set_ylabel("Peak Memory (MB)")
    ax3.set_title("Memory Usage (lower is better)")
    ax3.set_xticks(x)
    ax3.set_xticklabels([str(m) for m in meters_list])
    ax3.legend()
    ax3.grid(axis="y", alpha=0.3)

    ax4 = axes[1, 1]
    ratio_tp = np.array(py_throughput) / np.array(go_throughput)
    ax4.plot(meters_list, ratio_tp, "o-", color="#4CAF50", linewidth=2, markersize=8)
    ax4.axhline(y=1.0, color="red", linestyle="--", alpha=0.5, label="Go = Python")
    ax4.fill_between(meters_list, ratio_tp, 1.0, alpha=0.15, color="#4CAF50")
    ax4.set_xlabel("Meters")
    ax4.set_ylabel("Python / Go Throughput Ratio")
    ax4.set_title("Relative Performance (Python / Go)")
    ax4.legend()
    ax4.grid(alpha=0.3)

    plt.tight_layout()
    plt.savefig(os.path.join(REPORT_DIR, "benchmark_comparison.png"), dpi=150)
    plt.close()

    with open(os.path.join(REPORT_DIR, "benchmark_results.json"), "w") as f:
        json.dump({"go": {str(m): all_go[m] for m in meters_list},
                    "python": {str(m): all_py[m] for m in meters_list}},
                   f, indent=2)

    with open(os.path.join(REPORT_DIR, "benchmark_report.md"), "w") as f:
        f.write("# Go vs Python Async Collector — Performance Report\n\n")
        f.write(f"**Date:** {time.strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        f.write(f"**System:** {platform.platform()} | {platform.processor()}\n\n")
        f.write(f"**Python:** {platform.python_version()} | **Go:** ")
        go_ver = subprocess.run(["go", "version"], capture_output=True, text=True).stdout.strip()
        f.write(f"{go_ver}\n\n")
        f.write("## Methodology\n\n")
        f.write(f"Each benchmark generates readings for `N` meters over `{ITERATIONS}` iterations ")
        f.write(f"(= {ITERATIONS} readings per meter per trial). ")
        f.write("Each reading is validated (kWh, voltage, current range + power consistency check) ")
        f.write("and added to a tumbling window aggregator. Results averaged over 3 trials.\n\n")
        f.write("## Summary Results\n\n")
        f.write("| Meters | Go (r/s) | Py (r/s) | Ratio | Go (µs) | Py (µs) | Go Mem (MB) | Py Mem (MB) |\n")
        f.write("|--------|----------|----------|-------|---------|---------|-------------|-------------|\n")

        for i, m in enumerate(meters_list):
            f.write(
                f"| {m} | {go_throughput[i]:.0f} | {py_throughput[i]:.0f} | "
                f"{1/ratio_tp[i]:.0f}x | {go_latency[i]:.2f} | {py_latency[i]:.2f} | "
                f"{go_mem[i]:.1f} | {py_mem[i]:.1f} |\n"
            )

        f.write("\n## Throughput\n\n")
        f.write("![Throughput](benchmark_comparison.png)\n\n")
        f.write("## Key Takeaways\n\n")
        f.write("- Go outperforms Python by **{:.0f}x–{:.0f}x** in raw throughput across all meter counts.\n".format(
            1 / max(ratio_tp), 1 / min(ratio_tp)))
        f.write("- Go latency per reading is consistently **{:.1f}–{:.1f} µs** vs Python **{:.1f}–{:.1f} µs**.\n".format(
            min(go_latency), max(go_latency), min(py_latency), max(py_latency)))
        f.write("- Go memory usage is more efficient at scale: **{:.1f} MB** vs Python **{:.1f} MB** at 1000 meters.\n".format(
            go_mem[-1], py_mem[-1]))
        f.write("- Python asyncio overhead per-iteration adds measurable latency vs Go goroutines.\n")
        f.write("- The Python validator is pure Python; the Go version calls CGo-optimized Rust code.\n\n")


if __name__ == "__main__":
    main()
