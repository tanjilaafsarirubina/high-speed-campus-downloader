"""
====================================================================================================
⚡ HIGH SPEED CAMPUS DOWNLOADER (EDGEMESH) — COMPREHENSIVE BENCHMARKING & PROFILING SUITE
====================================================================================================
Course:      CSE449 — Parallel, Distributed & High-Performance Computing
Authors:     Tanjila Afsari Rubina (Student ID: 24241310)
             Sandip Kumar Paul     (Student ID: 24241311)
Copyright:   (C) 2026 Tanjila Afsari Rubina & Sandip Kumar Paul. All Rights Reserved.
License:     GNU General Public License v3.0 (GPLv3)

This program is free software: you can redistribute it and/or modify it under the terms of the
GNU General Public License as published by the Free Software Foundation, either version 3 of
the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
See the GNU General Public License for more details: <https://www.gnu.org/licenses/>.

ACADEMIC INTEGRITY & ANTI-PLAGIARISM NOTICE:
This codebase is an original capstone submission for CSE449. Any unauthorized reproduction,
plagiarism, or submission of this work for academic credit by other students is strictly
prohibited and constitutes academic misconduct under university regulations.
====================================================================================================

This module benchmarks the engine and renders every figure the README embeds.
Each experiment is either MEASURED or MODELED, and each figure says which:

MEASURED — real runs of engine.py against a local RFC 7233 test server on loopback:
  1a. Parallel range scaling (p = 1, 2, 3, 4, 8) with the server capping EACH connection
      at 10 Mbps, standing in for one capped student account per stream.
  2.  File-size sweep on an UNthrottled server: with no per-connection cap, extra streams
      alone buy nothing (the speedup comes from pooling separately capped pipes).
  5.  Memory: peak RSS of direct seek-writes vs. buffering the whole file in RAM.
  6.  Engine overhead: where download-thread time goes (HTTP receive, disk writes, lock
      waits) and what SHA-256 verification costs.

MODELED — closed-form arithmetic, no network involved:
  1b/3. End-to-end campus model: p accounts x 10 Mbps WAN, then (p - 1) chunks merged to
        the Master over a 350 Mbps hotspot, plus a fixed 2.5 s protocol overhead. It
        excludes the time the Master's user needs to switch Wi-Fi networks.
  4.    Heterogeneous scheduling: engine.compute_optimal_chunks() splits, with per-node
        times from the same WAN + LAN model.
  The ~3.7x headline speedup is a model output. This repo records no multi-PC measurement.

Generated outputs (in assets/):
- fig1..fig6 PNGs (200 DPI)
- benchmark_results.json (every number behind the figures) and benchmark_summary.csv
====================================================================================================
"""

import os
import sys
import gc
import time
import json
import csv
import shutil
import tempfile
import threading
import statistics
from socketserver import ThreadingMixIn
from http.server import HTTPServer, BaseHTTPRequestHandler
from typing import Dict, List, Tuple, Optional

import psutil
import requests
import matplotlib
matplotlib.use("Agg")  # Headless backend for deterministic PNG generation
import matplotlib.pyplot as plt
import numpy as np

# Ensure Windows CP1252 terminal handles Unicode safely
if sys.stdout and hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if sys.stderr and hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

from engine import (
    ThreadSafeFileWriter,
    WANChunkDownloader,
    calculate_file_hash,
    compute_optimal_chunks,
    split_uniform,
    format_speed
)

# Figures and result files land next to the README that embeds them
ASSETS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "assets")
os.makedirs(ASSETS_DIR, exist_ok=True)

# ============================================================================
# CAMPUS MODEL PARAMETERS (shared by every MODELED experiment)
# ============================================================================
MIB = 1024 * 1024
WAN_ACCOUNT_BPS = 10.0 * MIB / 8.0     # 10 Mbps per student account = 1.25 MiB/s
LAN_HOTSPOT_BPS = 350.0 * MIB / 8.0    # Wi-Fi hotspot merge rate = 43.75 MiB/s
MODEL_OVERHEAD_S = 2.5                 # discovery, handshakes and ACKs (fixed, assumed)
SCALING_P = [1, 2, 3, 4, 8]


def model_campus_time(size_bytes: float, p: int) -> Tuple[float, float, float]:
    """
    End-to-end model for p cooperating nodes, as (t_wan, t_lan, t_overhead) in seconds.

    p = 1 is one PC downloading over its own 10 Mbps account (no merge, no overhead).
    p > 1: every node downloads size/p over its own account in parallel, then the p - 1
    Worker chunks cross the shared hotspot to the Master one after another.
    """
    if p == 1:
        return size_bytes / WAN_ACCOUNT_BPS, 0.0, 0.0
    t_wan = (size_bytes / p) / WAN_ACCOUNT_BPS
    t_lan = ((p - 1) * size_bytes / p) / LAN_HOTSPOT_BPS
    return t_wan, t_lan, MODEL_OVERHEAD_S


# ============================================================================
# MULTI-THREADED RFC 7233 MOCK HTTP SERVER WITH OPTIONAL PER-CONNECTION THROTTLING
# ============================================================================

class ThreadingHTTPServer(ThreadingMixIn, HTTPServer):
    """Thread-safe multi-connection HTTP server for concurrent range downloads."""
    daemon_threads = True
    allow_reuse_address = True


class MockRangeHTTPHandler(BaseHTTPRequestHandler):
    """
    RFC 7233 compliant HTTP Range server serving a synthetic binary payload.
    With `throttle_bytes_per_sec` > 0 every connection is paced independently,
    like a campus proxy capping each account.
    """
    server_payload: bytes = b""
    throttle_bytes_per_sec: float = 0.0  # 0 = unthrottled (maximum loopback bus speed)

    def do_HEAD(self):
        self.send_response(200)
        self.send_header("Content-Length", str(len(self.server_payload)))
        self.send_header("Accept-Ranges", "bytes")
        self.send_header("Content-Disposition", 'attachment; filename="benchmark_payload.bin"')
        self.end_headers()

    def do_GET(self):
        range_header = self.headers.get("Range")
        total_len = len(self.server_payload)
        # memoryview slices are zero-copy, so the in-process server does not inflate the
        # RSS numbers that experiment 5 measures
        payload = memoryview(self.server_payload)

        if range_header and range_header.startswith("bytes="):
            byte_range = range_header.replace("bytes=", "").split("-")
            start = int(byte_range[0])
            end = int(byte_range[1]) if byte_range[1] else total_len - 1
            start = max(0, min(start, total_len - 1))
            end = max(start, min(end, total_len - 1))
            data = payload[start:end + 1]

            self.send_response(206)
            self.send_header("Content-Range", f"bytes {start}-{end}/{total_len}")
            self.send_header("Content-Length", str(len(data)))
            self.send_header("Accept-Ranges", "bytes")
            self.end_headers()
            self._write_throttled(data)
        else:
            self.send_response(200)
            self.send_header("Content-Length", str(total_len))
            self.send_header("Accept-Ranges", "bytes")
            self.end_headers()
            self._write_throttled(payload)

    def _write_throttled(self, data: memoryview):
        chunk_size = 64 * 1024
        t_start = time.perf_counter()
        sent = 0
        for i in range(0, len(data), chunk_size):
            chunk = data[i:i + chunk_size]
            sent += len(chunk)
            if self.throttle_bytes_per_sec > 0:
                # Never let the connection get ahead of rate x elapsed time (sleeping BEFORE each
                # write, against the cumulative schedule, so jitter cannot accumulate)
                delay = t_start + sent / self.throttle_bytes_per_sec - time.perf_counter()
                if delay > 0:
                    time.sleep(delay)
            try:
                self.wfile.write(chunk)
            except (ConnectionResetError, BrokenPipeError, ConnectionAbortedError):
                break

    def log_message(self, format, *args):
        pass  # Suppress HTTP access logging to keep benchmark stdout clean


def start_mock_server(payload_size: int, throttle_bps: float = 0.0) -> Tuple[ThreadingHTTPServer, str]:
    """Spawns an ephemeral RFC 7233 server with a deterministic random payload of `payload_size` bytes."""
    rng = np.random.default_rng(42)
    block_1mb = rng.bytes(MIB)
    payload = (block_1mb * (payload_size // MIB)) + block_1mb[:payload_size % MIB]

    # A subclass per server, so concurrent or successive servers never share a payload
    handler = type("PayloadHandler", (MockRangeHTTPHandler,),
                   {"server_payload": payload, "throttle_bytes_per_sec": throttle_bps})
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server, f"http://127.0.0.1:{server.server_port}/benchmark_payload.bin"


def stop_mock_server(server: ThreadingHTTPServer):
    server.shutdown()
    server.server_close()


def parallel_download(url: str, total_size: int, p: int, dest: str,
                      writer: Optional[ThreadSafeFileWriter] = None) -> Dict:
    """
    Downloads bytes [0, total_size) with p concurrent WANChunkDownloader threads writing
    into one preallocated file — the exact code path STANDALONE mode runs.

    Returns wall-clock seconds for preallocation and download, plus per-thread busy time.
    """
    t0 = time.perf_counter()
    writer = writer or ThreadSafeFileWriter(dest, total_size)
    t_prealloc = time.perf_counter() - t0

    chunks = split_uniform(total_size, p)
    thread_s: List[float] = []

    def _run(downloader: WANChunkDownloader):
        t_start = time.perf_counter()
        downloader.start_download()
        thread_s.append(time.perf_counter() - t_start)  # list.append is atomic

    threads = [threading.Thread(target=_run, args=(WANChunkDownloader(url, c, writer),)) for c in chunks]
    t1 = time.perf_counter()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    writer.close()
    t_download = time.perf_counter() - t1

    if not all(c.status == "COMPLETED" for c in chunks):
        raise RuntimeError(f"Benchmark download incomplete: {[c.status for c in chunks]}")
    return {"prealloc_sec": t_prealloc, "download_sec": t_download, "thread_sec": thread_s}


# ============================================================================
# MEMORY PROFILING MONITOR
# ============================================================================

class MemorySampler:
    """Samples process Resident Set Size (RSS) in MiB at high frequency."""
    def __init__(self, interval: float = 0.005):
        self.interval = interval
        self.samples: List[Tuple[float, float]] = []
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._process = psutil.Process(os.getpid())
        self._start_time = 0.0

    def start(self):
        self.samples.clear()
        self._stop_event.clear()
        self._start_time = time.perf_counter()
        self._thread = threading.Thread(target=self._sample_loop, daemon=True)
        self._thread.start()

    def _sample_loop(self):
        while not self._stop_event.is_set():
            t = time.perf_counter() - self._start_time
            try:
                self.samples.append((t, self._process.memory_info().rss / MIB))
            except Exception:
                break
            time.sleep(self.interval)

    def stop(self) -> List[Tuple[float, float]]:
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=1.0)
        return list(self.samples)


# ============================================================================
# EXPERIMENT 1: SPEEDUP & PARALLEL EFFICIENCY SCALING (p = 1 to 8)
# ============================================================================

def run_scaling_experiment(out_dir: str, payload_size: int = 8 * MIB, repeats: int = 3) -> Dict:
    """
    1a (MEASURED): time to fetch `payload_size` bytes with p streams when the server caps
       each connection at 10 Mbps. Median of `repeats` runs per p.
    1b (MODELED): model_campus_time() for a 100 MiB file, which adds the LAN merge phase
       and protocol overhead that a single machine cannot reproduce.
    """
    print("\n" + "=" * 80)
    print(" 🚀 EXPERIMENT 1: PARALLEL SCALING & SPEEDUP ANALYSIS (p = 1, 2, 3, 4, 8)")
    print("=" * 80)
    print(f"  [Measured] {payload_size // MIB} MiB via engine.py, server capped at "
          f"{format_speed(WAN_ACCOUNT_BPS)} per connection, median of {repeats} runs")

    measured = []
    server, url = start_mock_server(payload_size, throttle_bps=WAN_ACCOUNT_BPS)
    try:
        for p in SCALING_P:
            runs = []
            for r in range(repeats):
                dest = os.path.join(out_dir, f"scale_p{p}_{r}.bin")
                runs.append(parallel_download(url, payload_size, p, dest)["download_sec"])
                os.remove(dest)
            measured.append({"workers": p, "time_sec": statistics.median(runs), "runs_sec": runs})
    finally:
        stop_mock_server(server)

    t1 = measured[0]["time_sec"]
    for row in measured:
        row["speedup"] = t1 / row["time_sec"]
        row["efficiency"] = (row["speedup"] / row["workers"]) * 100.0
        print(f"  Streams p={row['workers']:<2} | Time: {row['time_sec']:6.2f}s | "
              f"Speedup: {row['speedup']:5.2f}x | Efficiency: {row['efficiency']:5.1f}%")

    model_size = 100 * MIB
    t1_model = sum(model_campus_time(model_size, 1))
    model = []
    print(f"\n  [Model] {model_size // MIB} MiB end-to-end (10 Mbps WAN + 350 Mbps hotspot merge + {MODEL_OVERHEAD_S}s overhead)")
    for p in SCALING_P:
        t_p = sum(model_campus_time(model_size, p))
        speedup = t1_model / t_p
        model.append({"workers": p, "time_sec": t_p, "speedup": speedup, "efficiency": speedup / p * 100.0})
        print(f"  Nodes   p={p:<2} | Time: {t_p:6.2f}s | Speedup: {speedup:5.2f}x | Efficiency: {speedup / p * 100:5.1f}%")

    return {
        "measured_payload_bytes": payload_size,
        "per_connection_cap_bps": WAN_ACCOUNT_BPS,
        "repeats": repeats,
        "measured": measured,
        "model_size_bytes": model_size,
        "model": model
    }


# ============================================================================
# EXPERIMENT 2: FILE SIZE SENSITIVITY ON AN UNTHROTTLED SERVER (MEASURED)
# ============================================================================

def run_filesize_experiment(out_dir: str, repeats: int = 3) -> Dict:
    """
    1 stream vs. 4 streams on an unthrottled loopback server (median of `repeats` runs,
    after one warm-up). Without a per-connection cap there is no bandwidth to pool, so
    this isolates the engine's own overhead.
    """
    print("\n" + "=" * 80)
    print(" 📦 EXPERIMENT 2: FILE SIZE SWEEP, UNTHROTTLED LOOPBACK (1 vs 4 STREAMS)")
    print("=" * 80)

    def _timed(url, size, p, dest):
        elapsed = parallel_download(url, size, p, dest)["download_sec"]
        os.remove(dest)
        return elapsed

    results = []
    for sz in [10, 25, 50, 100]:
        payload_bytes = sz * MIB
        server, url = start_mock_server(payload_bytes)
        try:
            dest = os.path.join(out_dir, f"size_{sz}mb.bin")
            _timed(url, payload_bytes, 4, dest)  # warm-up: page cache, sockets, allocator
            t_single = statistics.median(_timed(url, payload_bytes, 1, dest) for _ in range(repeats))
            t_parallel = statistics.median(_timed(url, payload_bytes, 4, dest) for _ in range(repeats))
        finally:
            stop_mock_server(server)

        speedup = t_single / t_parallel if t_parallel > 0 else 1.0
        print(f"  File Size: {sz:3} MiB | 1 stream: {t_single:6.2f}s | 4 streams: {t_parallel:6.2f}s | "
              f"Speedup: {speedup:5.2f}x | 4-stream rate: {format_speed(payload_bytes / t_parallel)}")
        results.append({
            "size_mb": sz,
            "size_bytes": payload_bytes,
            "t_single_sec": t_single,
            "t_parallel_sec": t_parallel,
            "speedup": speedup,
            "efficiency": (speedup / 4) * 100.0
        })

    return {"filesize_data": results}


# ============================================================================
# EXPERIMENT 3: CAMPUS NETWORK 10 Mbps QUOTA (MODELED)
# ============================================================================

def run_campus_quota_experiment() -> Dict:
    """
    model_campus_time() for typical student downloads: one PC on a 10 Mbps account vs.
    2, 3 and 4 pooled accounts merged over a 350 Mbps hotspot.
    """
    print("\n" + "=" * 80)
    print(" 🏫 EXPERIMENT 3: CAMPUS WI-FI 10 Mbps QUOTA MODEL (THE UNIVERSITY SCENARIO)")
    print("=" * 80)

    datasets = [
        {"name": "Lecture Recording", "size_mb": 100},
        {"name": "Lab VM Appliance", "size_mb": 500},
        {"name": "Ubuntu Linux ISO", "size_mb": 1024},
        {"name": "MATLAB / CUDA Toolkit", "size_mb": 4096},
        {"name": "Kaggle / ML Dataset", "size_mb": 10240}
    ]

    def fmt_dur(sec):
        if sec < 60:
            return f"{sec:4.1f}s"
        if sec < 3600:
            return f"{int(sec // 60)}m {int(sec % 60)}s"
        return f"{int(sec // 3600)}h {int((sec % 3600) // 60)}m"

    print(f" {'Dataset':<22} | {'Size':<9} | {'Single PC (10M)':<16} | {'2 Nodes':<10} | {'3 Nodes':<10} | {'4 Nodes':<10} | {'Speedup (4N)'}")
    print("-" * 96)

    comparisons = []
    for ds in datasets:
        size_bytes = ds["size_mb"] * MIB
        t_single = sum(model_campus_time(size_bytes, 1))
        p_times = {p: sum(model_campus_time(size_bytes, p)) for p in (2, 3, 4)}
        p_speedups = {p: t_single / t for p, t in p_times.items()}
        print(f" {ds['name']:<22} | {ds['size_mb']:>5} MiB | {fmt_dur(t_single):<16} | {fmt_dur(p_times[2]):<10} | "
              f"{fmt_dur(p_times[3]):<10} | {fmt_dur(p_times[4]):<10} | {p_speedups[4]:5.2f}x")
        comparisons.append({
            "name": ds["name"],
            "size_mb": ds["size_mb"],
            "size_bytes": size_bytes,
            "t_single_sec": t_single,
            "t_2nodes_sec": p_times[2],
            "t_3nodes_sec": p_times[3],
            "t_4nodes_sec": p_times[4],
            "speedup_2nodes": p_speedups[2],
            "speedup_3nodes": p_speedups[3],
            "speedup_4nodes": p_speedups[4]
        })

    return {
        "wan_account_mbps": 10.0,
        "lan_hotspot_mbps": 350.0,
        "overhead_sec": MODEL_OVERHEAD_S,
        "comparisons": comparisons
    }


# ============================================================================
# EXPERIMENT 4: HETEROGENEOUS DYNAMIC SCHEDULING (MAKESPAN OPTIMIZATION, MODELED)
# ============================================================================

def run_heterogeneous_experiment() -> Dict:
    """
    Naive equal 25% partitioning vs. compute_optimal_chunks() when node WAN speeds differ
    (5, 20, 10 and 15 MiB/s = 40, 160, 80 and 120 Mbps). The split comes from the real
    scheduler; per-node times use the WAN + LAN model with a 35 MiB/s hotspot.
    """
    print("\n" + "=" * 80)
    print(" ⚖️ EXPERIMENT 4: HETEROGENEOUS DYNAMIC SCHEDULING VS NAIVE EQUAL SPLITTING")
    print("=" * 80)

    file_size = 500 * MIB
    lan_speed = 35 * MIB  # Hotspot transmission rate used by compute_optimal_chunks' model

    node_speeds = {0: 5.0 * MIB, 1: 20.0 * MIB, 2: 10.0 * MIB, 3: 15.0 * MIB}

    # 1. Naive Equal Partitioning: Each node gets 25%
    naive_chunk_size = file_size / 4.0
    naive_node_times = {}
    for node_id, speed in node_speeds.items():
        t_lan = (naive_chunk_size / lan_speed) if node_id > 0 else 0.0
        naive_node_times[node_id] = naive_chunk_size / speed + t_lan
    naive_makespan = max(naive_node_times.values())
    straggler_node = max(naive_node_times, key=naive_node_times.get)

    # 2. Optimal Dynamic Allocation (via compute_optimal_chunks)
    optimal_chunks = compute_optimal_chunks(file_size, node_speeds, lan_speed=lan_speed)
    optimal_node_times = {}
    optimal_chunk_sizes = {}
    for chunk in optimal_chunks:
        nid = chunk.chunk_id
        optimal_chunk_sizes[nid] = chunk.total_bytes
        t_lan = (chunk.total_bytes / lan_speed) if nid > 0 else 0.0
        optimal_node_times[nid] = chunk.total_bytes / node_speeds[nid] + t_lan
    optimal_makespan = max(optimal_node_times.values())

    makespan_reduction = ((naive_makespan - optimal_makespan) / naive_makespan) * 100.0

    print(f"  Naive Equal Split Makespan : {naive_makespan:6.2f}s (Governed by Straggler Node {straggler_node})")
    print(f"  Optimal Split Makespan     : {optimal_makespan:6.2f}s")
    print(f"  🚀 Makespan Improvement    : {makespan_reduction:5.1f}% Faster Completion")

    node_breakdowns = []
    for nid in range(4):
        speed_mbps = (node_speeds[nid] * 8) / MIB
        naive_pct = (naive_chunk_size / file_size) * 100
        opt_pct = (optimal_chunk_sizes[nid] / file_size) * 100
        print(f"    Node {nid} ({speed_mbps:5.1f} Mbps) | Naive: {naive_pct:4.1f}% ({naive_node_times[nid]:5.2f}s) | "
              f"Optimal: {opt_pct:4.1f}% ({optimal_node_times[nid]:5.2f}s)")
        node_breakdowns.append({
            "node_id": nid,
            "bandwidth_mbps": speed_mbps,
            "naive_size_mb": naive_chunk_size / MIB,
            "naive_time_sec": naive_node_times[nid],
            "optimal_size_mb": optimal_chunk_sizes[nid] / MIB,
            "optimal_time_sec": optimal_node_times[nid]
        })

    return {
        "file_size_mb": file_size / MIB,
        "lan_speed_mib_s": lan_speed / MIB,
        "naive_makespan_sec": naive_makespan,
        "optimal_makespan_sec": optimal_makespan,
        "makespan_reduction_pct": makespan_reduction,
        "node_breakdowns": node_breakdowns
    }


# ============================================================================
# EXPERIMENT 5: OUT-OF-CORE I/O MEMORY PROFILING (MEASURED)
# ============================================================================

def _profile_rss(fn) -> Tuple[List[Tuple[float, float]], float]:
    """Runs fn() under a MemorySampler; returns the trace (seconds, RSS MiB above start) and the peak delta."""
    gc.collect()
    sampler = MemorySampler()
    sampler.start()
    time.sleep(0.05)  # Establish baseline
    fn()
    time.sleep(0.05)
    samples = sampler.stop()
    base = samples[0][1] if samples else 0.0
    trace = [(t, rss - base) for t, rss in samples]
    return trace, max((d for _, d in trace), default=0.0)


def run_memory_profiling_experiment(out_dir: str, sizes_mib=(16, 32, 64, 128), trace_mib: int = 64) -> Dict:
    """
    Peak RSS growth while downloading N MiB two ways:
    1. The engine: 4 range streams writing straight to their file offsets (64 KB buffers).
    2. Traditional in-memory buffering: accumulate the whole body in RAM, then write it.
    """
    print("\n" + "=" * 80)
    print(" 🧠 EXPERIMENT 5: OUT-OF-CORE I/O MEMORY PROFILING")
    print("=" * 80)

    rows = []
    traces = {}
    for size_mib in sizes_mib:
        size = size_mib * MIB
        server, url = start_mock_server(size)
        dest = os.path.join(out_dir, "mem_target.bin")
        try:
            def _direct():
                parallel_download(url, size, 4, dest)

            def _in_memory():
                resp = requests.get(url, stream=True, timeout=30)
                buffer = bytearray()
                for block in resp.iter_content(chunk_size=64 * 1024):
                    buffer.extend(block)
                with open(dest, "wb") as f:
                    f.write(buffer)
                del buffer

            direct_trace, direct_peak = _profile_rss(_direct)
            os.remove(dest)
            inmem_trace, inmem_peak = _profile_rss(_in_memory)
            os.remove(dest)
        finally:
            stop_mock_server(server)
            del server
            gc.collect()

        print(f"  {size_mib:4d} MiB file | direct seek-writes peak RSS +{direct_peak:6.1f} MiB | "
              f"in-memory buffering +{inmem_peak:6.1f} MiB")
        rows.append({"size_mib": size_mib, "direct_peak_delta_mib": direct_peak, "inmem_peak_delta_mib": inmem_peak})
        if size_mib == trace_mib:
            traces = {"direct": direct_trace, "inmem": inmem_trace}

    return {"trace_size_mib": trace_mib, "sizes": rows, "traces": traces}


# ============================================================================
# EXPERIMENT 6: ENGINE OVERHEAD BREAKDOWN (INSTRUMENTED TIMING, MEASURED)
# ============================================================================

class TimedLock:
    """Drop-in for ThreadSafeFileWriter's mutex that records time spent waiting to acquire it."""
    def __init__(self):
        self._lock = threading.Lock()
        self.wait_s = 0.0

    def __enter__(self):
        t0 = time.perf_counter()
        self._lock.acquire()
        self.wait_s += time.perf_counter() - t0  # safe: updated while holding the lock
        return self

    def __exit__(self, *exc):
        self._lock.release()


class TimedFileWriter(ThreadSafeFileWriter):
    """ThreadSafeFileWriter that also accumulates the time spent inside write_at()."""
    def __init__(self, filepath: str, total_size: int):
        super().__init__(filepath, total_size)
        self._lock = TimedLock()
        self.write_at_s = 0.0
        self._stats_lock = threading.Lock()

    def write_at(self, offset: int, data: bytes):
        t0 = time.perf_counter()
        super().write_at(offset, data)
        dt = time.perf_counter() - t0
        with self._stats_lock:
            self.write_at_s += dt


def run_system_profiling_experiment(out_dir: str, payload_size: int = 64 * MIB) -> Dict:
    """
    Instruments a 4-stream download of `payload_size` bytes from an unthrottled server.
    Download-thread time (summed over the 4 threads) splits into:
      - HTTP receive: time in requests/urllib3 reading the socket (thread time minus write_at)
      - Disk seek + write: time inside write_at() holding the lock
      - Lock wait: time blocked acquiring the writer's mutex
    Wall-clock phases: preallocation, download, SHA-256 verification of the assembled file.
    The server runs in the same process, so receive time also includes GIL contention with it.
    """
    print("\n" + "=" * 80)
    print(" ⏱️ EXPERIMENT 6: ENGINE OVERHEAD BREAKDOWN (INSTRUMENTED TIMING)")
    print("=" * 80)

    dest = os.path.join(out_dir, "profile_target.bin")
    server, url = start_mock_server(payload_size)
    try:
        t0 = time.perf_counter()
        writer = TimedFileWriter(dest, payload_size)
        t_prealloc = time.perf_counter() - t0
        run = parallel_download(url, payload_size, 4, dest, writer=writer)
    finally:
        stop_mock_server(server)

    t0 = time.perf_counter()
    file_hash = calculate_file_hash(dest, "sha256")
    t_hash = time.perf_counter() - t0
    os.remove(dest)

    thread_total = sum(run["thread_sec"])
    lock_wait = writer._lock.wait_s
    disk_write = max(0.0, writer.write_at_s - lock_wait)
    http_receive = max(0.0, thread_total - writer.write_at_s)
    breakdown = [
        {"component": "HTTP receive (requests / socket)", "time_sec": http_receive},
        {"component": "Disk seek + write", "time_sec": disk_write},
        {"component": "Writer lock wait", "time_sec": lock_wait},
    ]
    for b in breakdown:
        b["percentage"] = (b["time_sec"] / thread_total) * 100.0 if thread_total > 0 else 0.0

    wall = [
        {"phase": "Preallocate file", "time_sec": t_prealloc},
        {"phase": f"Download {payload_size // MIB} MiB (4 streams)", "time_sec": run["download_sec"]},
        {"phase": "SHA-256 verify", "time_sec": t_hash},
    ]
    download_rate = payload_size / run["download_sec"]
    hash_rate = payload_size / t_hash if t_hash > 0 else 0.0

    print(f" {'Download-thread time (4 threads)':<36} | {'Seconds':<9} | Share")
    print("-" * 60)
    for b in breakdown:
        print(f" {b['component']:<36} | {b['time_sec']:7.3f}   | {b['percentage']:5.1f}%")
    print("-" * 60)
    for w in wall:
        print(f" {w['phase']:<36} | {w['time_sec']:7.3f}s  (wall clock)")
    print(f"\n  Engine throughput on loopback: {format_speed(download_rate)} "
          f"= {download_rate / WAN_ACCOUNT_BPS:.0f}x a 10 Mbps account | SHA-256: {format_speed(hash_rate)}")

    return {
        "payload_bytes": payload_size,
        "thread_time_total_sec": thread_total,
        "breakdown": breakdown,
        "wall_clock": wall,
        "download_rate_bps": download_rate,
        "sha256_rate_bps": hash_rate,
        "file_hash": file_hash
    }


# ============================================================================
# FIGURE GENERATION (matplotlib, 200 DPI)
# ============================================================================

# Palette: validated categorical slots (in fixed order), an ordinal blue ramp, and neutral ink
SURFACE = "#fcfcfb"
INK = "#0b0b0b"
INK_2 = "#52514e"
MUTED = "#898781"
GRID = "#e1e0d9"
AXIS = "#c3c2b7"
BLUE, ORANGE, AQUA, YELLOW = "#2a78d6", "#eb6834", "#1baf7a", "#eda100"
BLUE_RAMP = ["#86b6ef", "#3987e5", "#1c5cab", "#0d366b"]  # ordinal: 1, 2, 3, 4 nodes
MEASURED_TAG = "MEASURED"
MODEL_TAG = "MODEL"


def _style():
    plt.rcParams.update({
        "figure.facecolor": SURFACE,
        "axes.facecolor": SURFACE,
        "savefig.facecolor": SURFACE,
        "font.family": "sans-serif",
        "font.sans-serif": ["Segoe UI", "DejaVu Sans", "Helvetica", "Arial"],
        "font.size": 10,
        "text.color": INK,
        "axes.edgecolor": AXIS,
        "axes.linewidth": 0.8,
        "axes.labelcolor": INK_2,
        "axes.titlesize": 12,
        "axes.titleweight": "bold",
        "axes.titlecolor": INK,
        "axes.titlelocation": "left",
        "axes.spines.top": False,
        "axes.spines.right": False,
        "axes.grid": True,
        "axes.axisbelow": True,
        "grid.color": GRID,
        "grid.linewidth": 0.6,
        "grid.linestyle": "-",
        "xtick.color": AXIS,
        "ytick.color": AXIS,
        "xtick.labelcolor": INK_2,
        "ytick.labelcolor": INK_2,
        "legend.frameon": False,
        "legend.fontsize": 9,
        "lines.linewidth": 2,
        "lines.markersize": 7,
        "figure.autolayout": False,
    })


def _title(ax, title: str, tag: str, subtitle: str):
    """Bold title plus a muted one-line subtitle that starts with MEASURED or MODEL."""
    ax.set_title(title, pad=26)
    ax.text(0, 1.02, f"{tag} · {subtitle}", transform=ax.transAxes, color=INK_2, fontsize=8.5, va="bottom")


def _save(fig, name: str):
    path = os.path.join(ASSETS_DIR, name)
    fig.savefig(path, dpi=200, bbox_inches="tight", pad_inches=0.25)
    plt.close(fig)
    print(f"  [+] Saved: {path}")


def generate_presentation_charts(exp1: Dict, exp2: Dict, exp3: Dict, exp4: Dict, exp5: Dict, exp6: Dict):
    """Renders the six README figures into assets/."""
    print("\n" + "=" * 80)
    print(" 🎨 GENERATING FIGURES (200 DPI PNGs in assets/)")
    print("=" * 80)
    _style()

    # ------------------------------------------------------------------------
    # FIGURE 1: SPEEDUP & PARALLEL EFFICIENCY (MEASURED WAN PHASE vs END-TO-END MODEL)
    # ------------------------------------------------------------------------
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 4.8))
    p_vals = [r["workers"] for r in exp1["measured"]]
    meas_s = [r["speedup"] for r in exp1["measured"]]
    meas_e = [r["efficiency"] for r in exp1["measured"]]
    model_s = [r["speedup"] for r in exp1["model"]]
    model_e = [r["efficiency"] for r in exp1["model"]]
    cap = format_speed(exp1["per_connection_cap_bps"])

    ax1.plot([1, 8], [1, 8], color=MUTED, linewidth=1, linestyle=(0, (4, 3)), label="Ideal ($S_p = p$)")
    ax1.plot(p_vals, meas_s, "o-", color=BLUE, label=f"Measured: engine, {exp1['measured_payload_bytes'] // MIB} MiB, {cap} cap per stream")
    ax1.plot(p_vals, model_s, "s-", color=ORANGE, label="Model: 100 MiB incl. hotspot merge + overhead")
    for x, y in zip(p_vals, meas_s):
        if x in (4, 8):
            ax1.annotate(f"{y:.2f}×", (x, y), textcoords="offset points", xytext=(-8, 8), ha="right", color=INK, fontsize=9, fontweight="bold")
    for x, y in zip(p_vals, model_s):
        if x in (4, 8):
            ax1.annotate(f"{y:.2f}×", (x, y), textcoords="offset points", xytext=(8, -14), ha="left", color=INK_2, fontsize=9)
    _title(ax1, "Speedup vs. cooperating nodes", MEASURED_TAG + " + " + MODEL_TAG,
           f"measured = loopback server capping each connection; median of {exp1['repeats']} runs")
    ax1.set_xlabel("Streams / nodes (p)")
    ax1.set_ylabel("Speedup  $S_p = T_1 / T_p$")
    ax1.set_xticks(p_vals)
    ax1.set_ylim(0, 8.6)
    ax1.legend(loc="upper left")

    ax2.axhline(100, color=MUTED, linewidth=1, linestyle=(0, (4, 3)))
    ax2.plot(p_vals, meas_e, "o-", color=BLUE, label="Measured")
    ax2.plot(p_vals, model_e, "s-", color=ORANGE, label="Model")
    ax2.annotate(f"{meas_e[-1]:.0f}%", (p_vals[-1], meas_e[-1]), textcoords="offset points", xytext=(0, 9), ha="center", color=INK, fontsize=9, fontweight="bold")
    ax2.annotate(f"{model_e[-1]:.0f}%", (p_vals[-1], model_e[-1]), textcoords="offset points", xytext=(0, -16), ha="center", color=INK_2, fontsize=9)
    _title(ax2, "Parallel efficiency", MEASURED_TAG + " + " + MODEL_TAG, "$E_p = S_p / p$")
    ax2.set_xlabel("Streams / nodes (p)")
    ax2.set_ylabel("Efficiency (%)")
    ax2.set_xticks(p_vals)
    ax2.set_ylim(0, 112)
    ax2.legend(loc="lower left")
    fig.tight_layout(w_pad=3)
    _save(fig, "fig1_speedup_scaling.png")

    # ------------------------------------------------------------------------
    # FIGURE 2: CAMPUS 10 Mbps QUOTA MODEL (DOWNLOAD TIME COMPARISON)
    # ------------------------------------------------------------------------
    fig, ax = plt.subplots(figsize=(12, 5.2))
    comps = exp3["comparisons"]
    labels = [f"{c['name']}\n{c['size_mb']:,} MiB" for c in comps]
    x = np.arange(len(comps))
    width = 0.2
    series = [("Single PC (10 Mbps)", "t_single_sec"), ("2 nodes", "t_2nodes_sec"),
              ("3 nodes", "t_3nodes_sec"), ("4 nodes", "t_4nodes_sec")]
    bars4 = None
    for i, (name, key) in enumerate(series):
        bars = ax.bar(x + (i - 1.5) * width, [c[key] / 60.0 for c in comps], width * 0.92,
                      color=BLUE_RAMP[i], label=name, edgecolor=SURFACE, linewidth=0)
        if key == "t_4nodes_sec":
            bars4 = bars
    for bar, c in zip(bars4, comps):
        ax.annotate(f"{c['speedup_4nodes']:.2f}×", (bar.get_x() + bar.get_width() / 2, bar.get_height()),
                    textcoords="offset points", xytext=(0, 4), ha="center", color=INK, fontsize=9, fontweight="bold")
    ax.set_yscale("log")
    minute_ticks = [0.5, 1, 2, 5, 10, 20, 50, 100, 200]
    ax.set_yticks(minute_ticks)
    ax.set_yticklabels([f"{t:g}" for t in minute_ticks])
    ax.minorticks_off()
    ax.set_ylim(0.3, 400)
    ax.set_xticks(x)
    ax.set_xticklabels(labels, fontsize=9)
    ax.set_ylabel("Completion time (minutes, log scale)")
    ax.grid(True, axis="y", which="major")
    ax.grid(False, axis="x")
    ax.legend(loc="upper left", ncol=4)
    _title(ax, "Campus 10 Mbps quota: download time, single PC vs. pooled accounts", MODEL_TAG,
           f"10 Mbps per account, {exp3['lan_hotspot_mbps']:.0f} Mbps hotspot merge, {exp3['overhead_sec']} s overhead; "
           "labels = 4-node speedup")
    fig.tight_layout()
    _save(fig, "fig2_campus_download_time.png")

    # ------------------------------------------------------------------------
    # FIGURE 3: MODELED PROTOCOL TIMELINE (1 GiB, 4 NODES, WORKERS STREAM TO MASTER)
    # ------------------------------------------------------------------------
    fig, ax = plt.subplots(figsize=(12, 4.2))
    size = 1024 * MIB
    t_wan, t_lan, t_over = model_campus_time(size, 4)
    t_hash = size / exp6["sha256_rate_bps"]
    single = sum(model_campus_time(size, 1))
    makespan = t_over + t_wan + t_lan + t_hash
    rows = ["Master (PC1)", "Worker 1", "Worker 2", "Worker 3"]
    y = np.arange(len(rows))[::-1]
    seg = dict(height=0.5, edgecolor=SURFACE, linewidth=1.5)
    ax.barh(y, [t_over] * 4, left=0, color=AXIS, label=f"Setup / handshakes ({t_over:.1f} s, assumed)", **seg)
    ax.barh(y, [t_wan] * 4, left=t_over, color=BLUE, label=f"WAN download, 256 MiB each at 10 Mbps ({t_wan:.0f} s)", **seg)
    ax.barh(y, [t_lan] * 4, left=t_over + t_wan, color=ORANGE,
            label=f"Hotspot merge: 3 chunks into Master at 350 Mbps ({t_lan:.1f} s)", **seg)
    ax.barh(y[0], t_hash, left=t_over + t_wan + t_lan, color=AQUA,
            label=f"SHA-256 verify at measured rate ({t_hash:.1f} s)", **seg)
    ax.axvline(makespan, color=INK_2, linewidth=1)
    ax.annotate(f"4 nodes: {makespan / 60:.1f} min\n1 PC: {single / 60:.1f} min\n{single / makespan:.2f}× faster",
                (makespan, float(np.mean(y))), textcoords="offset points", xytext=(10, 0), ha="left", va="center",
                color=INK, fontsize=9.5, fontweight="bold")
    ax.set_yticks(y)
    ax.set_yticklabels(rows)
    ax.set_xlim(0, makespan * 1.28)
    ax.set_xlabel("Time (seconds)")
    ax.grid(True, axis="x")
    ax.grid(False, axis="y")
    ax.legend(loc="upper center", bbox_to_anchor=(0.5, -0.2), ncol=2)
    _title(ax, "Protocol timeline for a 1 GiB file on 4 nodes", MODEL_TAG,
           "Workers stream straight to the Master once it joins the hotspot; excludes the Master's Wi-Fi switch")
    fig.tight_layout()
    _save(fig, "fig3_makespan_timeline.png")

    # ------------------------------------------------------------------------
    # FIGURE 4: HETEROGENEOUS DYNAMIC SCHEDULING (STRAGGLER MITIGATION)
    # ------------------------------------------------------------------------
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 4.8))
    nb = exp4["node_breakdowns"]
    nodes_lbl = [f"{'Master' if b['node_id'] == 0 else 'Worker ' + str(b['node_id'])}\n{b['bandwidth_mbps']:.0f} Mbps" for b in nb]
    x = np.arange(len(nb))
    w = 0.38
    ax1.bar(x - w / 2, [b["naive_time_sec"] for b in nb], w * 0.94, color=ORANGE, label="Equal 25% split")
    ax1.bar(x + w / 2, [b["optimal_time_sec"] for b in nb], w * 0.94, color=BLUE, label="compute_optimal_chunks()")
    ax1.set_xticks(x)
    ax1.set_xticklabels(nodes_lbl, fontsize=9)
    ax1.set_ylabel("Node finish time (seconds)")
    ax1.grid(False, axis="x")
    ax1.legend(loc="upper right")
    _title(ax1, f"Makespan: {exp4['naive_makespan_sec']:.1f} s → {exp4['optimal_makespan_sec']:.1f} s "
                f"({exp4['makespan_reduction_pct']:.0f}% shorter)", MODEL_TAG,
           f"{exp4['file_size_mb']:.0f} MiB file; real scheduler split, WAN + {exp4['lan_speed_mib_s']:.0f} MiB/s LAN time model")

    ax2.bar(x - w / 2, [b["naive_size_mb"] for b in nb], w * 0.94, color=ORANGE, label="Equal 25% split")
    opt_bars = ax2.bar(x + w / 2, [b["optimal_size_mb"] for b in nb], w * 0.94, color=BLUE, label="compute_optimal_chunks()")
    for bar in opt_bars:
        ax2.annotate(f"{bar.get_height():.0f}", (bar.get_x() + bar.get_width() / 2, bar.get_height()),
                     textcoords="offset points", xytext=(0, 3), ha="center", color=INK, fontsize=9)
    ax2.set_xticks(x)
    ax2.set_xticklabels(nodes_lbl, fontsize=9)
    ax2.set_ylim(0, max(b["optimal_size_mb"] for b in nb) * 1.25)  # headroom for the legend
    ax2.set_ylabel("Assigned chunk (MiB)")
    ax2.grid(False, axis="x")
    ax2.legend(loc="upper left")
    _title(ax2, "Faster nodes get bigger chunks", MODEL_TAG, "Workers pay a LAN hop, so the Master's share is boosted")
    fig.tight_layout(w_pad=3)
    _save(fig, "fig4_heterogeneous_makespan.png")

    # ------------------------------------------------------------------------
    # FIGURE 5: OUT-OF-CORE I/O MEMORY PROFILING
    # ------------------------------------------------------------------------
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 4.8))
    tr = exp5["traces"]
    ax1.plot([t for t, _ in tr["direct"]], [d for _, d in tr["direct"]], color=BLUE, label="Direct seek-writes (engine)")
    ax1.plot([t for t, _ in tr["inmem"]], [d for _, d in tr["inmem"]], color=ORANGE, label="Buffer whole file in RAM")
    ax1.set_xlabel("Elapsed time (seconds)")
    ax1.set_ylabel("RSS above baseline (MiB)")
    ax1.legend(loc="upper left")
    _title(ax1, f"Memory trace while downloading {exp5['trace_size_mib']} MiB", MEASURED_TAG,
           "process RSS sampled every 5 ms; 4 streams vs. one buffered stream")

    sizes = [r["size_mib"] for r in exp5["sizes"]]
    direct = [r["direct_peak_delta_mib"] for r in exp5["sizes"]]
    inmem = [r["inmem_peak_delta_mib"] for r in exp5["sizes"]]
    ax2.plot(sizes, direct, "o-", color=BLUE, label="Direct seek-writes (engine)")
    ax2.plot(sizes, inmem, "s-", color=ORANGE, label="Buffer whole file in RAM")
    ax2.annotate(f"+{direct[-1]:.1f} MiB", (sizes[-1], direct[-1]), textcoords="offset points", xytext=(0, 8), ha="center", color=INK, fontsize=9, fontweight="bold")
    ax2.annotate(f"+{inmem[-1]:.0f} MiB", (sizes[-1], inmem[-1]), textcoords="offset points", xytext=(-6, 4), ha="right", color=INK, fontsize=9, fontweight="bold")
    ax2.set_xticks(sizes)
    ax2.set_xlabel("File size (MiB)")
    ax2.set_ylabel("Peak RSS growth (MiB)")
    ax2.set_ylim(bottom=0)
    ax2.legend(loc="upper left")
    _title(ax2, "Peak memory stays flat as files grow", MEASURED_TAG, "peak RSS growth per download, same machine and server")
    fig.tight_layout(w_pad=3)
    _save(fig, "fig5_memory_profiling.png")

    # ------------------------------------------------------------------------
    # FIGURE 6: ENGINE OVERHEAD BREAKDOWN (INSTRUMENTED TIMING)
    # ------------------------------------------------------------------------
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 4.2))
    bd = exp6["breakdown"]
    yb = np.arange(len(bd))[::-1]
    ax1.barh(yb, [b["time_sec"] for b in bd], height=0.55, color=BLUE)
    for yy, b in zip(yb, bd):
        ax1.annotate(f"{b['time_sec']:.3f} s  ({b['percentage']:.1f}%)", (b["time_sec"], yy),
                     textcoords="offset points", xytext=(5, 0), va="center", color=INK, fontsize=9)
    ax1.set_yticks(yb)
    ax1.set_yticklabels([b["component"] for b in bd])
    ax1.set_xlim(0, max(b["time_sec"] for b in bd) * 1.45)
    ax1.set_xlabel("Thread-seconds, summed over 4 download threads")
    ax1.grid(False, axis="y")
    _title(ax1, "Where download-thread time goes", MEASURED_TAG,
           f"{exp6['payload_bytes'] // MIB} MiB, unthrottled loopback server, instrumented writer")

    wc = exp6["wall_clock"]
    yw = np.arange(len(wc))[::-1]
    ax2.barh(yw, [p["time_sec"] for p in wc], height=0.55, color=BLUE)
    for yy, p in zip(yw, wc):
        ax2.annotate(f"{p['time_sec']:.3f} s", (p["time_sec"], yy), textcoords="offset points", xytext=(5, 0),
                     va="center", color=INK, fontsize=9)
    ax2.set_yticks(yw)
    ax2.set_yticklabels([p["phase"] for p in wc])
    ax2.set_xlim(0, max(p["time_sec"] for p in wc) * 1.35)
    ax2.set_xlabel("Wall-clock seconds")
    ax2.grid(False, axis="y")
    _title(ax2, "Wall-clock phases", MEASURED_TAG,
           f"engine sustained {format_speed(exp6['download_rate_bps'])}, "
           f"{exp6['download_rate_bps'] / WAN_ACCOUNT_BPS:.0f}× one 10 Mbps account")
    fig.tight_layout(w_pad=3)
    _save(fig, "fig6_cpu_io_breakdown.png")


# ============================================================================
# DATA EXPORTERS (JSON & CSV)
# ============================================================================

def export_results(all_results: Dict):
    """Exports every number behind the figures to JSON, plus a short summary CSV."""
    json_path = os.path.join(ASSETS_DIR, "benchmark_results.json")
    cleaned = dict(all_results)
    cleaned["exp5"] = {k: v for k, v in all_results["exp5"].items() if k != "traces"}  # traces are large
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(cleaned, f, indent=2)
    print(f"\n[+] Exported benchmark metrics: {json_path}")

    csv_path = os.path.join(ASSETS_DIR, "benchmark_summary.csv")
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["Kind", "Experiment", "Metric", "Baseline", "Campus Downloader", "Result"])

        e1 = all_results["exp1"]
        m1, m4 = e1["measured"][0], next(r for r in e1["measured"] if r["workers"] == 4)
        writer.writerow(["measured", f"Scaling, {e1['measured_payload_bytes'] // MIB} MiB, 10 Mbps cap per stream",
                         "Download time (s)", f"{m1['time_sec']:.2f}", f"{m4['time_sec']:.2f}", f"{m4['speedup']:.2f}x (4 streams)"])
        md1, md4 = e1["model"][0], next(r for r in e1["model"] if r["workers"] == 4)
        writer.writerow(["model", "End-to-end, 100 MiB", "Completion time (s)",
                         f"{md1['time_sec']:.2f}", f"{md4['time_sec']:.2f}", f"{md4['speedup']:.2f}x (4 nodes)"])

        for c in all_results["exp3"]["comparisons"]:
            writer.writerow(["model", f"Campus 10 Mbps ({c['name']}, {c['size_mb']} MiB)", "Completion time (s)",
                             f"{c['t_single_sec']:.1f}", f"{c['t_4nodes_sec']:.1f}", f"{c['speedup_4nodes']:.2f}x (4 nodes)"])

        h = all_results["exp4"]
        writer.writerow(["model", "Heterogeneous scheduling, 500 MiB", "Makespan (s)",
                         f"{h['naive_makespan_sec']:.2f}", f"{h['optimal_makespan_sec']:.2f}", f"{h['makespan_reduction_pct']:.1f}% shorter"])

        for r in all_results["exp5"]["sizes"]:
            writer.writerow(["measured", f"Memory, {r['size_mib']} MiB file", "Peak RSS growth (MiB)",
                             f"{r['inmem_peak_delta_mib']:.1f}", f"{r['direct_peak_delta_mib']:.1f}", "in-memory buffer vs. direct seek-writes"])

        e6 = all_results["exp6"]
        writer.writerow(["measured", f"Engine throughput, {e6['payload_bytes'] // MIB} MiB loopback", "Rate (MiB/s)",
                         "", f"{e6['download_rate_bps'] / MIB:.1f}", f"{e6['download_rate_bps'] / WAN_ACCOUNT_BPS:.0f}x a 10 Mbps account"])
    print(f"[+] Exported summary CSV table  : {csv_path}")


# ============================================================================
# MAIN ENTRYPOINT
# ============================================================================

def main():
    print("=" * 80)
    print(" ⚡ HIGH SPEED CAMPUS DOWNLOADER: BENCHMARKING & PROFILING RUNNER")
    print(" Course: CSE449 (Parallel, Distributed & High-Performance Computing)")
    print(" Authors: Tanjila Afsari Rubina (24241310) & Sandip Kumar Paul (24241311)")
    print("=" * 80)

    work_dir = tempfile.mkdtemp(prefix="edgemesh_bench_")
    overall_start = time.time()

    try:
        exp1_data = run_scaling_experiment(work_dir)
        exp2_data = run_filesize_experiment(work_dir)
        exp3_data = run_campus_quota_experiment()
        exp4_data = run_heterogeneous_experiment()
        exp5_data = run_memory_profiling_experiment(work_dir)
        exp6_data = run_system_profiling_experiment(work_dir)

        generate_presentation_charts(exp1_data, exp2_data, exp3_data, exp4_data, exp5_data, exp6_data)

        all_results = {
            "metadata": {
                "course": "CSE449: Parallel, Distributed & High-Performance Computing",
                "project": "High Speed Campus Downloader for Students (EdgeMesh)",
                "authors": [
                    {"name": "Tanjila Afsari Rubina", "student_id": "24241310"},
                    {"name": "Sandip Kumar Paul", "student_id": "24241311"}
                ],
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                "python": sys.version.split()[0],
                "platform": sys.platform,
                "cpu_count": os.cpu_count(),
                "note": "exp1.measured, exp2, exp5, exp6 are measured on loopback; exp1.model, exp3, exp4 are modeled."
            },
            "exp1": exp1_data,
            "exp2": exp2_data,
            "exp3": exp3_data,
            "exp4": exp4_data,
            "exp5": exp5_data,
            "exp6": exp6_data
        }
        export_results(all_results)

        total_elapsed = time.time() - overall_start
        print("\n" + "=" * 80)
        print(f" 🎉 BENCHMARKING & PROFILING COMPLETE in {total_elapsed:.1f} seconds!")
        print(f" 📂 Figures and datasets saved to: {ASSETS_DIR}")
        print("=" * 80)

    finally:
        shutil.rmtree(work_dir, ignore_errors=True)


if __name__ == "__main__":
    main()
