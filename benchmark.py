"""
====================================================================================================
⚡ HIGH SPEED CAMPUS DOWNLOADER (EDGEMESH) — PERFORMANCE BENCHMARK HARNESS
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

THEORETICAL FOUNDATIONS & BENCHMARK METHODOLOGY:
This automated CLI benchmark evaluates the performance of single-connection baseline downloads
versus parallel multi-stream HTTP range partitioning with out-of-core direct SSD writes.

Metrics Evaluated:
1. Speedup Factor ($S_p$):
   Defined by Amdahl's Law and parallel performance theory as the ratio of serial execution
   time ($T_1$) to parallel execution time on $p$ worker streams ($T_p$):
       $$S_p = \\frac{T_1}{T_p}$$

2. Parallel Efficiency ($E_p$):
   Quantifies how effectively the parallel streams utilize available bandwidth:
       $$E_p = \\frac{S_p}{p} \\times 100\\% = \\frac{T_1}{p \\cdot T_p} \\times 100\\%$$
   In ideal linear speedup, $E_p = 100\\%$. Real-world network efficiency is bounded by:
   - Remote HTTP server connection concurrency throttling.
   - TCP slow-start ramp-up latency across multiple simultaneous connections.
   - Tail latency: makespan is bounded by $\\max_i (T_i)$ where the slowest chunk governs completion.

3. Out-of-Core Bit-for-Bit Cryptographic Integrity:
   Verifies that multi-threaded concurrent seek-writes produce a byte-for-byte identical
   file to the sequential baseline via streaming SHA-256 hash digests.
====================================================================================================
"""

import os
import sys
import time
import argparse
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
from socketserver import ThreadingMixIn
import requests

# Ensure Windows CP1252 terminal handles Unicode safely
if sys.stdout and hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if sys.stderr and hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

from engine import (
    fetch_file_metadata,
    ThreadSafeFileWriter,
    WANChunkDownloader,
    calculate_file_hash,
    format_bytes,
    format_speed
)

# Standard public high-bandwidth test file (100 MB binary asset)
DEFAULT_TEST_URL = "https://speed.hetzner.de/100MB.bin"


class MockRangeHTTPHandler(BaseHTTPRequestHandler):
    """RFC 7233 compliant mock HTTP server for offline benchmark execution."""
    server_payload = b""

    def do_HEAD(self):
        self.send_response(200)
        self.send_header("Content-Length", str(len(self.server_payload)))
        self.send_header("Accept-Ranges", "bytes")
        self.send_header("Content-Disposition", 'attachment; filename="benchmark_asset.bin"')
        self.end_headers()

    def do_GET(self):
        range_header = self.headers.get("Range")
        total_len = len(self.server_payload)
        if range_header and range_header.startswith("bytes="):
            byte_range = range_header.replace("bytes=", "").split("-")
            start = int(byte_range[0])
            end = int(byte_range[1]) if byte_range[1] else total_len - 1
            data = self.server_payload[start:end + 1]
            self.send_response(206)
            self.send_header("Content-Range", f"bytes {start}-{end}/{total_len}")
            self.send_header("Content-Length", str(len(data)))
            self.send_header("Accept-Ranges", "bytes")
            self.end_headers()
            self.wfile.write(data)
        else:
            self.send_response(200)
            self.send_header("Content-Length", str(total_len))
            self.send_header("Accept-Ranges", "bytes")
            self.end_headers()
            self.wfile.write(self.server_payload)

    def log_message(self, format, *args):
        pass


class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
    daemon_threads = True
    allow_reuse_address = True


def start_local_mock_server(size_mb: int = 25):
    """Spawns an ephemeral RFC 7233 compliant local HTTP server."""
    payload = b"EDGEMESH_BENCHMARK_MOCK_STREAM_" * ((size_mb * 1024 * 1024) // 31)
    MockRangeHTTPHandler.server_payload = payload
    server = ThreadedHTTPServer(("127.0.0.1", 0), MockRangeHTTPHandler)
    port = server.server_port
    threading.Thread(target=server.serve_forever, daemon=True).start()
    url = f"http://127.0.0.1:{port}/benchmark_asset.bin"
    return server, url


def benchmark_single_stream(url: str, output_path: str) -> dict:
    """
    Executes a standard single-connection sequential HTTP download to establish baseline ($T_1$).

    Methodology:
    Issues a single standard GET request and streams chunks sequentially to disk,
    measuring total wall-clock elapsed time and average transfer throughput.

    Args:
        url: Remote file URL to download.
        output_path: Local disk destination for the baseline file.

    Returns:
        dict: Performance metrics including execution time (s), total bytes, and average speed (Bytes/s).
    """
    print(f"\n[1/2] 🚀 Running Baseline (Single-Connection Download)...")
    start_time = time.time()
    
    response = requests.get(url, stream=True, timeout=30)
    response.raise_for_status()
    
    total_bytes = 0
    with open(output_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=65536):
            if chunk:
                f.write(chunk)
                total_bytes += len(chunk)

    elapsed = time.time() - start_time
    avg_speed = total_bytes / elapsed if elapsed > 0 else 0
    print(f"      ↳ Completed in {elapsed:.2f}s | Avg Speed: {format_speed(avg_speed)}")

    return {
        "mode": "Single-Stream",
        "time_sec": elapsed,
        "bytes": total_bytes,
        "speed_bps": avg_speed
    }


def benchmark_parallel_stream(url: str, output_path: str, num_chunks: int = 4) -> dict:
    """
    Executes a $p$-way parallel HTTP range download using ThreadSafeFileWriter ($T_p$).

    Methodology:
    1. Probes remote file metadata and decomposes byte envelope into $p$ uniform 1D domains.
    2. Preallocates destination file envelope using sparse allocation.
    3. Spawns $p$ concurrent worker threads pulling byte ranges over independent HTTP connections.
    4. Threads commit incoming buffers directly to disk at designated byte coordinates.
    5. Synchronizes at a thread join barrier and measures total execution makespan.

    Args:
        url: Remote file URL to download.
        output_path: Local disk destination for parallel assembled file.
        num_chunks: Number of concurrent parallel worker streams ($p$).

    Returns:
        dict: Performance metrics including execution time (s), total bytes, and average speed (Bytes/s).
    """
    print(f"\n[2/2] ⚡ Running Parallel ({num_chunks}-Connection HTTP Range Download)...")
    meta = fetch_file_metadata(url, num_chunks=num_chunks)
    
    start_time = time.time()
    writer = ThreadSafeFileWriter(output_path, meta.total_size)
    
    threads = []
    for chunk in meta.chunks:
        downloader = WANChunkDownloader(url, chunk, writer)
        t = threading.Thread(target=downloader.start_download)
        threads.append(t)
        t.start()
        
    for t in threads:
        t.join()
        
    writer.close()
    elapsed = time.time() - start_time
    avg_speed = meta.total_size / elapsed if elapsed > 0 else 0
    print(f"      ↳ Completed in {elapsed:.2f}s | Avg Speed: {format_speed(avg_speed)}")

    return {
        "mode": f"Parallel ({num_chunks} Chunks)",
        "time_sec": elapsed,
        "bytes": meta.total_size,
        "speed_bps": avg_speed
    }


def print_comparison(single_res: dict, parallel_res: dict, single_path: str, parallel_path: str, num_chunks: int = 4):
    """
    Computes and formats academic-grade speedup, efficiency, and cryptographic validation reports.

    Formulations:
        Measured Speedup ($S_p$)     = T_single / T_parallel
        Parallel Efficiency ($E_p$)   = (S_p / num_chunks) * 100%

    Args:
        single_res: Single-stream baseline performance dictionary.
        parallel_res: Parallel execution performance dictionary.
        single_path: Path to baseline downloaded file.
        parallel_path: Path to parallel assembled file.
        num_chunks: Number of parallel streams used.
    """
    speedup = single_res["time_sec"] / parallel_res["time_sec"] if parallel_res["time_sec"] > 0 else 1.0
    efficiency = (speedup / float(num_chunks)) * 100.0

    print("\n" + "=" * 65)
    print(" 📊 HIGH SPEED CAMPUS DOWNLOADER (EDGEMESH) BENCHMARK REPORT")
    print("=" * 65)
    print(f" {'Metric':<25} | {'Single-Stream':<16} | {f'Parallel ({num_chunks}-Way)':<16}")
    print("-" * 65)
    print(f" {'Execution Time':<25} | {single_res['time_sec']:<13.2f} s | {parallel_res['time_sec']:<13.2f} s")
    print(f" {'Effective Throughput':<25} | {format_speed(single_res['speed_bps']):<16} | {format_speed(parallel_res['speed_bps']):<16}")
    print(f" {'Total File Size':<25} | {format_bytes(single_res['bytes']):<16} | {format_bytes(parallel_res['bytes']):<16}")
    print("-" * 65)
    print(f" 🚀 Measured Speedup     : {speedup:.2f}x")
    print(f" 🎯 Parallel Efficiency   : {efficiency:.1f}%")
    print("=" * 65)

    # Cryptographic integrity verification
    print("\n🔒 Verifying Cryptographic Integrity (SHA-256)...")
    hash_single = calculate_file_hash(single_path, "sha256")
    hash_parallel = calculate_file_hash(parallel_path, "sha256")
    print(f"   Single Stream SHA-256 : {hash_single}")
    print(f"   Parallel Mesh SHA-256 : {hash_parallel}")

    if hash_single == hash_parallel:
        print("   ✅ INTEGRITY MATCH: Bit-for-bit identical file assembly verified!")
    else:
        print("   ❌ INTEGRITY MISMATCH: Hash mismatch detected.")


def main():
    """CLI Entrypoint parsing user flags and executing automated benchmark runs."""
    print("=" * 72)
    print("⚡ High Speed Campus Downloader — Performance Benchmark Harness")
    print("Authors: Tanjila Afsari Rubina (24241310) & Sandip Kumar Paul (24241311)")
    print("Course:  CSE449 — Parallel, Distributed & High-Performance Computing")
    print("License: GNU General Public License v3.0 (GPLv3)")
    print("=" * 72)

    parser = argparse.ArgumentParser(description="EdgeMesh Campus Downloader Performance Benchmark")
    parser.add_argument("--url", default=DEFAULT_TEST_URL, help="Target download file URL")
    parser.add_argument("--chunks", type=int, default=4, help="Number of parallel chunks (default: 4)")
    parser.add_argument("--out-dir", default="./benchmark_out", help="Output directory for test downloads")
    parser.add_argument("--mock", action="store_true", help="Force execution against local RFC 7233 mock server")
    args = parser.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)
    single_out = os.path.join(args.out_dir, "baseline.bin")
    parallel_out = os.path.join(args.out_dir, "parallel.bin")

    mock_server = None
    target_url = args.url

    # Probe target URL or fallback to local mock server if offline
    if args.mock:
        print("[i] Launching local RFC 7233 mock HTTP server (--mock specified)...")
        mock_server, target_url = start_local_mock_server(size_mb=25)
    else:
        try:
            r = requests.head(target_url, timeout=3)
            r.raise_for_status()
        except Exception as e:
            print(f"[!] Target URL unreachable ({e.__class__.__name__}).")
            print("[i] Automatically falling back to local RFC 7233 mock server (25 MB dataset)...")
            mock_server, target_url = start_local_mock_server(size_mb=25)

    print(f"Testing URL: {target_url}")
    try:
        single_res = benchmark_single_stream(target_url, single_out)
        parallel_res = benchmark_parallel_stream(target_url, parallel_out, num_chunks=args.chunks)
        print_comparison(single_res, parallel_res, single_out, parallel_out, num_chunks=args.chunks)
    finally:
        if mock_server:
            try:
                mock_server.shutdown()
                mock_server.server_close()
            except Exception:
                pass
        # Cleanup temporary benchmark storage files
        for p in (single_out, parallel_out):
            if os.path.exists(p):
                try:
                    os.remove(p)
                except Exception:
                    pass


if __name__ == "__main__":
    main()
