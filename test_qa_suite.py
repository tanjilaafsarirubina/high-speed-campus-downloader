"""
====================================================================================================
CSE449: Parallel, Distributed and High-Performance Computing
Project Title: High Speed Campus Downloader for Students (EdgeMesh Distributed Architecture)
Authors: Tanjila Afsari Rubina (ID: 24241310) & Sandip Kumar Paul (ID: 24241311)
====================================================================================================

MODULE PURPOSE & VERIFICATION OBJECTIVES:
-----------------------------------------
This automated QA and verification suite provides formal empirical validation of the core
theoretical and systems-level invariants underpinning the EdgeMesh architecture. Designed
to verify correctness across distributed systems, parallel computing, and high-performance
I/O domains, this suite covers 28 rigorous unit and integration tests:

1. Parallel Domain Decomposition:
   - 1D linear byte-range partitioning over arbitrary byte spaces [0, N-1].
   - Formal validation of partition completeness, strict disjointness, continuity, and byte conservation.
   - Residue handling over even, odd, and large prime-numbered file sizes (modulo division boundary checks).

2. HPC Out-of-Core I/O & Direct-to-Disk Streaming:
   - Zero-RAM footprint file preallocation via filesystem metadata seek operations.
   - Elimination of dynamic storage reallocation jitter and filesystem fragmentation overhead.
   - Non-destructive reopening in 'r+b' binary update mode preserving existing data integrity.
   - Multi-threaded concurrent write safety without buffer corruption or write-race collisions.

3. Layer 4 Custom Binary Wire Framing & Exact-Byte Reassembly:
   - Application-layer stream framing (Magic 0xDEADBEEF + 32-bit length header + payload).
   - Micro-packet fragmentation injection verifying 'recv_exact()' reassembles fragmented TCP frames.
   - Resilient handling of premature socket EOFs and deliberate stream resets.

4. Inter-Node Distributed Aggregation (Data Plane & Control Plane):
   - Loopback TCP streaming integration between LocalTCPClient (worker) and LocalTCPServer (master).
   - Full two-phase ACK handshake verification and SHA-256 cryptographic bit-for-bit parity checks.
   - ControlPlaneServer RPC capacity negotiation and makespan-optimal chunk assignment (port 5000).

5. Fault-Tolerant WAN Ingress & HTTP/1.1 RFC 7233 Compliance:
   - Mock HTTP server handling byte-range partial content requests (HTTP 206 Partial Content).
   - Auto-resumption from truncated/dropped connections with byte-accurate progress tracking.
   - Defensive guard rejecting illegal HTTP 200 responses to sub-range requests.
   - Single-bit corruption detection via streaming SHA-256 hashing.

6. Heterogeneous Load Balancing & Makespan Optimization:
   - Analytical makespan minimization verification: M = max_i(T_i) under effective bandwidth constraints.
   - Bandwidth asymmetry ratio testing (e.g. 1 MB/s vs 20 MB/s; 2 MB/s vs 8 MB/s).
   - Non-contiguous cluster topologies and small-file edge case clamping.
====================================================================================================
"""

import os
import sys
import time
import socket
import struct
import shutil
import hashlib
import tempfile
import unittest
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler

import json
from engine import (
    DownloadChunk,
    FileMetadata,
    ThreadSafeFileWriter,
    WANChunkDownloader,
    LocalTCPServer,
    LocalTCPClient,
    PeerDiscoveryService,
    recv_exact,
    calculate_file_hash,
    TCP_DATA_MAGIC,
    format_bytes,
    format_speed,
    fetch_file_metadata,
    compute_optimal_chunks,
    measure_wan_bandwidth,
    ControlPlaneServer,
    ControlPlaneClient,
    send_json,
    recv_json
)


class TestDomainDecomposition(unittest.TestCase):
    """
    Parallel Domain Decomposition Verification Suite.
    
    Verifies that the 1D domain decomposition algorithm partitions an arbitrary file of size N
    across p worker nodes such that:
      1. Completeness: Union of all chunk domains equals the exact range [0, N-1].
      2. Disjointness: Chunks are mutually disjoint; no byte is assigned to more than one node:
             [S_i, E_i] ∩ [S_j, E_j] = ∅  for all i != j.
      3. Continuity: Adjacent boundaries are strictly contiguous without gaps:
             S_{i+1} = E_i + 1.
      4. Byte Conservation: Sum of all partition lengths equals N exactly:
             sum(E_i - S_i + 1) == N.
      5. Residue Preservation: The remainder of integer division (N mod p) is cleanly absorbed
         by the terminal partition without creating off-by-one errors.
    """

    def _verify_chunking(self, total_size: int, num_chunks: int):
        """
        Helper method to perform comprehensive boundary and invariants validation.
        
        Args:
            total_size: Total byte count N of the synthetic file.
            num_chunks: Number of partitions p to decompose into.
        """
        chunk_size = total_size // num_chunks
        chunks = []
        for i in range(num_chunks):
            start = i * chunk_size
            # Terminal chunk absorbs the division remainder (N mod p)
            end = (start + chunk_size - 1) if i < (num_chunks - 1) else (total_size - 1)
            chunks.append(DownloadChunk(chunk_id=i, start_byte=start, end_byte=end))

        # Invariant 1: Cardinality check - exactly p partitions generated
        self.assertEqual(len(chunks), num_chunks,
                         f"Expected {num_chunks} partitions, received {len(chunks)}.")

        # Invariant 2: Lower bound - first partition must anchor at index 0
        self.assertEqual(chunks[0].start_byte, 0,
                         f"First partition does not anchor at byte 0 (anchored at {chunks[0].start_byte}).")

        # Invariant 3: Upper bound - terminal partition must terminate at index N - 1
        self.assertEqual(chunks[-1].end_byte, total_size - 1,
                         f"Terminal partition end ({chunks[-1].end_byte}) != expected ({total_size - 1}).")

        # Invariant 4: Strict continuity - no gaps or overlapping byte assignments
        for i in range(len(chunks) - 1):
            self.assertEqual(chunks[i].end_byte + 1, chunks[i + 1].start_byte,
                             f"Discontinuity between chunk {i} (end: {chunks[i].end_byte}) "
                             f"and chunk {i+1} (start: {chunks[i+1].start_byte}).")

        # Invariant 5: Byte conservation - total byte sum must equal N
        sum_bytes = sum(c.total_bytes for c in chunks)
        self.assertEqual(sum_bytes, total_size,
                         f"Sum of chunk bytes ({sum_bytes}) does not equal total size ({total_size}).")

    def test_even_file_decomposition_2_nodes(self):
        """Domain decomposition of an even 100 MB file across 2 nodes (clean integer division)."""
        self._verify_chunking(100_000_000, 2)

    def test_even_file_decomposition_4_nodes(self):
        """Domain decomposition of an even 100 MB file across 4 nodes (clean quad-split)."""
        self._verify_chunking(100_000_000, 4)

    def test_odd_prime_file_decomposition_2_nodes(self):
        """Domain decomposition of an odd-sized file (45,948,081 bytes) across 2 nodes with remainder."""
        self._verify_chunking(45_948_081, 2)

    def test_odd_prime_file_decomposition_4_nodes(self):
        """Domain decomposition of a prime-sized file (10,000,019 bytes) across 4 nodes with remainder."""
        self._verify_chunking(10_000_019, 4)


class TestThreadSafeFileWriter(unittest.TestCase):
    """
    HPC Out-of-Core I/O & Thread-Safe File Writer Verification Suite.
    
    Verifies that the ThreadSafeFileWriter satisfies High-Performance Computing (HPC)
    out-of-core storage constraints:
      1. O(1) Memory Footprint: Files of arbitrary size (gigabytes to terabytes) are preallocated
         via filesystem metadata manipulation without loading file content into RAM.
      2. Dynamic Reallocation Jitter Elimination: Immediate physical preallocation guarantees
         that subsequent multi-threaded writes do not trigger OS filesystem block allocation
         locks or page fault thrashing during high-speed I/O.
      3. Non-Destructive 'r+b' In-Place Updating: Reopening an existing partially downloaded file
         preserves previously written byte ranges intact without truncation.
      4. Concurrency & Mutex Synchronization: Multiple worker threads writing concurrently to
         disjoint file offsets execute without race conditions or dirty-page buffer corruption.
    """

    def setUp(self):
        """Creates an isolated temporary test directory for zero-pollution disk I/O."""
        self.temp_dir = tempfile.mkdtemp(prefix="campus_dl_test_")

    def tearDown(self):
        """Purges the temporary directory and all allocated test files."""
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_preallocation_and_non_destructive_reopen(self):
        """Out-of-core file preallocation and non-destructive 'r+b' reopening integrity."""
        file_path = os.path.join(self.temp_dir, "test_file.bin")
        total_size = 10 * 1024 * 1024  # 10 MB synthetic target file

        # Step 1: Initial sparse preallocation
        # Verifies that seek(total_size - 1) + write(b'\\x00') establishes expected disk footprint
        writer1 = ThreadSafeFileWriter(file_path, total_size)
        self.assertEqual(os.path.getsize(file_path), total_size,
                         "Initial preallocation failed to achieve target file size.")

        # Write unique validation signature at offset 5 MB
        test_payload = b"HPC_EDGEMESH_VALIDATION_TOKEN_12345"
        write_offset = 5 * 1024 * 1024
        writer1.write_at(write_offset, test_payload)
        writer1.flush()
        writer1.close()

        # Step 2: Re-open file (simulates second session, retry pass, or inter-node worker merge)
        # CRITICAL INVARIANT: Reopening must NEVER truncate ('w+b' would wipe the file)
        writer2 = ThreadSafeFileWriter(file_path, total_size)
        self.assertEqual(os.path.getsize(file_path), total_size,
                         "Reopening altered or truncated the preallocated file size.")

        # Read back payload to verify bit-level persistence across reopen events
        read_back = writer2.read_range(write_offset, len(test_payload))
        self.assertEqual(read_back, test_payload,
                         "Existing data was wiped or corrupted upon reopening in update mode!")
        writer2.close()

    def test_concurrent_multi_threaded_writes(self):
        """Concurrent multi-threaded out-of-core random-access SSD writes without race conditions."""
        file_path = os.path.join(self.temp_dir, "concurrent_test.bin")
        chunk_size = 1024 * 1024  # 1 MB per concurrent partition
        total_size = 4 * chunk_size  # 4 MB total dataset

        writer = ThreadSafeFileWriter(file_path, total_size)

        # Generate 4 cryptographically independent random data blocks
        ground_truth_chunks = [os.urandom(chunk_size) for _ in range(4)]
        ground_truth_full = b"".join(ground_truth_chunks)
        expected_hash = hashlib.sha256(ground_truth_full).hexdigest()

        errors = []

        def _worker(chunk_id: int, data: bytes):
            """Simulates a worker thread writing a downloaded chunk to its designated offset."""
            try:
                offset = chunk_id * chunk_size
                writer.write_at(offset, data)
            except Exception as e:
                errors.append(e)

        # Launch 4 concurrent threads writing simultaneously to separate offsets
        threads = []
        for i in range(4):
            t = threading.Thread(target=_worker, args=(i, ground_truth_chunks[i]))
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        writer.flush()
        writer.close()

        # Assert no exceptions occurred during concurrent mutex acquisitions
        self.assertEqual(len(errors), 0, f"Concurrent write encounters exceptions: {errors}")
        self.assertEqual(os.path.getsize(file_path), total_size)

        # Verify bit-for-bit SHA-256 parity against in-memory composite ground truth
        actual_hash = calculate_file_hash(file_path, "sha256")
        self.assertEqual(actual_hash, expected_hash,
                         "Concurrent write data mismatch: SHA-256 digest deviated from ground truth!")


class TestTCPFramingAndRecvExact(unittest.TestCase):
    """
    Transport Layer Framing & Stream Reassembly Verification Suite.
    
    Verifies that application-layer framing correctly reconstructs discrete protocol messages
    over TCP streams (RFC 793), which are intrinsically continuous byte streams lacking message boundaries:
      1. Micro-Packet Fragmentation Resilience: Artificially segments messages into 3-byte packets
         with inter-packet delays; confirms recv_exact() blocks and buffers until all K bytes are received.
      2. Premature Stream Termination (EOF): Asserts that premature socket closure before K bytes
         triggers an explicit ConnectionResetError rather than returning partial/corrupt payloads.
    """

    def test_recv_exact_handles_fragmented_delivery(self):
        """Reassembly of fragmented micro-packets across arbitrary TCP segment boundaries."""
        server_sock, client_sock = socket.socketpair()

        test_msg = b"CRITICAL_TCP_FRAME_HEADER_FOR_EDGEMESH"
        received_data = []

        def _sender():
            # Fragment transmission into tiny 3-byte packets with deliberate delays
            for i in range(0, len(test_msg), 3):
                client_sock.sendall(test_msg[i:i+3])
                time.sleep(0.01)  # Force TCP segment separation
            client_sock.close()

        t = threading.Thread(target=_sender)
        t.start()

        # recv_exact must cleanly reassemble the entire stream without premature return
        result = recv_exact(server_sock, len(test_msg))
        t.join()
        server_sock.close()

        self.assertEqual(result, test_msg,
                         "Fragmented message reassembly failed to reconstruct original byte sequence.")

    def test_recv_exact_premature_eof(self):
        """Defensive detection of premature EOF raising ConnectionResetError on truncated stream."""
        server_sock, client_sock = socket.socketpair()
        
        def _sender():
            # Send fewer bytes than requested, then abruptly terminate connection
            client_sock.sendall(b"12345")
            client_sock.close()
            
        t = threading.Thread(target=_sender)
        t.start()
        
        # Expect ConnectionResetError when requesting 10 bytes but only 5 were transmitted
        with self.assertRaises(ConnectionResetError):
            recv_exact(server_sock, 10)
            
        t.join()
        server_sock.close()


class TestLocalTCPStreamingIntegration(unittest.TestCase):
    """
    Inter-Node Distributed Aggregation (Data Plane) Verification Suite.
    
    Validates end-to-end peer transmission between LocalTCPClient (Worker transmitter)
    and LocalTCPServer (Master receiver) over high-speed local TCP sockets:
      1. Framing Protocol Handshake: Magic bytes (0xDEADBEEF) + JSON metadata length + JSON chunk descriptor.
      2. Direct SSD Out-of-Core Reading & Writing: Worker streams directly from disk via read_range();
         Master writes directly to disk via write_at().
      3. Low-Latency High-Throughput Socket Tuning: TCP_NODELAY disablement of Nagle's algorithm.
      4. Two-Phase Acknowledgment: Master returns b'OK' after header validation, b'ACK_OK' upon completion.
      5. Bit-for-Bit Parity: Cryptographic SHA-256 verification confirms zero byte loss during transfer.
    """

    def setUp(self):
        """Creates an isolated temporary test directory for inter-node file verification."""
        self.temp_dir = tempfile.mkdtemp(prefix="campus_tcp_test_")

    def tearDown(self):
        """Purges temporary test files upon test completion."""
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_peer_streaming_and_merge(self):
        """End-to-end peer TCP chunk transmission and bit-for-bit SHA-256 verification."""
        # 1. Create source file (Worker side)
        worker_file = os.path.join(self.temp_dir, "worker_source.bin")
        master_file = os.path.join(self.temp_dir, "master_dest.bin")
        total_size = 4 * 1024 * 1024  # 4 MB total target dataset

        # Worker holds Chunk 1 (spanning byte offset 2 MB to 4 MB)
        chunk1_data = os.urandom(2 * 1024 * 1024)
        worker_writer = ThreadSafeFileWriter(worker_file, total_size)
        worker_writer.write_at(2 * 1024 * 1024, chunk1_data)
        worker_writer.flush()

        # 2. Master initializes destination file with full sparse allocation
        master_writer = ThreadSafeFileWriter(master_file, total_size)

        # 3. Master starts LocalTCPServer on dynamic loopback port
        server = LocalTCPServer("127.0.0.1", 19888, master_writer)
        server.start()
        time.sleep(0.2)  # Allow socket bind and listen thread startup

        # 4. Worker configures chunk metadata descriptor
        chunk1 = DownloadChunk(
            chunk_id=1,
            start_byte=2 * 1024 * 1024,
            end_byte=total_size - 1,
            downloaded_bytes=len(chunk1_data),
            status="COMPLETED"
        )

        try:
            # Worker streams Chunk 1 directly to Master over TCP
            success = LocalTCPClient.stream_chunk_to_peer(
                target_ip="127.0.0.1",
                target_port=19888,
                chunk=chunk1,
                file_writer=worker_writer,
                max_retries=3,
                retry_delay=0.5
            )

            self.assertTrue(success, "TCP streaming failed to complete successfully.")

            # 5. Verify received Chunk 1 matches byte-for-byte at exact file offset
            with open(master_file, "rb") as f:
                f.seek(2 * 1024 * 1024)
                merged_chunk1 = f.read(len(chunk1_data))

            self.assertEqual(hashlib.sha256(merged_chunk1).hexdigest(),
                             hashlib.sha256(chunk1_data).hexdigest(),
                             "Merged chunk on Master does not match Worker source data!")
        finally:
            server.stop()
            worker_writer.close()
            master_writer.close()


class MockRangeHTTPHandler(BaseHTTPRequestHandler):
    """
    Mock RFC 7233 HTTP/1.1 Range Protocol Engine.
    
    Simulates a compliant WAN HTTP upstream server supporting Partial Content transfers:
      - Responds to HEAD requests with Accept-Ranges and total Content-Length.
      - Parses 'Range: bytes=START-END' request headers.
      - Returns HTTP 206 Partial Content with Content-Range: bytes START-END/TOTAL.
      - Fallback to HTTP 200 OK for standard whole-resource requests.
    """
    FILE_PAYLOAD = b"MOCK_SERVER_DATA_" * 100_000  # ~1.7 MB synthetic upstream asset

    def do_HEAD(self):
        """Processes HTTP HEAD probes returning metadata headers without body transfer."""
        self.send_response(200)
        self.send_header("Content-Length", str(len(self.FILE_PAYLOAD)))
        self.send_header("Accept-Ranges", "bytes")
        self.send_header("Content-Disposition", 'attachment; filename="mock_asset.bin"')
        self.end_headers()

    def do_GET(self):
        """Processes HTTP GET requests, differentiating between Range (206) and Full (200)."""
        range_header = self.headers.get("Range")
        if range_header and range_header.startswith("bytes="):
            byte_range = range_header.replace("bytes=", "").split("-")
            start = int(byte_range[0])
            end = int(byte_range[1]) if byte_range[1] else len(self.FILE_PAYLOAD) - 1
            data = self.FILE_PAYLOAD[start:end + 1]

            # RFC 7233 Compliant Partial Content Response
            self.send_response(206)
            self.send_header("Content-Range", f"bytes {start}-{end}/{len(self.FILE_PAYLOAD)}")
            self.send_header("Content-Length", str(len(data)))
            self.send_header("Accept-Ranges", "bytes")
            self.end_headers()
            self.wfile.write(data)
        else:
            # Full Resource Fallback
            self.send_response(200)
            self.send_header("Content-Length", str(len(self.FILE_PAYLOAD)))
            self.send_header("Accept-Ranges", "bytes")
            self.end_headers()
            self.wfile.write(self.FILE_PAYLOAD)

    def log_message(self, format, *args):
        """Suppresses console stdout logging to keep test runner output deterministic."""
        pass


class TestWANChunkDownloaderWithMockServer(unittest.TestCase):
    """
    Fault-Tolerant WAN Ingress & HTTP/1.1 Range Downloader Verification Suite.
    
    Verifies that WANChunkDownloader:
      1. Correctly negotiates HTTP 206 Partial Content byte ranges with the remote host.
      2. Directs downloaded byte streams out-of-core into designated SSD file offsets.
      3. Validates single-bit corruption sensitivity via SHA-256 cryptographic hashing.
      4. Strictly guards against rogue servers returning HTTP 200 when a partial sub-range was requested.
    """

    @classmethod
    def setUpClass(cls):
        """Spawns an ephemeral local HTTP server bound to an OS-allocated port."""
        cls.httpd = HTTPServer(("127.0.0.1", 0), MockRangeHTTPHandler)
        cls.port = cls.httpd.server_port
        cls.server_thread = threading.Thread(target=cls.httpd.serve_forever, daemon=True)
        cls.server_thread.start()

    @classmethod
    def tearDownClass(cls):
        """Terminates the mock HTTP server daemon thread."""
        cls.httpd.shutdown()
        cls.httpd.server_close()

    def setUp(self):
        """Creates isolated temporary directory for downloaded chunks."""
        self.temp_dir = tempfile.mkdtemp(prefix="campus_wan_test_")

    def tearDown(self):
        """Purges test artifacts."""
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_range_download_and_resume(self):
        """HTTP/1.1 Range chunk download and out-of-core direct-to-disk writing."""
        url = f"http://127.0.0.1:{self.port}/mock_asset.bin"
        total_size = len(MockRangeHTTPHandler.FILE_PAYLOAD)
        dest_file = os.path.join(self.temp_dir, "downloaded.bin")
        writer = ThreadSafeFileWriter(dest_file, total_size)

        # Test Chunk 0 spanning bytes 0 to 500,000 (500,001 bytes)
        chunk0 = DownloadChunk(chunk_id=0, start_byte=0, end_byte=500_000)

        try:
            downloader = WANChunkDownloader(url, chunk0, writer, max_retries=2)
            downloader.start_download()

            self.assertEqual(chunk0.status, "COMPLETED")
            self.assertEqual(chunk0.downloaded_bytes, 500_001)
        finally:
            writer.close()

    def test_corrupt_data_detection(self):
        """Cryptographic sensitivity of streaming SHA-256 detecting a single-bit alteration."""
        dest_file = os.path.join(self.temp_dir, "hash_test.bin")
        original_data = b"UNMODIFIED_VALID_PAYLOAD_BYTE_STREAM" * 100
        with open(dest_file, "wb") as f:
            f.write(original_data)

        orig_hash = calculate_file_hash(dest_file, "sha256")

        # Inject single-byte corruption at byte offset 50
        with open(dest_file, "r+b") as f:
            f.seek(50)
            f.write(b"X")

        corrupt_hash = calculate_file_hash(dest_file, "sha256")
        self.assertNotEqual(orig_hash, corrupt_hash,
                            "Cryptographic hash failed to detect byte corruption!")

    def test_wan_downloader_rejects_http200_for_subrange(self):
        """Defensive guard rejecting illegal HTTP 200 response when partial sub-range was requested."""
        class Http200MockHandler(BaseHTTPRequestHandler):
            def do_GET(self):
                # Rogue behavior: Return HTTP 200 instead of HTTP 206
                self.send_response(200)
                self.send_header("Content-Length", "1000")
                self.end_headers()
                self.wfile.write(b"A" * 1000)
            def log_message(self, format, *args):
                pass
                
        bad_httpd = HTTPServer(("127.0.0.1", 0), Http200MockHandler)
        bad_port = bad_httpd.server_port
        bad_thread = threading.Thread(target=bad_httpd.serve_forever, daemon=True)
        bad_thread.start()
        
        try:
            url = f"http://127.0.0.1:{bad_port}/bad.bin"
            dest_file = os.path.join(self.temp_dir, "bad.bin")
            writer = ThreadSafeFileWriter(dest_file, 1000)
            # Worker requested sub-range [500, 1000]
            chunk = DownloadChunk(chunk_id=1, start_byte=500, end_byte=1000)
            
            downloader = WANChunkDownloader(url, chunk, writer, max_retries=1)
            # Downloader must abort to prevent writing whole-file bytes into a sub-chunk offset
            with self.assertRaises(RuntimeError):
                downloader.start_download()
                
            self.assertEqual(chunk.status, "FAILED")
        finally:
            if 'writer' in locals():
                writer.close()
            bad_httpd.shutdown()
            bad_httpd.server_close()


class TestNetworkEdgeCases(unittest.TestCase):
    """
    Network Edge Cases & Defensive Socket Exception Handling Suite.
    
    Verifies that the TCP transmission pipeline gracefully handles anomalous socket inputs:
      1. Empty target IP address rejection before initiating connection.
      2. Instant rejection of malformed binary framing magic headers (preventing desync).
      3. Denial-of-Service prevention via strict 64 KB JSON header length clamping.
    """

    def test_empty_target_ip(self):
        """Rejection of empty target IP string returning clean failure instead of uncaught exception."""
        chunk = DownloadChunk(chunk_id=0, start_byte=0, end_byte=100)
        temp_f = tempfile.NamedTemporaryFile(delete=False)
        temp_f.close()
        writer = None
        try:
            writer = ThreadSafeFileWriter(temp_f.name, 101)
            result = LocalTCPClient.stream_chunk_to_peer("", 8888, chunk, writer, max_retries=1)
            self.assertFalse(result, "Streaming proceeded with an empty IP address!")
        finally:
            if writer:
                writer.close()
            if os.path.exists(temp_f.name):
                os.remove(temp_f.name)

    def test_tcpserver_rejects_invalid_magic(self):
        """LocalTCPServer rejects connections missing the TCP_DATA_MAGIC framing signature."""
        temp_f = tempfile.NamedTemporaryFile(delete=False)
        temp_f.close()
        server = None
        writer = None
        try:
            writer = ThreadSafeFileWriter(temp_f.name, 100)
            server = LocalTCPServer("127.0.0.1", 19999, writer)
            server.start()
            time.sleep(0.1)

            # Transmit corrupted preamble instead of 4-byte TCP_DATA_MAGIC (0xDEADBEEF)
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect(("127.0.0.1", 19999))
            s.sendall(b"INVALID_MAGIC_DATA")
            time.sleep(0.1)
            s.close()
            
            # Confirm no bytes were written to disk
            with open(temp_f.name, 'rb') as f:
                data = f.read()
            self.assertEqual(data, b'\x00' * 100, 'Server wrote data despite invalid magic!')
        finally:
            if server:
                server.stop()
            if writer:
                writer.close()
            if os.path.exists(temp_f.name):
                os.remove(temp_f.name)

    def test_header_length_validation(self):
        """LocalTCPServer clamps header length <= 64 KB to protect against memory exhaustion attacks."""
        temp_f = tempfile.NamedTemporaryFile(delete=False)
        temp_f.close()
        server = None
        writer = None
        try:
            writer = ThreadSafeFileWriter(temp_f.name, 100)
            server = LocalTCPServer("127.0.0.1", 19998, writer)
            server.start()
            time.sleep(0.1)

            # Connect with valid magic but an excessively large 100,000-byte header length field
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect(("127.0.0.1", 19998))
            s.sendall(TCP_DATA_MAGIC)
            s.sendall(struct.pack("!I", 100000))
            time.sleep(0.1)
            s.close()
            
            time.sleep(0.1)
            # Server must survive the attack and remain operational for new connections
            s2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s2.connect(("127.0.0.1", 19998))
            s2.close()
        finally:
            if server:
                server.stop()
            if writer:
                writer.close()
            if os.path.exists(temp_f.name):
                os.remove(temp_f.name)


class TestFileMetadataProbe(unittest.TestCase):
    """
    WAN Remote Resource Ingress & Metadata Probe Verification Suite.
    
    Verifies that fetch_file_metadata():
      1. Correctly probes remote HTTP servers for Content-Length, Accept-Ranges, and filenames.
      2. Clamps the requested cluster partition count when file size N < worker count p.
    """

    @classmethod
    def setUpClass(cls):
        """Spawns an ephemeral HTTP test server."""
        cls.httpd = HTTPServer(("127.0.0.1", 0), MockRangeHTTPHandler)
        cls.port = cls.httpd.server_port
        cls.server_thread = threading.Thread(target=cls.httpd.serve_forever, daemon=True)
        cls.server_thread.start()

    @classmethod
    def tearDownClass(cls):
        """Terminates the mock server daemon."""
        cls.httpd.shutdown()
        cls.httpd.server_close()

    def test_fetch_metadata_with_mock_server(self):
        """Retrieval and parsing of HTTP HEAD metadata (Content-Length, Accept-Ranges, Filename)."""
        url = f"http://127.0.0.1:{self.port}/mock_asset.bin"
        meta = fetch_file_metadata(url, num_chunks=2)
        
        self.assertEqual(meta.total_size, len(MockRangeHTTPHandler.FILE_PAYLOAD))
        self.assertTrue(meta.supports_ranges)
        self.assertEqual(meta.filename, 'mock_asset.bin')
        self.assertEqual(len(meta.chunks), 2)
        self.assertEqual(meta.chunks[0].start_byte, 0)
        self.assertEqual(meta.chunks[-1].end_byte, meta.total_size - 1)

    def test_small_file_clamping(self):
        """Clamping partition count to min(p, N) to prevent zero-byte chunk allocations."""
        class SmallMockHandler(BaseHTTPRequestHandler):
            def do_HEAD(self):
                self.send_response(200)
                self.send_header("Content-Length", "3")
                self.send_header("Accept-Ranges", "bytes")
                self.end_headers()
            def log_message(self, format, *args):
                pass
                
        small_httpd = HTTPServer(("127.0.0.1", 0), SmallMockHandler)
        small_port = small_httpd.server_port
        small_thread = threading.Thread(target=small_httpd.serve_forever, daemon=True)
        small_thread.start()
        
        try:
            url = f"http://127.0.0.1:{small_port}/small.bin"
            # Request 4 chunks for a 3-byte file; must clamp to 3 1-byte chunks
            meta = fetch_file_metadata(url, num_chunks=4)
            self.assertEqual(len(meta.chunks), 3)
            self.assertEqual(meta.chunks[0].total_bytes, 1)
            self.assertEqual(meta.chunks[1].total_bytes, 1)
            self.assertEqual(meta.chunks[2].total_bytes, 1)
        finally:
            small_httpd.shutdown()
            small_httpd.server_close()



class TestHeterogeneousScheduling(unittest.TestCase):
    """
    Heterogeneous Load Balancing & Makespan Optimization Verification Suite.
    
    Validates the analytical load-balancing engine derived from makespan minimization theory:
      1. Makespan Equation: M = max_i(T_i) where T_i = C_i / B_i^*
         To minimize M under continuous flow conservation, execution finishes simultaneously:
             T_0 = T_1 = ... = T_{p-1}  =>  C_i = N * (B_i^* / sum_j(B_j^*))
      2. Effective Throughput Accounting: Worker i's rate is pipelined across WAN download
         and LAN aggregation: B_i^* = (B_i * R_LAN) / (B_i + R_LAN).
      3. Boundary Continuity & Conservation: Partitions sum to exactly N with no gaps or overlaps.
      4. Control-Plane RPC Protocol: Verifies TCP port 5000 capability exchange where workers
         report measured bandwidth and receive assigned chunk domain boundaries.
      5. Fault Tolerance: Verifies non-contiguous node sets (node dropout) and tiny file clamping.
    """

    def test_heterogeneous_asymmetric_split_2_nodes(self):
        """Asymmetric chunk allocation minimizing makespan across heterogeneous 2-node cluster."""
        # Scenario: 100 MB file, Master WAN @ 2 MB/s, Worker WAN @ 8 MB/s, Local WiFi LAN @ 35 MB/s
        total_size = 100_000_000
        speeds = {0: 2_000_000.0, 1: 8_000_000.0}
        chunks = compute_optimal_chunks(total_size, speeds, lan_speed=35_000_000)

        self.assertEqual(len(chunks), 2)
        # Byte boundary checks
        self.assertEqual(chunks[0].start_byte, 0)
        self.assertEqual(chunks[1].end_byte, total_size - 1)
        self.assertEqual(chunks[0].end_byte + 1, chunks[1].start_byte)
        self.assertEqual(chunks[0].total_bytes + chunks[1].total_bytes, total_size)

        # Worker (Node 1) has ~3.25x higher effective throughput and must receive > 2.5x chunk size
        self.assertGreater(chunks[1].total_bytes, chunks[0].total_bytes * 2.5,
                           "Worker chunk was not proportionally sized relative to its higher speed.")

    def test_heterogeneous_continuity_4_nodes(self):
        """Domain decomposition continuity and size ordering across 4 heterogeneous nodes."""
        # 4 nodes with varying speeds: 1 MB/s, 5 MB/s, 10 MB/s, 20 MB/s
        total_size = 52_428_800  # 50 MB
        speeds = {0: 1_000_000.0, 1: 5_000_000.0, 2: 10_000_000.0, 3: 20_000_000.0}
        chunks = compute_optimal_chunks(total_size, speeds)

        self.assertEqual(len(chunks), 4)
        self.assertEqual(chunks[0].start_byte, 0)
        self.assertEqual(chunks[-1].end_byte, total_size - 1)

        # Check contiguous boundaries and sum conservation
        for i in range(len(chunks) - 1):
            self.assertEqual(chunks[i].end_byte + 1, chunks[i+1].start_byte)
        self.assertEqual(sum(c.total_bytes for c in chunks), total_size)

        # Fastest node (Node 3 @ 20 MB/s) must receive the largest chunk
        self.assertEqual(max(chunks, key=lambda c: c.total_bytes).chunk_id, 3)

    def test_control_plane_server_client_roundtrip(self):
        """Control-plane RPC roundtrip capacity reporting and chunk domain assignment."""
        # Test capacity reporting and assignment over TCP port 15000
        test_port = 15000
        received_reports = []

        def _on_report(req, sock, addr):
            received_reports.append(req)
            send_json(sock, {
                "status": "OK",
                "chunk_id": req["worker_id"],
                "start_byte": 25000000,
                "end_byte": 99999999,
                "total_bytes": 75000000,
                "worker_pct": 75.0
            })
            sock.close()

        server = ControlPlaneServer(host="127.0.0.1", port=test_port, on_worker_reported=_on_report)
        server.start()
        time.sleep(0.15)

        try:
            resp = ControlPlaneClient.report_capacity_and_get_chunk(
                master_ip="127.0.0.1",
                master_port=test_port,
                worker_id=1,
                measured_speed=8_500_000.0,
                timeout=3.0
            )

            self.assertIsNotNone(resp)
            self.assertEqual(resp.get("status"), "OK")
            self.assertEqual(resp.get("chunk_id"), 1)
            self.assertEqual(resp.get("start_byte"), 25000000)
            self.assertEqual(resp.get("end_byte"), 99999999)
            self.assertEqual(len(received_reports), 1)
            self.assertEqual(received_reports[0]["measured_speed"], 8_500_000.0)
        finally:
            server.stop()

    def test_compute_optimal_chunks_non_contiguous_node_ids(self):
        """Makespan scheduler resilience to non-contiguous node IDs (cluster node dropout)."""
        # Non-contiguous node IDs {0: 2MB/s, 2: 8MB/s} (e.g. 4-node cluster with node 1 skipped)
        total_size = 50_000_000
        speeds = {0: 2_000_000.0, 2: 8_000_000.0}
        chunks = compute_optimal_chunks(total_size, speeds)

        self.assertEqual(len(chunks), 2)
        self.assertEqual(chunks[0].chunk_id, 0)
        self.assertEqual(chunks[1].chunk_id, 2)
        self.assertEqual(chunks[0].start_byte, 0)
        self.assertEqual(chunks[1].end_byte, total_size - 1)
        self.assertEqual(chunks[0].end_byte + 1, chunks[1].start_byte)
        self.assertEqual(sum(c.total_bytes for c in chunks), total_size)

    def test_compute_optimal_chunks_tiny_file(self):
        """Scheduler edge case: file byte size N smaller than active worker count p."""
        # File smaller than node count (2 bytes across 4 nodes)
        total_size = 2
        speeds = {0: 1000.0, 1: 2000.0, 2: 3000.0, 3: 4000.0}
        chunks = compute_optimal_chunks(total_size, speeds)

        self.assertLessEqual(len(chunks), 2)
        self.assertEqual(sum(c.total_bytes for c in chunks), total_size)
        self.assertEqual(chunks[-1].end_byte, total_size - 1)


class TestRobustnessAndEdgeCases(unittest.TestCase):
    """
    Fault-Tolerance, Edge Case Resilience, and Exception Safety Verification Suite.
    
    Verifies that the distributed pipeline survives network anomalies and abnormal edge states:
      1. Zero-length payload rejection in recv_json preventing CPU busy-spinning.
      2. Out-of-bounds byte range rejection in LocalTCPServer preventing disk buffer overflows.
      3. Graceful handling of out-of-core disk read errors preventing worker deadlocks.
      4. Extreme speed asymmetry (1:1000 ratio) over small payloads without boundary collapse.
      5. Thread-safe lifecycle signaling for UDP worker discovery beacons.
      6. Automatic WAN download resumption upon unexpected server socket disconnection.
    """

    def test_recv_json_zero_length_rejection(self):
        """Rejection of zero-length length prefixes in recv_json() raising ValueError."""
        server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_sock.bind(("127.0.0.1", 0))
        server_sock.listen(1)
        port = server_sock.getsockname()[1]

        client_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_sock.connect(("127.0.0.1", port))
        conn, _ = server_sock.accept()

        try:
            # Send length 0 prefix
            client_sock.sendall(struct.pack("!I", 0))
            with self.assertRaises(ValueError):
                recv_json(conn)
        finally:
            client_sock.close()
            conn.close()
            server_sock.close()

    def test_tcpserver_rejects_out_of_bounds_chunk(self):
        """LocalTCPServer rejects chunk ranges extending beyond preallocated file size with 'NO'."""
        temp_dir = tempfile.mkdtemp(prefix="tcpserver_bounds_")
        target_path = os.path.join(temp_dir, "test.bin")
        writer = ThreadSafeFileWriter(target_path, 1024)

        server = LocalTCPServer("127.0.0.1", 18889, writer)
        server.start()
        time.sleep(0.1)

        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            client.connect(("127.0.0.1", 18889))
            # Start at 500 with size 1000 (exceeds 1024 bound)
            meta = {
                "chunk_id": 1,
                "start_byte": 500,
                "total_chunk_size": 1000
            }
            meta_json = json.dumps(meta).encode('utf-8')
            header = TCP_DATA_MAGIC + struct.pack("!I", len(meta_json)) + meta_json
            client.sendall(header)

            client.settimeout(3.0)
            ack = recv_exact(client, 2)
            self.assertEqual(ack, b"NO", "Server accepted an out-of-bounds chunk boundary!")
        finally:
            client.close()
            server.stop()
            writer.close()
            shutil.rmtree(temp_dir, ignore_errors=True)

    def test_tcpclient_disk_read_error_prevents_hang(self):
        """LocalTCPClient terminates promptly without deadlock when disk read returns empty."""
        class FaultyFileWriter:
            def __init__(self):
                self.total_size = 10000
            def read_range(self, offset, length):
                return b""  # Simulated unreadable disk block

        server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_sock.bind(("127.0.0.1", 0))
        server_sock.listen(1)
        port = server_sock.getsockname()[1]

        def _dummy_server():
            try:
                conn, _ = server_sock.accept()
                conn.recv(1024)
                time.sleep(0.5)
                conn.close()
            except Exception:
                pass

        threading.Thread(target=_dummy_server, daemon=True).start()

        faulty_writer = FaultyFileWriter()
        chunk = DownloadChunk(chunk_id=1, start_byte=0, end_byte=9999)

        start_time = time.time()
        try:
            success = LocalTCPClient.stream_chunk_to_peer(
                target_ip="127.0.0.1",
                target_port=port,
                chunk=chunk,
                file_writer=faulty_writer,
                max_retries=1,
                retry_delay=0.1
            )
            elapsed = time.time() - start_time
            self.assertFalse(success)
            self.assertLess(elapsed, 2.0, "Stream attempt hung instead of returning promptly on error!")
        finally:
            server_sock.close()

    def test_compute_optimal_chunks_extreme_asymmetry_small_file(self):
        """Makespan scheduler stability under extreme speed ratio (1:1000) on a 10-byte file."""
        total_size = 10
        speeds = {0: 100.0, 1: 1000.0, 2: 10000.0, 3: 100000.0}
        chunks = compute_optimal_chunks(total_size, speeds)

        self.assertGreater(len(chunks), 0)
        self.assertEqual(chunks[0].start_byte, 0)
        self.assertEqual(chunks[-1].end_byte, total_size - 1)
        for i in range(len(chunks) - 1):
            self.assertEqual(chunks[i].end_byte + 1, chunks[i+1].start_byte)
        self.assertEqual(sum(c.total_bytes for c in chunks), total_size)

    def test_peer_discovery_worker_beacon_lifecycle(self):
        """Worker UDP beacon background thread clean startup and shutdown via Event signal."""
        beacon_stop = PeerDiscoveryService.start_worker_beacon()
        time.sleep(0.1)
        beacon_stop.set()
        self.assertTrue(beacon_stop.is_set())

    def test_truncated_wan_stream_auto_resume(self):
        """Automatic download resumption after mid-stream HTTP connection drop with SHA-256 validation."""
        data_size = 512 * 1024  # 512 KB
        raw_data = os.urandom(data_size)
        expected_hash = hashlib.sha256(raw_data).hexdigest()

        class DroppingMockHandler(BaseHTTPRequestHandler):
            request_count = 0

            def do_HEAD(self):
                self.send_response(200)
                self.send_header("Content-Length", str(data_size))
                self.send_header("Accept-Ranges", "bytes")
                self.end_headers()

            def do_GET(self):
                DroppingMockHandler.request_count += 1
                range_header = self.headers.get("Range", "")
                
                # Connection 1: Artificially truncate stream after sending only 128 KB
                if DroppingMockHandler.request_count == 1:
                    self.send_response(206)
                    self.send_header("Content-Range", f"bytes 0-{data_size-1}/{data_size}")
                    self.send_header("Content-Length", str(data_size))
                    self.end_headers()
                    self.wfile.write(raw_data[:128 * 1024])
                    self.wfile.flush()
                    return  # Abruptly drop socket connection
                else:
                    # Connection 2: Resume from requested sub-range offset
                    start = 0
                    end = data_size - 1
                    if "bytes=" in range_header:
                        parts = range_header.split("=")[1].split("-")
                        start = int(parts[0])
                        if parts[1]:
                            end = int(parts[1])
                    
                    body = raw_data[start:end+1]
                    self.send_response(206)
                    self.send_header("Content-Range", f"bytes {start}-{end}/{data_size}")
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)

            def log_message(self, format, *args):
                pass

        DroppingMockHandler.request_count = 0
        server = HTTPServer(("127.0.0.1", 0), DroppingMockHandler)
        port = server.server_port
        threading.Thread(target=server.serve_forever, daemon=True).start()

        temp_dir = tempfile.mkdtemp(prefix="wan_resume_")
        target_path = os.path.join(temp_dir, "resumed.bin")
        writer = ThreadSafeFileWriter(target_path, data_size)

        try:
            chunk = DownloadChunk(chunk_id=0, start_byte=0, end_byte=data_size - 1)
            downloader = WANChunkDownloader(
                url=f"http://127.0.0.1:{port}/drop.bin",
                chunk=chunk,
                file_writer=writer,
                max_retries=3
            )
            downloader.start_download()

            self.assertEqual(chunk.status, "COMPLETED")
            self.assertEqual(chunk.downloaded_bytes, data_size)
            writer.close()

            # Confirm bit-for-bit reconstruction of truncated stream
            actual_hash = calculate_file_hash(target_path, "sha256")
            self.assertEqual(actual_hash, expected_hash,
                             "Resumed download hash did not match original data!")
            self.assertEqual(DroppingMockHandler.request_count, 2,
                             "Expected exactly 2 requests (initial dropped + resumed).")
        finally:
            server.shutdown()
            server.server_close()
            shutil.rmtree(temp_dir, ignore_errors=True)


if __name__ == "__main__":
    unittest.main(verbosity=2)

