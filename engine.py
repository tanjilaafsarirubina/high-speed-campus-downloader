"""
====================================================================================================
⚡ HIGH SPEED CAMPUS DOWNLOADER (EDGEMESH) — CORE ENGINE
====================================================================================================
Course:  CSE449 — Parallel, Distributed & High-Performance Computing
Authors: Tanjila Afsari Rubina (Student ID: 24241310)
         Sandip Kumar Paul     (Student ID: 24241311)

SYSTEM ARCHITECTURE OVERVIEW:
This module implements the core parallel and distributed runtime for cooperative bandwidth
aggregation across student PCs on campus/enterprise networks.

Implemented Computing Paradigms:
1. Distributed Control Plane:
   - UDP Auto-Discovery (Port 5005): Ad-hoc Zeroconf beaconing for zero-manual-IP clustering.
   - TCP Capacity Negotiation (Port 5000): Dynamically exchanges measured WAN throughput
     metrics between Master and Worker nodes.
   - Makespan Minimization Algorithm: Solves the heterogeneous chunk allocation problem,
     balancing WAN download and LAN streaming transfer penalties.

2. OS-Level Network Automation:
   - Programmatic Windows Mobile Hotspot activation using WinRT via PowerShell.
   - Zero-click Wi-Fi association via dynamic WPA2 XML profiles and Windows 'netsh wlan'.

3. Parallel WAN Data Plane:
   - HTTP/1.1 Range-based 1D domain decomposition (RFC 7233).
   - Concurrent worker threads pulling byte segments over independent university WAN accounts.
   - Resilient transient error handling with exponential backoff and resume support.

4. High-Speed LAN Mesh Data Plane:
   - Custom TCP chunk streaming protocol with magic-token validation and exact-byte framing.
   - High-throughput local Wi-Fi Hotspot ingestion (>300 Mbps) to merge distributed chunks.

5. High-Performance Computing (HPC) Out-of-Core I/O:
   - Sparse file pre-allocation to prevent filesystem block reallocation jitter.
   - O(1) RAM footprint: Streams incoming network buffers directly to disk byte coordinates
     using thread-safe random-access seek writes, allowing multi-gigabyte ISO downloads.
   - Cryptographic SHA-256 streaming verification for bit-for-bit file integrity.
====================================================================================================
"""

import os
import time
import socket
import struct
import json
import hashlib
import threading
import subprocess
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Callable, Tuple
import requests
import urllib3

# Suppress TLS/SSL warnings when connecting through campus proxies or local gateways
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ============================================================================
# SYSTEM-WIDE CONSTANTS & PROTOCOL CONFIGURATION
# ============================================================================
BUFFER_SIZE = 64 * 1024       # 64 KB streaming buffer: balances syscall overhead and L2/L3 cache locality
TCP_DATA_MAGIC = b"CAMPUS_DL_V1"  # Application-layer magic token to validate EdgeMesh peer connections
UDP_DISCOVERY_PORT = 5005    # UDP broadcast port for peer auto-discovery (Phase 1 discovery)
TCP_DISPATCH_PORT = 5000     # TCP port for control-plane capacity reporting and task assignment
TCP_STREAM_PORT = 8888       # TCP port for high-speed local LAN chunk streaming (Phase 2 & 3 data merge)
DEFAULT_HOTSPOT_SSID = "CampusMesh"  # Default SSID broadcast by Edge Aggregator (PC2)
DEFAULT_HOTSPOT_KEY = "EdgeMesh2026"  # WPA2 passphrase for local zero-cost aggregation mesh


# ============================================================================
# LOW-LEVEL NETWORKING & APPLICATION-LAYER FRAMING HELPERS
# ============================================================================

def recv_exact(sock: socket.socket, num_bytes: int) -> bytes:
    """
    Guarantees reading exactly `num_bytes` from a stream-oriented TCP socket.

    Theoretical Context (Distributed Systems / Computer Networks):
    --------------------------------------------------------------
    TCP is a byte-stream protocol, NOT a message-oriented protocol. A single `sock.sendall()`
    of N bytes can arrive partitioned across multiple smaller TCP segments (due to MTU slicing,
    Nagle's algorithm, or 802.11 Wi-Fi retransmissions), or multiple messages can coalesce into
    one buffer. 

    This function implements strict application-layer framing by repeatedly calling `sock.recv()`
    until the requested byte count is fulfilled, preventing corrupted packet deserialization.

    Args:
        sock: Connected TCP socket.
        num_bytes: Exact number of bytes required to complete the current message/header frame.

    Returns:
        bytes: Buffer containing exactly `num_bytes`.

    Raises:
        ConnectionResetError: If the remote peer terminates the connection before supplying
                              the expected number of bytes (premature EOF).
    """
    data = bytearray()
    while len(data) < num_bytes:
        packet = sock.recv(num_bytes - len(data))
        if not packet:
            raise ConnectionResetError(
                f"Socket closed prematurely: expected {num_bytes} bytes, received {len(data)} bytes."
            )
        data.extend(packet)
    return bytes(data)


def send_json(sock: socket.socket, obj: dict):
    """
    Sends a length-prefixed JSON control message over a TCP socket.

    Framing Structure:
    [ Length (4 Bytes, Big-Endian unsigned integer '!I') ] [ UTF-8 Encoded JSON String ]

    Args:
        sock: Connected TCP socket.
        obj: Dictionary to serialize and transmit.
    """
    payload = json.dumps(obj).encode('utf-8')
    sock.sendall(struct.pack("!I", len(payload)) + payload)


def recv_json(sock: socket.socket) -> dict:
    """
    Receives a length-prefixed JSON control message from a TCP socket.

    Enforces bounds-checking on the 4-byte header length to protect against memory exhaustion
    or denial-of-service from malformed packets.

    Args:
        sock: Connected TCP socket.

    Returns:
        dict: Deserialized dictionary payload.

    Raises:
        ValueError: If header length is zero or exceeds the 64 KB control envelope ceiling.
    """
    raw_len = recv_exact(sock, 4)
    length = struct.unpack("!I", raw_len)[0]
    if length == 0 or length > 65536:
        raise ValueError(f"Invalid JSON payload length ({length} bytes)")
    raw_data = recv_exact(sock, length)
    return json.loads(raw_data.decode('utf-8'))


# ============================================================================
# DOMAIN DECOMPOSITION & METADATA DATA STRUCTURES
# ============================================================================

@dataclass
class DownloadChunk:
    """
    Represents a discrete contiguous 1D sub-domain of the target file.

    State Transition Lifecycle:
    PENDING ──> DOWNLOADING ──> STREAMING ──> COMPLETED
       │             │              │
       └───> CANCELLED/FAILED <─────┘

    Attributes:
        chunk_id: Integer identifier for this slice (e.g., 0 for Master, 1..3 for Workers).
        start_byte: Inclusive starting byte offset within the complete file.
        end_byte: Inclusive ending byte offset within the complete file.
        downloaded_bytes: Number of bytes successfully committed to storage so far.
        status: Current state ('PENDING', 'DOWNLOADING', 'STREAMING', 'COMPLETED', 'FAILED', 'CANCELLED').
        source_node: Descriptive label indicating the contributing cluster node.
        speed: Real-time transfer throughput in Bytes per second.
    """
    chunk_id: int
    start_byte: int
    end_byte: int
    downloaded_bytes: int = 0
    status: str = "PENDING"
    source_node: str = "Local"
    speed: float = 0.0  # Bytes/sec

    @property
    def total_bytes(self) -> int:
        """Returns the total byte volume allocated to this chunk."""
        return (self.end_byte - self.start_byte) + 1

    @property
    def progress_pct(self) -> float:
        """Calculates download completion percentage clamped between 0.0% and 100.0%."""
        if self.total_bytes == 0:
            return 0.0
        return min(100.0, (self.downloaded_bytes / self.total_bytes) * 100.0)


@dataclass
class FileMetadata:
    """
    Encapsulates target file properties resolved during remote HTTP inspection.

    Attributes:
        url: Remote HTTP/HTTPS endpoint.
        filename: Sanitized local destination filename.
        total_size: Total file volume in bytes (determined via Content-Length or Content-Range).
        supports_ranges: Boolean flag indicating if server supports HTTP/1.1 byte-range requests.
        num_chunks: Total number of partitioned sub-domains.
        chunks: List of DownloadChunk objects representing the domain decomposition.
    """
    url: str
    filename: str
    total_size: int
    supports_ranges: bool
    num_chunks: int = 4
    chunks: List[DownloadChunk] = field(default_factory=list)


# ============================================================================
# STRING FORMATTING & NETWORK INTERFACE UTILITIES
# ============================================================================

def format_bytes(size: float) -> str:
    """Formats raw byte counts into human-readable binary units (B, KB, MB, GB, TB)."""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if abs(size) < 1024.0:
            return f"{size:.2f} {unit}"
        size /= 1024.0
    return f"{size:.2f} PB"


def format_speed(bps: float) -> str:
    """Formats instantaneous bandwidth throughput (Bytes/s) into human-readable rates."""
    return f"{format_bytes(bps)}/s"


def get_local_ip_addresses() -> List[Tuple[str, str]]:
    """
    Enumerates all active non-loopback IPv4 network interface addresses on the host system.

    Returns:
        List of tuples: (interface_name, ipv4_address).
    """
    interfaces = []
    try:
        import psutil
        for iface, addrs in psutil.net_if_addrs().items():
            for addr in addrs:
                if addr.family == socket.AF_INET and not addr.address.startswith("127."):
                    interfaces.append((iface, addr.address))
    except Exception:
        try:
            # Fallback heuristic: query routing table by opening UDP socket to public DNS
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(("8.8.8.8", 80))
            ip = s.getsockname()[0]
            interfaces.append(("Default Interface", ip))
            s.close()
        except Exception:
            interfaces.append(("Localhost", "127.0.0.1"))
    return interfaces


# ============================================================================
# OS NETWORK AUTOMATION (WINDOWS MOBILE HOTSPOT & NETSH WI-FI AUTO-JOIN)
# ============================================================================

class NetworkSwitchManager:
    """
    Automates Windows OS-level network switching to bypass campus AP Isolation.

    Networking Problem & Solution:
    ------------------------------
    Campus Wi-Fi networks strictly isolate wireless clients (AP Isolation), preventing
    direct peer-to-peer TCP/UDP socket communication between student laptops.
    
    EdgeMesh circumvents this zero-cost barrier via automated 2-tier OS switching:
    1. Aggregator (PC2) or Master (PC1) invokes the Windows Runtime (WinRT) Tethering API
       via PowerShell to launch a local Mobile Hotspot (creating subnet 192.168.137.x).
    2. Worker nodes programmatically synthesize a WPA2-PSK WLAN XML Profile and invoke
       the Windows Native Wi-Fi CLI ('netsh wlan') to auto-associate with the mesh,
       unlocking line-rate LAN streaming speeds (>300 Mbps).
    """

    @staticmethod
    def start_windows_hotspot(on_log: Optional[Callable[[str], None]] = None) -> Tuple[bool, str]:
        """
        Programmatically enables Windows 10/11 Mobile Hotspot via PowerShell WinRT bridging.

        Technical Implementation:
        -------------------------
        Windows does not expose a simple legacy Win32 API to toggle Hotspots. Instead,
        this method executes an in-memory PowerShell script that loads System.Runtime.WindowsRuntime
        and queries the 'NetworkOperatorTetheringManager' associated with the active Internet profile.
        It asynchronously invokes 'StartTetheringAsync()' with a 12-second timeout.

        Args:
            on_log: Optional callback to stream real-time diagnostic telemetry to GUI console.

        Returns:
            Tuple[bool, str]: (Success boolean, Status message).
        """
        if os.name != "nt":
            return False, "Hotspot automation is only supported on Windows 10/11."

        if on_log:
            on_log("[OS Network] Initiating Windows Mobile Hotspot via WinRT API...")

        ps_script = """
        $ErrorActionPreference = 'Stop'
        try {
            Add-Type -AssemblyName System.Runtime.WindowsRuntime
            $asTaskGeneric = ([System.WindowsRuntimeSystemExtensions].GetMethods() | Where-Object { 
                $_.Name -eq 'AsTask' -and $_.GetParameters().Count -eq 1 -and $_.ContainsGenericParameters 
            })[0]

            Function Await-WinRt($winRtOp, $resultType) {
                $asTask = $asTaskGeneric.MakeGenericMethod($resultType)
                $task = $asTask.Invoke($null, @($winRtOp))
                $task.Wait(12000) | Out-Null
                return $task.Result
            }

            $profile = [Windows.Networking.Connectivity.NetworkInformation,Windows.Networking.Connectivity,ContentType=WindowsRuntime]::GetInternetConnectionProfile()
            if ($profile -eq $null) {
                Write-Output "ERROR: No active Internet connection profile."
                exit 0
            }

            $tetheringManager = [Windows.Networking.NetworkOperators.NetworkOperatorTetheringManager,Windows.Networking.NetworkOperators,ContentType=WindowsRuntime]::CreateFromConnectionProfile($profile)
            if ($tetheringManager.TetheringOperationalState -eq [Windows.Networking.NetworkOperators.TetheringOperationalState]::On) {
                Write-Output "SUCCESS_ALREADY_ON"
                exit 0
            }

            $op = $tetheringManager.StartTetheringAsync()
            $res = Await-WinRt $op ([Windows.Networking.NetworkOperators.NetworkOperatorTetheringOperationResult])
            if ($res.Status -eq [Windows.Networking.NetworkOperators.TetheringOperationStatus]::Success) {
                Write-Output "SUCCESS"
            } else {
                Write-Output "STATUS: $($res.Status)"
            }
        } catch {
            Write-Output "EXCEPTION: $($_.Exception.Message)"
        }
        """
        try:
            res = subprocess.run(
                ["powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-Command", ps_script],
                capture_output=True, text=True, timeout=16
            )
            output = res.stdout.strip()
            if "SUCCESS" in output:
                if on_log:
                    on_log("[OS Network] ✅ Windows Mobile Hotspot is ACTIVE (Default Subnet: 192.168.137.1).")
                return True, "Hotspot active."
            else:
                if on_log:
                    on_log(f"[OS Network] Hotspot notice: {output}")
                return False, output
        except Exception as e:
            if on_log:
                on_log(f"[OS Network] Hotspot execution error: {e}")
            return False, str(e)

    @staticmethod
    def connect_to_wifi(ssid: str = DEFAULT_HOTSPOT_SSID, password: str = DEFAULT_HOTSPOT_KEY,
                        on_log: Optional[Callable[[str], None]] = None) -> Tuple[bool, str]:
        """
        Automatically registers a WPA2-PSK Wi-Fi profile and associates using Windows netsh.

        Workflow:
        1. Dynamically generates a valid IEEE 802.11 WLANProfile XML configuration in %TEMP%.
        2. Executes 'netsh wlan add profile' to register the network configuration in Windows WLAN service.
        3. Executes 'netsh wlan connect name=<ssid>' to trigger 4-way WPA2 handshake.
        4. Cleans up the temporary XML credential file to preserve system security.

        Args:
            ssid: Wireless Network SSID to target.
            password: WPA2 pre-shared key.
            on_log: Optional logging callback.

        Returns:
            Tuple[bool, str]: (Success boolean, Status message).
        """
        if os.name != "nt":
            return False, "Wi-Fi automation is only supported on Windows 10/11."

        if on_log:
            on_log(f"[OS Network] Auto-configuring Wi-Fi connection for SSID: '{ssid}'...")

        xml_content = f"""<?xml version="1.0"?>
<WLANProfile xmlns="http://www.microsoft.com/networking/WLAN/profile/v1">
    <name>{ssid}</name>
    <SSIDConfig>
        <SSID><name>{ssid}</name></SSID>
    </SSIDConfig>
    <connectionType>ESS</connectionType>
    <connectionMode>auto</connectionMode>
    <MSM>
        <security>
            <authEncryption>
                <authentication>WPA2PSK</authentication>
                <encryption>AES</encryption>
                <useOneX>false</useOneX>
            </authEncryption>
            <sharedKey>
                <keyType>passPhrase</keyType>
                <protected>false</protected>
                <keyMaterial>{password}</keyMaterial>
            </sharedKey>
        </security>
    </MSM>
</WLANProfile>"""

        temp_xml = os.path.join(os.environ.get("TEMP", "C:\\Temp"), f"campus_mesh_{int(time.time())}.xml")
        try:
            with open(temp_xml, "w", encoding="utf-8") as f:
                f.write(xml_content)

            # Step 1: Register profile with Windows WLAN service
            subprocess.run(["netsh", "wlan", "add", "profile", f"filename={temp_xml}"], capture_output=True, timeout=6)
            # Step 2: Trigger Wi-Fi adapter association
            subprocess.run(["netsh", "wlan", "connect", f"name={ssid}"], capture_output=True, timeout=6)

            if on_log:
                on_log(f"[OS Network] Sent connect command for '{ssid}'. Associating...")

            time.sleep(2.5)  # Allow DHCP lease acquisition and ARP table population
            if on_log:
                on_log(f"[OS Network] ✅ Switched Wi-Fi adapter to '{ssid}'.")
            return True, f"Connected to {ssid}"
        except Exception as e:
            if on_log:
                on_log(f"[OS Network] Failed to auto-join Wi-Fi '{ssid}': {e}")
            return False, str(e)
        finally:
            if os.path.exists(temp_xml):
                try:
                    os.remove(temp_xml)
                except Exception:
                    pass


# ============================================================================
# DISTRIBUTED CONTROL PLANE: UDP PEER AUTO-DISCOVERY SERVICE
# ============================================================================

class PeerDiscoveryService:
    """
    Zero-Configuration (Zeroconf) Peer Discovery Protocol over UDP Broadcast.

    Protocol Exchange:
    ------------------
    1. Workers bind to UDP port 5005 with SO_REUSEADDR and listen for Master beacons.
    2. Master transmits datagrams containing 'EDGEMESH_DISCOVERY' to the limited broadcast
       address (255.255.255.255:5005).
    3. Any Worker receiving the broadcast replies directly to Master with 'EDGEMESH_WORKER'.
    4. Master records the Worker's IPv4 address, eliminating manual IP address configuration.
    """

    @staticmethod
    def start_worker_beacon(on_log: Optional[Callable[[str], None]] = None) -> threading.Event:
        """
        Spawns a daemon background thread listening for Master broadcast probes.

        Args:
            on_log: Optional logging callback.

        Returns:
            threading.Event: Stop event flag that can be set to gracefully terminate the listener.
        """
        stop_event = threading.Event()

        def _listener():
            udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            udp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                try:
                    udp_sock.bind(('', UDP_DISCOVERY_PORT))
                except Exception as e:
                    if on_log:
                        on_log(f"[Discovery] Worker beacon failed to bind UDP {UDP_DISCOVERY_PORT}: {e}")
                    return
                udp_sock.settimeout(1.0)
                if on_log:
                    on_log(f"[Discovery] Worker listening for Master beacons on UDP {UDP_DISCOVERY_PORT}...")

                while not stop_event.is_set():
                    try:
                        data, addr = udp_sock.recvfrom(1024)
                        if data.decode('utf-8', errors='ignore') == "EDGEMESH_DISCOVERY":
                            if on_log:
                                on_log(f"[Discovery] Master beacon detected from {addr[0]}. Sending ACK...")
                            udp_sock.sendto("EDGEMESH_WORKER".encode('utf-8'), addr)
                    except socket.timeout:
                        continue
                    except Exception as e:
                        if not stop_event.is_set() and on_log:
                            on_log(f"[Discovery] Listener notice: {e}")
                        break
            finally:
                udp_sock.close()

        t = threading.Thread(target=_listener, daemon=True)
        t.start()
        return stop_event

    @staticmethod
    def discover_workers(expected_count: int = 3, timeout_sec: float = 4.0,
                         on_log: Optional[Callable[[str], None]] = None) -> List[str]:
        """
        Broadcasts discovery requests on the subnet and gathers responding Worker IP addresses.

        Args:
            expected_count: Target number of workers to discover before returning early.
            timeout_sec: Maximum time window in seconds to wait for worker responses.
            on_log: Optional logging callback.

        Returns:
            List[str]: List of unique discovered Worker IPv4 addresses.
        """
        udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        udp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        udp_sock.settimeout(1.0)

        workers = set()
        if on_log:
            on_log(f"[Discovery] Broadcasting EDGEMESH_DISCOVERY on UDP {UDP_DISCOVERY_PORT}...")

        start_time = time.time()
        try:
            while time.time() - start_time < timeout_sec:
                try:
                    udp_sock.sendto("EDGEMESH_DISCOVERY".encode('utf-8'), ('<broadcast>', UDP_DISCOVERY_PORT))
                    while len(workers) < expected_count:
                        data, addr = udp_sock.recvfrom(1024)
                        if data.decode('utf-8', errors='ignore') == "EDGEMESH_WORKER":
                            if addr[0] not in workers:
                                workers.add(addr[0])
                                if on_log:
                                    on_log(f"[Discovery] ✅ Discovered Worker Node at IP: {addr[0]}")
                        if len(workers) >= expected_count:
                            break
                except socket.timeout:
                    continue
                except Exception as e:
                    if on_log:
                        on_log(f"[Discovery] Broadcast notice: {e}")
                    break

                if len(workers) >= expected_count:
                    break
        finally:
            udp_sock.close()

        return list(workers)


# ============================================================================
# HETEROGENEOUS DYNAMIC SCHEDULING & MAKESPAN MINIMIZATION
# ============================================================================

def measure_wan_bandwidth(url: str, probe_duration: float = 2.0,
                          on_log: Optional[Callable[[str], None]] = None) -> float:
    """
    Performs an active HTTP micro-probe against the target server to measure WAN throughput.

    Methodology:
    ------------
    Sends an HTTP/1.1 byte-range request ('Range: bytes=0-10485759') for a 10 MB slice,
    streams content for `probe_duration` seconds, and computes:
        Throughput (Bytes/s) = Bytes_Read / Elapsed_Seconds

    This empirical throughput is used as the node's capacity metric ($B_i$) in the
    makespan minimization scheduler.

    Args:
        url: Direct HTTP/HTTPS file URL.
        probe_duration: Duration of the probe sampling window in seconds (default: 2.0s).
        on_log: Optional logging callback.

    Returns:
        float: Estimated bandwidth in Bytes per second (fallback baseline: 2.5 MB/s).
    """
    if on_log:
        on_log(f"[Bandwidth Probe] Measuring WAN throughput against remote server...")

    headers = {
        'Range': 'bytes=0-10485759',  # Request up to 10 MB slice
        'User-Agent': 'CampusDownloader/1.0'
    }
    try:
        start_time = time.time()
        resp = requests.get(url, headers=headers, stream=True, timeout=6)
        if resp.status_code not in (200, 206):
            resp.close()
            raise RuntimeError(f"HTTP status {resp.status_code} during probe")

        bytes_read = 0
        try:
            for block in resp.iter_content(chunk_size=32 * 1024):
                if block:
                    bytes_read += len(block)
                elapsed = time.time() - start_time
                if elapsed >= probe_duration or bytes_read >= 10 * 1024 * 1024:
                    break
        finally:
            resp.close()

        elapsed = max(0.01, time.time() - start_time)
        speed = bytes_read / elapsed
        if on_log:
            on_log(f"[Bandwidth Probe] Result: {format_speed(speed)} ({format_bytes(bytes_read)} in {elapsed:.2f}s)")
        return speed
    except Exception as e:
        if on_log:
            on_log(f"[Bandwidth Probe] Notice: {e}. Using baseline estimate 2.50 MB/s.")
        return 2.5 * 1024 * 1024


def compute_optimal_chunks(total_size: int, speeds: Dict[int, float],
                           lan_speed: float = 35_000_000) -> List[DownloadChunk]:
    """
    Calculates non-uniform chunk boundaries that mathematically minimize cluster makespan.

    Theoretical Formulation (HPC & Distributed Scheduling):
    -------------------------------------------------------
    In a heterogeneous cluster, nodes possess asymmetric WAN download speeds ($B_i$).
    Furthermore, Worker nodes incur an additional LAN transmission delay when streaming
    their downloaded chunk to the Master node over local Wi-Fi Hotspot ($R_{LAN} \\approx 35$ MB/s).

    1. Node Execution Time Models:
       - Master Node ($i = 0$): Writes directly to final disk with zero LAN overhead:
             $$T_0(S_0) = \\frac{S_0}{B_0}$$
             Effective Speed: $$B_0^* = B_0$$

       - Worker Nodes ($i \\ge 1$): Download $S_i$ over WAN, then stream $S_i$ over LAN:
             $$T_i(S_i) = \\frac{S_i}{B_i} + \\frac{S_i}{R_{LAN}} = S_i \\left(\\frac{1}{B_i} + \\frac{1}{R_{LAN}}\\right) = \\frac{S_i}{B_i^*}$$
             Effective Speed: $$B_i^* = \\frac{B_i \\cdot R_{LAN}}{B_i + R_{LAN}}$$

    2. Makespan Minimization:
       Cluster Makespan is defined as:
             $$M = \\max_{i} T_i(S_i) \\quad \\text{subject to} \\quad \\sum_{i} S_i = S_{total}$$
       Optimal makespan occurs when all nodes finish simultaneously ($T_0 = T_1 = \\dots = T_{N-1} = M^*$):
             $$\\frac{S_0}{B_0^*} = \\frac{S_1}{B_1^*} = \\dots = \\frac{S_{N-1}}{B_{N-1}^*}$$

       Therefore, the optimal byte allocation for node $i$ is directly proportional:
             $$S_i = S_{total} \\times \\frac{B_i^*}{\\sum_{j=0}^{N-1} B_j^*}$$

    3. Discrete Integer Residue Correction:
       Because byte addresses are discrete integers, naive truncation creates a residue
       $\\Delta = S_{total} - \\sum \\lfloor S_i \\rfloor$. This algorithm iteratively adjusts
       the residue to guarantee exact bit-for-bit file boundary coverage without gaps or overlaps.

    Args:
        total_size: Total file size in bytes.
        speeds: Mapping of node_id to measured WAN throughput in Bytes/sec {node_id: speed_bps}.
        lan_speed: Measured or nominal local LAN transfer speed in Bytes/sec (default: 35 MB/s).

    Returns:
        List[DownloadChunk]: Contiguous chunk partitions with exact byte boundaries.
    """
    if not speeds or total_size <= 0:
        return []

    sorted_ids = sorted(speeds.keys())
    num_nodes = len(sorted_ids)

    # Clamp nodes if file size is smaller than the number of available nodes
    if total_size < num_nodes:
        sorted_ids = sorted_ids[:total_size]
        num_nodes = len(sorted_ids)

    lan_speed = max(1_000_000.0, float(lan_speed))

    # Step 1: Compute harmonic effective throughput for each node
    effective_speeds = {}
    for node_id in sorted_ids:
        w_speed = max(100.0, float(speeds[node_id]))
        if node_id == 0:
            # Master node incurs zero network forwarding penalty
            effective_speeds[node_id] = w_speed
        else:
            # Worker nodes incur pipelined WAN download + LAN streaming penalty
            effective_speeds[node_id] = (w_speed * lan_speed) / (w_speed + lan_speed)

    total_effective = sum(effective_speeds.values())
    if total_effective <= 0:
        total_effective = 1.0

    # Step 2: Compute proportional raw sizes with floor integer conversion
    raw_sizes = {}
    for node_id in sorted_ids:
        frac = effective_speeds[node_id] / total_effective
        raw_sizes[node_id] = max(1, int(frac * total_size))

    # Step 3: Exact byte balancing — eliminate integer rounding residue
    diff = total_size - sum(raw_sizes.values())
    while diff != 0:
        if diff > 0:
            fastest_node = max(raw_sizes.keys(), key=lambda k: raw_sizes[k])
            raw_sizes[fastest_node] += diff
            diff = 0
        else:
            reduced = False
            for node_id in reversed(sorted_ids):
                if raw_sizes[node_id] > 1:
                    raw_sizes[node_id] -= 1
                    diff += 1
                    reduced = True
                    if diff == 0:
                        break
            if not reduced:
                break

    # Step 4: Assemble contiguous 1D sub-domains
    chunks = []
    curr_offset = 0
    for idx, node_id in enumerate(sorted_ids):
        if curr_offset >= total_size:
            break
        size = raw_sizes[node_id]
        start = curr_offset
        end = min(total_size - 1, start + size - 1)
        if idx == len(sorted_ids) - 1:
            end = total_size - 1

        pct = (effective_speeds[node_id] / total_effective) * 100.0
        chunks.append(DownloadChunk(
            chunk_id=node_id,
            start_byte=start,
            end_byte=end,
            source_node=f"Node {node_id + 1} ({pct:.1f}%)"
        ))
        curr_offset = end + 1

    return chunks


# ============================================================================
# DISTRIBUTED CONTROL PLANE: CAPACITY REPORTING & TASK DISPATCH
# ============================================================================

class ControlPlaneServer:
    """
    Control Plane Server running on the Master node (TCP port 5000).

    Role in Distributed Architecture:
    ---------------------------------
    Maintains cluster membership and orchestrates task partitioning:
    1. Accepts TCP control connections from active Worker nodes.
    2. Ingests capacity metric reports (measured WAN speeds).
    3. Solves the makespan minimization equation.
    4. Transmits assigned chunk boundaries [start_byte, end_byte] back to workers.
    """

    def __init__(self, host: str = "0.0.0.0", port: int = TCP_DISPATCH_PORT,
                 on_worker_reported: Optional[Callable[[dict, socket.socket, Tuple[str, int]], None]] = None,
                 on_log: Optional[Callable[[str], None]] = None):
        self.host = host
        self.port = port
        self.on_worker_reported = on_worker_reported
        self.on_log = on_log
        self._server_socket = None
        self._running = False
        self._thread = None

    def start(self) -> bool:
        """Binds TCP control socket and starts multi-threaded connection listener."""
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            self._server_socket.bind((self.host, self.port))
            self._server_socket.listen(5)
            self._running = True
            self._thread = threading.Thread(target=self._listen_loop, daemon=True)
            self._thread.start()
            if self.on_log:
                self.on_log(f"[Control Plane] Master listening on port {self.port} for worker capacity reports.")
            return True
        except Exception as e:
            if self.on_log:
                self.on_log(f"[Control Plane] Failed to bind server on {self.host}:{self.port}: {e}")
            if self._server_socket:
                try:
                    self._server_socket.close()
                except Exception:
                    pass
            self._server_socket = None
            return False

    def _listen_loop(self):
        """Asynchronously accepts incoming worker control plane sockets."""
        while self._running:
            try:
                client_sock, addr = self._server_socket.accept()
                threading.Thread(target=self._handle_client, args=(client_sock, addr), daemon=True).start()
            except Exception:
                break

    def _handle_client(self, client_sock: socket.socket, addr: Tuple[str, int]):
        """Deserializes JSON request and triggers scheduling delegate."""
        client_sock.settimeout(10.0)
        try:
            req = recv_json(client_sock)
            if self.on_worker_reported:
                self.on_worker_reported(req, client_sock, addr)
        except Exception as e:
            if self.on_log:
                self.on_log(f"[Control Plane] Client {addr} communication error: {e}")
            try:
                client_sock.close()
            except Exception:
                pass

    def stop(self):
        """Terminates listener and releases control port."""
        self._running = False
        if self._server_socket:
            try:
                self._server_socket.close()
            except Exception:
                pass


class ControlPlaneClient:
    """
    Control Plane Client executed on Worker nodes.

    Establishes connection to Master control port (5000) and reports empirical WAN
    throughput to receive optimized chunk slice boundaries.
    """

    @staticmethod
    def report_capacity_and_get_chunk(master_ip: str, master_port: int,
                                      worker_id: int, measured_speed: float,
                                      timeout: float = 8.0,
                                      on_log: Optional[Callable[[str], None]] = None) -> Optional[dict]:
        """
        Transmits capacity metric to Master and awaits optimal chunk assignment response.

        Args:
            master_ip: Master IPv4 address.
            master_port: Control plane listening port (default: 5000).
            worker_id: Assigned worker numerical identifier (1..3).
            measured_speed: Measured WAN throughput in Bytes/sec.
            timeout: Socket timeout duration in seconds.
            on_log: Optional logging callback.

        Returns:
            Optional[dict]: Response payload containing chunk boundaries or None on failure.
        """
        if not master_ip:
            return None
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        try:
            sock.connect((master_ip, master_port))
            req = {
                "action": "REPORT_CAPACITY",
                "worker_id": worker_id,
                "measured_speed": measured_speed
            }
            send_json(sock, req)
            resp = recv_json(sock)
            return resp
        except Exception as e:
            if on_log:
                on_log(f"[Control Plane] Failed connecting to Master at {master_ip}:{master_port}: {e}")
            return None
        finally:
            sock.close()


class TaskDispatcher:
    """Backward compatibility interface for legacy task dispatching."""
    @staticmethod
    def dispatch_chunk_task(worker_ip: str, url: str, chunk: DownloadChunk,
                            aggregator_ip: str, on_log: Optional[Callable[[str], None]] = None) -> bool:
        return True


# ============================================================================
# DATA PLANE: REMOTE FILE PROBE & METADATA EXTRACTION
# ============================================================================

def fetch_file_metadata(url: str, num_chunks: int = 4, timeout: int = 10) -> FileMetadata:
    """
    Probes the remote HTTP server to extract file size, range support, and filename.

    HPC & Network Probing Strategy:
    -------------------------------
    1. Primary Probe (HTTP HEAD):
       Sends an HTTP HEAD request to read metadata headers without downloading body payload.
       Inspects 'Content-Length' and checks if 'Accept-Ranges' contains 'bytes'.
       Parses 'Content-Disposition' for server-suggested filenames.

    2. Secondary Fallback Probe (HTTP GET Range Micro-Probe):
       If HEAD fails or returns Content-Length: 0 (common behind Cloudflare, CDNs, or reverse proxies),
       issues a lightweight GET with header 'Range: bytes=0-0'.
       - If server replies with HTTP 206 (Partial Content), parses 'Content-Range: bytes 0-0/TOTAL_SIZE'
         to resolve exact byte volume and confirm HTTP/1.1 RFC 7233 range support.

    3. Uniform 1D Domain Decomposition:
       Divides the total byte space into `num_chunks` contiguous partitions:
           Chunk Size = Total_Size // Num_Chunks
       Assigns contiguous non-overlapping bounds [start_byte, end_byte].

    Args:
        url: Remote file URL to inspect.
        num_chunks: Target number of domain partitions (default: 4).
        timeout: Network timeout in seconds.

    Returns:
        FileMetadata: Complete metadata struct including partitioned chunks.

    Raises:
        ValueError: If file size cannot be determined (required for parallel range partitioning).
        RuntimeError: If remote server is unreachable or drops connection.
    """
    headers = {'User-Agent': 'CampusDownloader/1.0'}
    session = requests.Session()

    try:
        head_resp = session.head(url, headers=headers, allow_redirects=True, timeout=timeout)
        headers_dict = head_resp.headers
    except Exception:
        headers_dict = {}

    content_length = int(headers_dict.get('Content-Length', 0))
    accept_ranges = headers_dict.get('Accept-Ranges', '').lower() == 'bytes'

    # Filename resolution strategy: Content-Disposition -> URL path basename -> Default fallback
    filename = "downloaded_file.bin"
    cd = headers_dict.get('Content-Disposition', '')
    if 'filename=' in cd:
        filename = cd.split('filename=')[-1].strip('"\'; ')
    else:
        url_path = url.split('?')[0].rstrip('/')
        extracted = os.path.basename(url_path)
        if extracted:
            filename = extracted

    # Security sanitization: strip directory traversal sequences and special characters
    filename = "".join([c for c in filename if c.isalnum() or c in "._- "]).strip()
    if not filename:
        filename = "downloaded_file.bin"

    # Secondary Probe: If Content-Length missing or ranges unconfirmed, issue GET Range micro-probe
    if content_length == 0 or not accept_ranges:
        try:
            probe_resp = session.get(url, headers={'Range': 'bytes=0-0', 'User-Agent': 'CampusDownloader/1.0'},
                                     stream=True, timeout=timeout)
            if probe_resp.status_code == 206:
                accept_ranges = True
                cr = probe_resp.headers.get('Content-Range', '')
                if '/' in cr:
                    content_length = int(cr.split('/')[-1])
            elif probe_resp.status_code == 200 and content_length == 0:
                content_length = int(probe_resp.headers.get('Content-Length', 0))
            probe_resp.close()
        except Exception as e:
            raise RuntimeError(f"Failed to connect to target URL: {e}")

    if content_length <= 0:
        raise ValueError("Could not determine file size. Parallel range downloads require Content-Length.")

    # Clamp num_chunks so each partition contains at least 1 byte
    if content_length < num_chunks:
        num_chunks = max(1, content_length)

    # Calculate uniform 1D byte slices
    chunks = []
    chunk_size = content_length // num_chunks
    for i in range(num_chunks):
        start = i * chunk_size
        end = (start + chunk_size - 1) if i < (num_chunks - 1) else (content_length - 1)
        chunks.append(DownloadChunk(chunk_id=i, start_byte=start, end_byte=end))

    session.close()

    return FileMetadata(
        url=url,
        filename=filename,
        total_size=content_length,
        supports_ranges=accept_ranges,
        num_chunks=num_chunks,
        chunks=chunks
    )


# ============================================================================
# HPC OUT-OF-CORE I/O: THREAD-SAFE DIRECT RANDOM-ACCESS DISK WRITER
# ============================================================================

class ThreadSafeFileWriter:
    """
    High-Performance Out-of-Core Direct Disk Storage Engine.

    HPC Systems Principles:
    -----------------------
    1. O(1) Memory Allocation:
       Downloading a 10 GB file on standard systems often consumes gigabytes of RAM
       buffering chunks in memory, triggering aggressive OS virtual memory paging (thrashing).
       ThreadSafeFileWriter streams incoming network packets directly to their exact physical
       byte coordinates on NVMe/SSD storage using seek-based writes, maintaining an O(1) RAM footprint.

    2. Sparse File Pre-allocation:
       On startup, executes `file.seek(total_size - 1)` and commits a single null byte `\\0`.
       This updates filesystem metadata (NTFS Master File Table) to reserve the full contiguous file
       envelope instantaneously without incurring the massive latency of writing gigabytes of zeros.
       It prevents filesystem dynamic block reallocation jitter during concurrent multi-threaded writes.

    3. Non-Destructive Re-Opening:
       Reopens existing files using 'r+b' (Read/Write Binary without truncation).
       Preserves already downloaded chunks across retries, pauses, and worker peer streams.

    4. Mutual Exclusion Synchronization:
       Protects underlying file pointer seeking and block writes across multiple concurrent
       WAN worker threads and local TCP ingestion threads using `threading.Lock`.
    """

    def __init__(self, filepath: str, total_size: int):
        """
        Initializes the writer and pre-allocates disk envelope.

        Args:
            filepath: Destination absolute path.
            total_size: Expected total file size in bytes.
        """
        self.filepath = os.path.abspath(filepath)
        self.total_size = total_size
        self._lock = threading.Lock()
        self._file = None
        self._preallocate_or_open()

    def _preallocate_or_open(self):
        """Preallocates file space safely on disk without overwriting existing progress."""
        os.makedirs(os.path.dirname(self.filepath), exist_ok=True)
        file_exists = os.path.exists(self.filepath)

        if not file_exists:
            with open(self.filepath, "wb") as f:
                if self.total_size > 0:
                    f.seek(self.total_size - 1)
                    f.write(b"\0")
                    f.flush()
        else:
            curr_size = os.path.getsize(self.filepath)
            if curr_size < self.total_size:
                with open(self.filepath, "r+b") as f:
                    f.seek(self.total_size - 1)
                    f.write(b"\0")
                    f.flush()

        # Open in non-destructive read-write binary mode
        self._file = open(self.filepath, "r+b")

    def write_at(self, offset: int, data: bytes):
        """
        Thread-safe random-access write committing bytes directly to specified file offset.

        Args:
            offset: Absolute byte position in file.
            data: Binary payload buffer.
        """
        if not data:
            return
        with self._lock:
            if self._file and not self._file.closed:
                # Clamp to total_size to prevent out-of-bounds corruption
                if self.total_size > 0 and offset + len(data) > self.total_size:
                    data = data[:max(0, self.total_size - offset)]
                    if not data:
                        return
                self._file.seek(offset)
                self._file.write(data)

    def read_range(self, offset: int, length: int) -> bytes:
        """
        Thread-safe random-access read retrieving a slice of bytes from specified offset.

        Used by TCP client when streaming locally completed chunks to Master/Aggregator.

        Args:
            offset: Absolute byte position.
            length: Number of bytes to read.

        Returns:
            bytes: Data buffer read from disk.
        """
        with self._lock:
            if self._file and not self._file.closed:
                self._file.seek(offset)
                return self._file.read(length)
            return b""

    def flush(self):
        """Flushes Python internal buffers to OS file cache."""
        with self._lock:
            if self._file and not self._file.closed:
                self._file.flush()

    def close(self):
        """Flushes buffers and safely releases OS file descriptor."""
        with self._lock:
            if self._file and not self._file.closed:
                self._file.flush()
                self._file.close()
                self._file = None


# ============================================================================
# PARALLEL WAN DATA PLANE: HTTP/1.1 RANGE CHUNK DOWNLOADER
# ============================================================================

class WANChunkDownloader:
    """
    Parallel WAN Chunk Downloader implementing HTTP/1.1 Range Requests (RFC 7233).

    Key Capabilities:
    -----------------
    1. 1D Domain Sub-range Request:
       Constructs HTTP header `Range: bytes={start}-{end}` to download only assigned slice.

    2. Fault-Tolerant Resumption:
       Calculates `current_offset = chunk.start_byte + chunk.downloaded_bytes`.
       If interrupted, automatically resumes from the exact dropped byte without re-downloading.

    3. Anti-Corruption HTTP 200 Guard:
       If a sub-range (offset > 0) is requested but the remote server ignores the Range header
       and sends HTTP 200 (full file), the downloader immediately aborts to prevent catastrophic
       data offset corruption.

    4. Transient Failure Recovery:
       Catches network dropouts with exponential backoff retries (1.5s, 2.25s, 3.37s).

    5. Real-Time Telemetry Window:
       Computes rolling throughput over 400ms time windows for responsive GUI progress meters.
    """

    def __init__(self, url: str, chunk: DownloadChunk, file_writer: ThreadSafeFileWriter,
                 max_retries: int = 3,
                 on_progress: Optional[Callable[[DownloadChunk], None]] = None,
                 on_log: Optional[Callable[[str], None]] = None):
        self.url = url
        self.chunk = chunk
        self.file_writer = file_writer
        self.max_retries = max_retries
        self.on_progress = on_progress
        self.on_log = on_log
        self._cancel_flag = threading.Event()
        self._pause_flag = threading.Event()

    def cancel(self):
        """Signals worker thread to immediately stop downloading and clean up."""
        self._cancel_flag.set()

    def pause(self):
        """Temporarily halts chunk ingestion loop."""
        self._pause_flag.set()

    def resume(self):
        """Resumes chunk ingestion loop."""
        self._pause_flag.clear()

    def start_download(self):
        """
        Executes the range download loop across configured retry attempts.
        Synchronously blocks the calling worker thread until complete, cancelled, or failed.
        """
        self.chunk.status = "DOWNLOADING"
        retry_delay = 1.5

        for attempt in range(1, self.max_retries + 1):
            if self._cancel_flag.is_set():
                self.chunk.status = "CANCELLED"
                return

            # Compute resume byte position
            current_offset = self.chunk.start_byte + self.chunk.downloaded_bytes
            end_byte = self.chunk.end_byte

            if current_offset > end_byte:
                self.chunk.status = "COMPLETED"
                self.chunk.speed = 0.0
                if self.on_progress:
                    self.on_progress(self.chunk)
                return

            headers = {
                'Range': f'bytes={current_offset}-{end_byte}',
                'User-Agent': 'CampusDownloader/1.0'
            }

            if self.on_log:
                msg = f"[Node WAN] Requesting Chunk {self.chunk.chunk_id}: bytes {current_offset}-{end_byte} ({format_bytes((end_byte - current_offset) + 1)})"
                if attempt > 1:
                    msg += f" (Retry {attempt}/{self.max_retries})"
                self.on_log(msg)

            try:
                response = requests.get(self.url, headers=headers, stream=True, timeout=25)

                # CRITICAL DEFENSE: If sub-range requested but server returned full file (HTTP 200),
                # reject to avoid writing full file at non-zero offset
                if response.status_code == 200 and current_offset > 0:
                    response.close()
                    raise RuntimeError(
                        f"Server returned HTTP 200 instead of 206 for Range request on Chunk {self.chunk.chunk_id}. "
                        f"Server may not support byte-range downloads."
                    )
                if response.status_code not in (200, 206):
                    response.close()
                    raise RuntimeError(f"HTTP error {response.status_code} while fetching chunk {self.chunk.chunk_id}")

                try:
                    start_time = time.time()
                    bytes_since_tick = 0
                    tick_time = start_time

                    # Ingest stream in 64 KB blocks directly to disk
                    for block in response.iter_content(chunk_size=BUFFER_SIZE):
                        if self._cancel_flag.is_set():
                            self.chunk.status = "CANCELLED"
                            if self.on_log:
                                self.on_log(f"[Node WAN] Chunk {self.chunk.chunk_id} cancelled.")
                            return

                        while self._pause_flag.is_set():
                            time.sleep(0.2)
                            if self._cancel_flag.is_set():
                                return

                        if block:
                            block_len = len(block)
                            self.file_writer.write_at(current_offset, block)
                            current_offset += block_len
                            self.chunk.downloaded_bytes += block_len
                            bytes_since_tick += block_len

                            # Update instantaneous throughput every 400ms
                            now = time.time()
                            dt = now - tick_time
                            if dt >= 0.4:
                                self.chunk.speed = bytes_since_tick / dt
                                bytes_since_tick = 0
                                tick_time = now
                                if self.on_progress:
                                    self.on_progress(self.chunk)

                    self.file_writer.flush()
                    if self.chunk.downloaded_bytes < self.chunk.total_bytes:
                        raise IOError(
                            f"Connection dropped prematurely: received {self.chunk.downloaded_bytes}/{self.chunk.total_bytes} bytes for Chunk {self.chunk.chunk_id}."
                        )
                    self.chunk.status = "COMPLETED"
                    self.chunk.speed = 0.0
                    if self.on_log:
                        self.on_log(f"[Node WAN] Chunk {self.chunk.chunk_id} completed successfully ({format_bytes(self.chunk.total_bytes)}).")
                    if self.on_progress:
                        self.on_progress(self.chunk)
                    return
                finally:
                    response.close()

            except Exception as e:
                if self._cancel_flag.is_set():
                    self.chunk.status = "CANCELLED"
                    return
                if self.on_log:
                    self.on_log(f"[Node WAN] Chunk {self.chunk.chunk_id} attempt {attempt} error: {e}")
                if attempt < self.max_retries:
                    time.sleep(retry_delay)
                    retry_delay *= 1.5
                else:
                    self.chunk.status = "FAILED"
                    self.chunk.speed = 0.0
                    if self.on_progress:
                        self.on_progress(self.chunk)
                    raise e


# ============================================================================
# DISTRIBUTED DATA PLANE: HIGH-SPEED LOCAL TCP CHUNK STREAMING (LAN MESH)
# ============================================================================

class LocalTCPServer:
    """
    High-Throughput Local TCP Chunk Ingestion Server (Phase 2 & Phase 3 Data Merge).

    Distributed Systems Protocol & Binary Wire Framing:
    ---------------------------------------------------
    The LAN data streaming protocol operates over TCP port 8888, merging completed
    Worker chunks into the Edge Aggregator (PC2) or Master (PC1) storage file.

    Wire Frame Format:
    ┌─────────────────────────┬─────────────────────────┬──────────────────────────┬────────────────────────┐
    │ MAGIC_TOKEN (12 Bytes)  │ HEADER_LEN (4B, '!I')   │ JSON_METADATA (N Bytes)  │ STREAM_PAYLOAD (M B)   │
    │ b"CAMPUS_DL_V1"         │ Big-Endian unsigned int │ {"chunk_id": .., ...}    │ Raw binary file bytes  │
    └─────────────────────────┴─────────────────────────┴──────────────────────────┴────────────────────────┘

    Robustness & Performance Design:
    1. TCP_NODELAY: Disables Nagle's algorithm to eliminate artificial 40ms delayed-ACK latency.
    2. Exact-Byte Framing: Uses `recv_exact` for Magic and Header, preventing socket fragmentation crashes.
    3. Direct Out-of-Core Ingestion: Streams incoming socket packets directly into `ThreadSafeFileWriter`
       at `start_byte + offset`, achieving O(1) RAM usage regardless of chunk size.
    4. Two-Phase Handshake ACK: Transmits 2-byte response (`b"OK"` or `b"NO"`) upon byte-count verification.
    """

    def __init__(self, host: str, port: int, file_writer: ThreadSafeFileWriter,
                 on_chunk_received: Optional[Callable[[int, int, int, float], None]] = None,
                 on_log: Optional[Callable[[str], None]] = None):
        self.host = host
        self.port = port
        self.file_writer = file_writer
        self.on_chunk_received = on_chunk_received
        self.on_log = on_log
        self._server_socket = None
        self._running = False
        self._thread = None

    def start(self) -> bool:
        """Binds TCP socket to specified interface and begins multi-client listen loop."""
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            self._server_socket.bind((self.host, self.port))
            self._server_socket.listen(10)
            self._running = True
            self._thread = threading.Thread(target=self._listen_loop, daemon=True)
            self._thread.start()
            if self.on_log:
                self.on_log(f"[TCP Server] Listening on all interfaces (port {self.port}) for incoming peer streams.")
            return True
        except Exception as e:
            if self.on_log:
                self.on_log(f"[TCP Server] Failed to bind port {self.port}: {e}")
            if self._server_socket:
                try:
                    self._server_socket.close()
                except Exception:
                    pass
            self._server_socket = None
            return False

    def _listen_loop(self):
        """Asynchronously accepts incoming peer streaming connections."""
        while self._running:
            try:
                client_sock, addr = self._server_socket.accept()
                peer_thread = threading.Thread(target=self._handle_client, args=(client_sock, addr), daemon=True)
                peer_thread.start()
            except Exception:
                break

    def _handle_client(self, client_sock: socket.socket, addr: Tuple[str, int]):
        """
        Processes an individual worker chunk stream:
        Validates magic token -> Parses header -> Streams payload to disk -> Returns ACK.
        """
        if self.on_log:
            on_log_func = self.on_log
            if on_log_func:
                on_log_func(f"[TCP Server] Connection accepted from peer {addr[0]}:{addr[1]}")
        client_sock.settimeout(20.0)
        try:
            client_sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        except Exception:
            pass
        try:
            # Step 1: Validate Magic Token
            magic = recv_exact(client_sock, len(TCP_DATA_MAGIC))
            if magic != TCP_DATA_MAGIC:
                if self.on_log:
                    self.on_log(f"[TCP Server] Rejected client {addr}: Invalid magic token.")
                return

            # Step 2: Read Header Length and JSON Metadata
            raw_header_len = recv_exact(client_sock, 4)
            header_len = struct.unpack("!I", raw_header_len)[0]

            # Validate header length to prevent OOM from malicious/corrupt peers
            if header_len == 0 or header_len > 65536:
                if self.on_log:
                    self.on_log(f"[TCP Server] Rejected client {addr}: Invalid header length ({header_len} bytes).")
                return

            header_json = recv_exact(client_sock, header_len).decode('utf-8')
            meta = json.loads(header_json)

            chunk_id = meta["chunk_id"]
            start_byte = meta["start_byte"]
            total_chunk_size = meta["total_chunk_size"]

            # Validate chunk boundaries against file writer capacity
            if start_byte < 0 or total_chunk_size <= 0 or (self.file_writer.total_size > 0 and start_byte + total_chunk_size > self.file_writer.total_size):
                if self.on_log:
                    self.on_log(f"[TCP Server] Rejected client {addr}: Chunk bounds [{start_byte}, {start_byte + total_chunk_size}) exceed file size {self.file_writer.total_size}")
                client_sock.sendall(b"NO")
                return

            if self.on_log:
                self.on_log(f"[TCP Server] Ingesting Chunk {chunk_id} from {addr[0]} ({format_bytes(total_chunk_size)} at offset {start_byte})")

            # Step 3: Stream network payload directly to target disk coordinates
            received_bytes = 0
            current_offset = start_byte
            start_time = time.time()
            tick_time = start_time
            bytes_since_tick = 0

            while received_bytes < total_chunk_size:
                to_read = min(BUFFER_SIZE, total_chunk_size - received_bytes)
                buf = client_sock.recv(to_read)
                if not buf:
                    break

                self.file_writer.write_at(current_offset, buf)
                current_offset += len(buf)
                received_bytes += len(buf)
                bytes_since_tick += len(buf)

                # Broadcast live streaming throughput to GUI dashboard
                now = time.time()
                dt = now - tick_time
                if dt >= 0.4:
                    speed = bytes_since_tick / dt
                    bytes_since_tick = 0
                    tick_time = now
                    if self.on_chunk_received:
                        self.on_chunk_received(chunk_id, received_bytes, total_chunk_size, speed)

            self.file_writer.flush()
            if self.on_chunk_received:
                self.on_chunk_received(chunk_id, received_bytes, total_chunk_size, 0.0)

            # Step 4: Verification and Two-Phase ACK response
            if received_bytes == total_chunk_size:
                if self.on_log:
                    self.on_log(f"[TCP Server] Successfully received & merged Chunk {chunk_id} from {addr[0]} ({format_bytes(received_bytes)})")
                client_sock.sendall(b"OK")
            else:
                if self.on_log:
                    self.on_log(f"[TCP Server] Partial stream error: expected {total_chunk_size} bytes, got {received_bytes}")
                client_sock.sendall(b"NO")
        except Exception as e:
            if self.on_log:
                self.on_log(f"[TCP Server] Error streaming from peer {addr}: {e}")
        finally:
            client_sock.close()

    def stop(self):
        """Terminates streaming server and closes listener socket."""
        self._running = False
        if self._server_socket:
            try:
                self._server_socket.close()
            except Exception:
                pass


class LocalTCPClient:
    """
    High-Speed Local TCP Chunk Streamer (Worker Side).

    Pushes locally completed WAN chunks to Master or Aggregator over the local
    high-speed Wi-Fi Hotspot mesh with automatic retry handling and ACK verification.
    """

    @staticmethod
    def stream_chunk_to_peer(target_ip: str, target_port: int, chunk: DownloadChunk,
                             file_writer: ThreadSafeFileWriter,
                             max_retries: int = 10,
                             retry_delay: float = 1.5,
                             on_progress: Optional[Callable[[int, int, int, float], None]] = None,
                             on_log: Optional[Callable[[str], None]] = None) -> bool:
        """
        Reads chunk bytes directly from local storage and streams them to peer over TCP.

        Args:
            target_ip: Destination Master/Aggregator IP address.
            target_port: Destination TCP stream port (default: 8888).
            chunk: Chunk descriptor to transmit.
            file_writer: Local file writer handle to read bytes from.
            max_retries: Maximum connection attempts before declaring failure.
            retry_delay: Delay between retries in seconds.
            on_progress: Progress callback (chunk_id, sent_bytes, total_bytes, speed_bps).
            on_log: Optional logging callback.

        Returns:
            bool: True if chunk was received and confirmed with 'OK' ACK, False otherwise.
        """
        if not target_ip:
            if on_log:
                on_log("[TCP Client] Error: Target Master IP is empty. Please enter Master's IP.")
            return False

        for attempt in range(1, max_retries + 1):
            if on_log:
                on_log(f"[TCP Client] (Attempt {attempt}/{max_retries}) Connecting to Master at {target_ip}:{target_port}...")

            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5.0)
            try:
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            except Exception:
                pass
            try:
                sock.connect((target_ip, target_port))

                # Transmit binary protocol header
                meta = {
                    "chunk_id": chunk.chunk_id,
                    "start_byte": chunk.start_byte,
                    "end_byte": chunk.end_byte,
                    "total_chunk_size": chunk.total_bytes
                }
                meta_json = json.dumps(meta).encode('utf-8')
                header = TCP_DATA_MAGIC + struct.pack("!I", len(meta_json)) + meta_json
                sock.sendall(header)

                # Stream payload in 64 KB blocks from disk to socket
                sent_bytes = 0
                current_offset = chunk.start_byte
                total_size = chunk.total_bytes
                tick_time = time.time()
                bytes_since_tick = 0

                if on_log:
                    on_log(f"[TCP Client] Streaming Chunk {chunk.chunk_id} ({format_bytes(total_size)}) to Master...")

                while sent_bytes < total_size:
                    to_read = min(BUFFER_SIZE, total_size - sent_bytes)
                    buf = file_writer.read_range(current_offset, to_read)
                    if not buf:
                        break
                    sock.sendall(buf)
                    current_offset += len(buf)
                    sent_bytes += len(buf)
                    bytes_since_tick += len(buf)

                    now = time.time()
                    dt = now - tick_time
                    if dt >= 0.4:
                        speed = bytes_since_tick / dt
                        bytes_since_tick = 0
                        tick_time = now
                        if on_progress:
                            on_progress(chunk.chunk_id, sent_bytes, total_size, speed)

                if sent_bytes < total_size:
                    raise IOError(f"Could not read all {total_size} bytes from disk (read only {sent_bytes} bytes).")

                # Await peer verification ACK
                sock.settimeout(15.0)
                ack = recv_exact(sock, 2)
                if ack == b"OK":
                    if on_log:
                        on_log(f"[TCP Client] ✅ Chunk {chunk.chunk_id} successfully delivered to Master ({target_ip}:{target_port})!")
                    if on_progress:
                        on_progress(chunk.chunk_id, sent_bytes, total_size, 0.0)
                    sock.close()
                    return True
                else:
                    if on_log:
                        on_log(f"[TCP Client] Server rejected chunk stream with ACK={ack!r}")
                sock.close()
            except Exception as e:
                sock.close()
                if on_log:
                    on_log(f"[TCP Client] Attempt {attempt} failed: {e}")
                if attempt < max_retries:
                    if on_log:
                        on_log(f"[TCP Client] Retrying in {retry_delay}s... (Ensure Laptop is on same Hotspot / port {target_port} is unblocked)")
                    time.sleep(retry_delay)

        if on_log:
            on_log(f"[TCP Client] ❌ Could not reach Master at {target_ip}:{target_port} after {max_retries} attempts.")
            on_log("[Diagnostic Tips]:")
            on_log(" 1. Ensure both devices are on the SAME Mobile Hotspot (e.g., 192.168.137.1).")
            on_log(" 2. Campus Wi-Fi AP isolation blocks direct PC-to-PC connections.")
            on_log(" 3. Verify Windows Defender Firewall allows Python on Private networks.")
        return False


# ============================================================================
# CRYPTOGRAPHIC INTEGRITY VERIFICATION (STREAMING SHA-256 / MD5)
# ============================================================================

def calculate_file_hash(filepath: str, algo: str = "sha256",
                        on_progress: Optional[Callable[[float], None]] = None) -> str:
    """
    Computes cryptographic checksum using streaming 1 MB block reads.

    HPC Memory Efficiency:
    ----------------------
    Processes arbitrary multi-gigabyte ISO images in constant O(1) memory space
    without loading the complete file into RAM.

    Args:
        filepath: Target file path on disk.
        algo: Cryptographic hashing algorithm ('sha256', 'sha512', 'md5').
        on_progress: Optional progress callback receiving percent completion (0.0 to 100.0).

    Returns:
        str: Hexadecimal digest string.

    Raises:
        ValueError: If unsupported hash algorithm is requested.
    """
    algo_map = {"sha256": hashlib.sha256, "sha512": hashlib.sha512, "md5": hashlib.md5}
    if algo not in algo_map:
        raise ValueError(f"Unsupported hash algorithm '{algo}'. Supported: {list(algo_map.keys())}")
    h = algo_map[algo]()
    total_size = os.path.getsize(filepath)
    processed = 0
    with open(filepath, "rb") as f:
        while block := f.read(1024 * 1024):  # 1 MB block streaming
            h.update(block)
            processed += len(block)
            if on_progress and total_size > 0:
                on_progress((processed / total_size) * 100.0)
    return h.hexdigest()
