"""
====================================================================================================
⚡ HIGH SPEED CAMPUS DOWNLOADER (EDGEMESH) — DASHBOARD & CONTROLLER
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

DASHBOARD & DISTRIBUTED CONTROLLER ARCHITECTURE:
This module provides the modern GUI and cluster orchestrator built using CustomTkinter.
It decouples heavy concurrent network I/O and HPC storage writes from the graphical thread.

Key Responsibilities & Design Patterns:
1. Thread-Safe Event Model:
   - All network operations (HTTP range downloads, socket servers, TCP streams, hash computations)
     execute on dedicated background worker daemon threads.
   - GUI state updates are dispatched safely onto the Tkinter main event loop using `queue.Queue`
     and asynchronous polling intervals (`self.after()`), preventing UI thread deadlocks or freezes.

2. Distributed Cluster State Machine:
   - STANDALONE: Simulates multi-stream WAN range downloading locally on a single machine.
   - MASTER (PC1): Downloads Chunk 0 over university Wi-Fi, hosts the TCP stream ingestion server
     on port 8888, and guides the student through Phase 3 network switching to assemble the final file.
   - AGGREGATOR (PC2): Broadcasts a Windows Mobile Hotspot mesh, downloads Chunk 1, and ingests
     peer streams from PC3 and PC4 before forwarding them to Master.
   - WORKER (PC3/PC4): Downloads assigned chunk over campus Wi-Fi, auto-joins the local mesh,
     and streams completed blocks directly to the Aggregator or Master.

3. Heterogeneous Auto-Balancing Orchestrator:
   - Interactively triggers active WAN micro-probes and control-plane negotiations (Port 5000)
     to configure mathematically optimal chunk boundaries that minimize total cluster makespan.

4. Real-Time Distributed Telemetry:
   - Dynamic 4-node grid rendering per-node progress bars, byte offsets, instantaneous transfer
     speeds, state labels, and real-time aggregated speedup multipliers.
====================================================================================================
"""

import os
import sys
import time
import queue
import threading
import subprocess
from tkinter import filedialog, messagebox
import customtkinter as ctk

from engine import (
    FileMetadata,
    DownloadChunk,
    ThreadSafeFileWriter,
    WANChunkDownloader,
    LocalTCPServer,
    LocalTCPClient,
    PeerDiscoveryService,
    NetworkSwitchManager,
    fetch_file_metadata,
    get_local_ip_addresses,
    format_bytes,
    format_speed,
    calculate_file_hash,
    measure_wan_bandwidth,
    compute_optimal_chunks,
    ControlPlaneServer,
    ControlPlaneClient,
    TCP_DISPATCH_PORT,
    TCP_STREAM_PORT,
    DEFAULT_HOTSPOT_SSID,
    DEFAULT_HOTSPOT_KEY,
    send_json,
    recv_json
)

# Set global CustomTkinter theme
ctk.set_appearance_mode("Dark")
ctk.set_default_color_theme("blue")


class EdgeMeshApp(ctk.CTk):
    """
    Main Application Window and Cluster Lifecycle Controller.

    Manages UI widgets, background worker threads, network server/client lifecycles,
    and telemetry monitoring timers.
    """

    def __init__(self):
        super().__init__()

        self.title("⚡ High Speed Campus Downloader — EdgeMesh")
        self.geometry("1160x890")
        self.minsize(980, 740)

        # --------------------------------------------------------------------
        # Application Runtime State
        # --------------------------------------------------------------------
        self.metadata: FileMetadata = None
        self.file_writer: ThreadSafeFileWriter = None
        self.active_downloaders = []
        self._downloaders_lock = threading.Lock()
        self.tcp_server: LocalTCPServer = None
        self.control_plane_server: ControlPlaneServer = None
        self._master_speed: float = 0.0
        self._master_speed_event = threading.Event()
        self._worker_speeds: dict = {}
        self.worker_beacon_stop = None
        self.is_running = False
        self._finished_lock = threading.Lock()
        self.log_queue = queue.Queue()
        self.target_filepath = ""

        # --------------------------------------------------------------------
        # Initialize UI Components
        # --------------------------------------------------------------------
        self._create_header()
        self._create_main_layout()
        self._create_status_bar()

        # --------------------------------------------------------------------
        # Periodic Telemetry & Logging Dispatchers (Main Event Loop)
        # --------------------------------------------------------------------
        self.after(100, self._process_log_queue)
        self.after(400, self._update_telemetry_loop)

        # Graceful application termination hook
        self.protocol("WM_DELETE_WINDOW", self._on_close)

    def _on_close(self):
        """Cleanly halts all background threads, network servers, and closes open files on exit."""
        if self.control_plane_server:
            self.control_plane_server.stop()
        if self.tcp_server:
            self.tcp_server.stop()
        if self.worker_beacon_stop:
            self.worker_beacon_stop.set()
        with self._downloaders_lock:
            for d in self.active_downloaders:
                d.cancel()
        time.sleep(0.5)
        if self.file_writer:
            self.file_writer.close()
        self.destroy()

    # ========================================================================
    # UI BUILDER METHODS
    # ========================================================================

    def _create_header(self):
        """Renders top header banner with title, course metadata, and local IP detection badge."""
        header_frame = ctk.CTkFrame(self, corner_radius=0, fg_color=("#1A1D24", "#12141A"))
        header_frame.pack(fill="x")

        title_box = ctk.CTkFrame(header_frame, fg_color="transparent")
        title_box.pack(side="left", padx=20, pady=12)

        title_label = ctk.CTkLabel(
            title_box,
            text="⚡ High Speed Campus Downloader",
            font=ctk.CTkFont(size=20, weight="bold")
        )
        title_label.pack(anchor="w")

        subtitle_label = ctk.CTkLabel(
            title_box,
            text="Cooperative Bandwidth Aggregation | Tanjila (24241310) & Sandip (24241311) | CSE449 [GPLv3]",
            font=ctk.CTkFont(size=12, weight="bold"),
            text_color="#81D4FA"
        )
        subtitle_label.pack(anchor="w")

        # Network IP Detection Pill (helps student find IP for peer connections)
        self.ip_box = ctk.CTkFrame(header_frame, fg_color=("#2B2F38", "#1E222B"), corner_radius=8)
        self.ip_box.pack(side="right", padx=20, pady=10)

        local_ips = get_local_ip_addresses()
        ip_summary = " | ".join([f"{name}: {ip}" for name, ip in local_ips[:2]]) if local_ips else "127.0.0.1"
        self.ip_label = ctk.CTkLabel(
            self.ip_box,
            text=f"📡 Node IP(s): {ip_summary}",
            font=ctk.CTkFont(size=12, weight="bold"),
            text_color="#4FC3F7"
        )
        self.ip_label.pack(padx=12, pady=6)

    def _create_main_layout(self):
        """
        Constructs the scrollable main container housing the four operational cards:
        1. Remote File Inspection & Range Probe Card
        2. Cluster Topology, Role Configuration & OS Automation Card
        3. Real-Time Distributed Striping Telemetry Grid
        4. HPC Engine Execution Log Console
        """
        self.main_container = ctk.CTkScrollableFrame(self, fg_color="transparent")
        self.main_container.pack(fill="both", expand=True, padx=20, pady=15)

        # 1. Target URL & Remote Probe Card
        self._create_url_card()

        # 2. Topology & Cluster Role Setup Card
        self._create_topology_card()

        # 3. Real-Time Distributed Telemetry Grid
        self._create_telemetry_card()

        # 4. HPC Engine Logs
        self._create_log_card()

    def _create_url_card(self):
        """
        Builds Card 1: Remote HTTP/HTTPS File URL input, destination folder selection,
        and remote range inspection trigger.
        """
        card = ctk.CTkFrame(self.main_container, corner_radius=12)
        card.pack(fill="x", pady=(0, 15))

        ctk.CTkLabel(card, text="1. Target File & Remote Range Probe", font=ctk.CTkFont(size=15, weight="bold")).pack(anchor="w", padx=16, pady=(12, 6))

        url_frame = ctk.CTkFrame(card, fg_color="transparent")
        url_frame.pack(fill="x", padx=16, pady=4)

        self.url_entry = ctk.CTkEntry(
            url_frame,
            placeholder_text="Enter direct file URL (e.g., https://speed.hetzner.de/100MB.bin)...",
            height=38
        )
        self.url_entry.insert(0, "https://speed.hetzner.de/100MB.bin")
        self.url_entry.pack(side="left", fill="x", expand=True, padx=(0, 10))

        self.probe_btn = ctk.CTkButton(url_frame, text="🔍 Inspect File", width=130, height=38, command=self._on_inspect_url)
        self.probe_btn.pack(side="right")

        meta_frame = ctk.CTkFrame(card, fg_color="transparent")
        meta_frame.pack(fill="x", padx=16, pady=(6, 12))

        self.save_dir_entry = ctk.CTkEntry(meta_frame, height=32)
        self.save_dir_entry.insert(0, os.path.join(os.path.expanduser("~"), "Downloads"))
        self.save_dir_entry.pack(side="left", fill="x", expand=True, padx=(0, 10))

        self.browse_btn = ctk.CTkButton(meta_frame, text="📁 Save Folder", width=130, height=32, command=self._on_browse_dir)
        self.browse_btn.pack(side="right")

        self.file_info_label = ctk.CTkLabel(
            card,
            text="File Status: Awaiting inspection...",
            font=ctk.CTkFont(size=12),
            text_color="gray60"
        )
        self.file_info_label.pack(anchor="w", padx=16, pady=(0, 10))

    def _create_topology_card(self):
        """
        Builds Card 2: Cluster role selection, topology size, heterogeneous makespan
        auto-balance trigger, peer target IP input, and OS network automation controls.
        """
        card = ctk.CTkFrame(self.main_container, corner_radius=12)
        card.pack(fill="x", pady=(0, 15))

        top_row = ctk.CTkFrame(card, fg_color="transparent")
        top_row.pack(fill="x", padx=16, pady=(12, 6))

        ctk.CTkLabel(top_row, text="2. Cluster Role & Cooperative Mesh Setup", font=ctk.CTkFont(size=15, weight="bold")).pack(side="left")

        # Cluster Size selection
        size_frame = ctk.CTkFrame(top_row, fg_color="transparent")
        size_frame.pack(side="right")

        ctk.CTkLabel(size_frame, text="Cluster Topology:", font=ctk.CTkFont(size=12, weight="bold")).pack(side="left", padx=(0, 6))
        self.cluster_size_var = ctk.StringVar(value="2 Nodes (50% each)")
        self.cluster_size_menu = ctk.CTkOptionMenu(
            size_frame,
            values=["2 Nodes (50% each)", "4 Nodes (25% each)"],
            variable=self.cluster_size_var,
            width=160,
            command=self._on_cluster_size_change
        )
        self.cluster_size_menu.pack(side="left")

        self.auto_dist_btn = ctk.CTkButton(
            size_frame,
            text="⚡ Auto-Balance Chunks",
            width=175,
            font=ctk.CTkFont(size=12, weight="bold"),
            fg_color="#D81B60",
            hover_color="#C2185B",
            command=self._on_auto_distribute_chunks
        )
        self.auto_dist_btn.pack(side="left", padx=(10, 0))

        # Role Selector
        role_row = ctk.CTkFrame(card, fg_color="transparent")
        role_row.pack(fill="x", padx=16, pady=4)

        self.role_var = ctk.StringVar(value="STANDALONE")
        self.role_seg = ctk.CTkSegmentedButton(
            role_row,
            values=["STANDALONE", "Master (PC1)", "Aggregator (PC2)", "Worker (PC2/PC3/PC4)"],
            variable=self.role_var,
            command=self._on_role_change
        )
        self.role_seg.pack(fill="x", expand=True)

        # Dynamic Instructions Banner
        self.role_tip_label = ctk.CTkLabel(
            card,
            text="💡 Standalone Mode: Simulates parallel WAN download on 1 machine without needing separate PCs.",
            font=ctk.CTkFont(size=12),
            text_color="#81D4FA"
        )
        self.role_tip_label.pack(anchor="w", padx=16, pady=(6, 4))

        # Peer configuration row
        self.peer_config_frame = ctk.CTkFrame(card, fg_color=("#252A34", "#181B22"), corner_radius=8)
        self.peer_config_frame.pack(fill="x", padx=16, pady=(4, 12))

        self.peer_ip_label = ctk.CTkLabel(self.peer_config_frame, text="Target Master IP:")
        self.peer_ip_label.grid(row=0, column=0, padx=8, pady=8, sticky="w")

        self.peer_ip_entry = ctk.CTkEntry(self.peer_config_frame, placeholder_text="192.168.137.1", width=140)
        self.peer_ip_entry.insert(0, "192.168.137.1")
        self.peer_ip_entry.grid(row=0, column=1, padx=8, pady=8, sticky="w")

        self.worker_chunk_label = ctk.CTkLabel(self.peer_config_frame, text="Chunk:")
        self.worker_chunk_label.grid(row=0, column=2, padx=8, pady=8, sticky="w")

        self.worker_chunk_combo = ctk.CTkComboBox(
            self.peer_config_frame,
            values=["Chunk 1 (50-100%)"],
            width=140
        )
        self.worker_chunk_combo.grid(row=0, column=3, padx=8, pady=8, sticky="w")

        # Automation Action Buttons
        self.auto_hotspot_btn = ctk.CTkButton(
            self.peer_config_frame,
            text="⚡ Start Hotspot",
            width=130,
            fg_color="#0288D1",
            hover_color="#0277BD",
            command=self._on_auto_start_hotspot
        )
        self.auto_hotspot_btn.grid(row=0, column=4, padx=6, pady=8, sticky="w")

        self.auto_join_btn = ctk.CTkButton(
            self.peer_config_frame,
            text="⚡ Auto-Join Wi-Fi",
            width=130,
            fg_color="#7B1FA2",
            hover_color="#6A1B9A",
            command=self._on_auto_join_wifi
        )
        self.auto_join_btn.grid(row=0, column=5, padx=6, pady=8, sticky="w")

        self.manual_push_btn = ctk.CTkButton(
            self.peer_config_frame,
            text="🔁 Stream to Master",
            width=140,
            fg_color="#E65100",
            hover_color="#BF360C",
            command=self._on_manual_push_stream
        )
        self.manual_push_btn.grid(row=0, column=6, padx=6, pady=8, sticky="w")

        self._update_chunk_options()
        self._update_peer_ui_visibility()

    def _get_num_chunks(self) -> int:
        """Parses the active cluster topology selection into an integer chunk count (2 or 4)."""
        return 2 if "2" in self.cluster_size_var.get() else 4

    def _on_cluster_size_change(self, choice):
        """Reconfigures chunk dropdown options and rebuilds telemetry grid when cluster size changes."""
        self._update_chunk_options()
        self._rebuild_telemetry_grid()
        if self.metadata:
            self._on_inspect_url()

    def _update_chunk_options(self):
        """Populates the worker chunk combo box with percentage intervals matching cluster topology."""
        num_c = self._get_num_chunks()
        if num_c == 2:
            vals = ["Chunk 0 (0-50%)", "Chunk 1 (50-100%)"]
            self.worker_chunk_combo.configure(values=vals)
            self.worker_chunk_combo.set("Chunk 1 (50-100%)")
        else:
            vals = ["Chunk 0 (0-25%)", "Chunk 1 (25-50%)", "Chunk 2 (50-75%)", "Chunk 3 (75-100%)"]
            self.worker_chunk_combo.configure(values=vals)
            self.worker_chunk_combo.set("Chunk 2 (50-75%)")

    def _create_telemetry_card(self):
        """Builds Card 3: Real-time distributed striping telemetry dashboard and speedup indicator."""
        self.telemetry_card = ctk.CTkFrame(self.main_container, corner_radius=12)
        self.telemetry_card.pack(fill="x", pady=(0, 15))

        top_row = ctk.CTkFrame(self.telemetry_card, fg_color="transparent")
        top_row.pack(fill="x", padx=16, pady=(12, 6))

        self.telemetry_title = ctk.CTkLabel(top_row, text="3. Distributed Striping Telemetry", font=ctk.CTkFont(size=15, weight="bold"))
        self.telemetry_title.pack(side="left")

        self.speedup_badge = ctk.CTkLabel(
            top_row,
            text="⚡ Aggregated Speed: 0.0 MB/s | Speedup: 1.0x",
            font=ctk.CTkFont(size=13, weight="bold"),
            text_color="#00E676"
        )
        self.speedup_badge.pack(side="right")

        self.grid_frame = ctk.CTkFrame(self.telemetry_card, fg_color="transparent")
        self.grid_frame.pack(fill="x", padx=16, pady=(6, 12))
        self._rebuild_telemetry_grid()

    def _rebuild_telemetry_grid(self):
        """
        Dynamically constructs the 2x2 grid representing individual cluster nodes.
        Renders progress bars, byte progress counters, live throughput meters, and node status tags.
        """
        for w in self.grid_frame.winfo_children():
            w.destroy()

        num_c = self._get_num_chunks()
        self.grid_frame.columnconfigure((0, 1), weight=1)

        self.node_widgets = []
        if num_c == 2:
            if self.metadata and len(self.metadata.chunks) >= 2 and self.metadata.total_size > 0:
                c0 = self.metadata.chunks[0]
                c1 = self.metadata.chunks[1]
                pct0 = (c0.total_bytes / self.metadata.total_size) * 100.0
                pct1 = (c1.total_bytes / self.metadata.total_size) * 100.0
                node_names = [
                    ("Node 1 (Laptop - Master)", f"Chunk 0 [0% - {pct0:.1f}%] ({format_bytes(c0.total_bytes)})"),
                    ("Node 2 (Lab PC - Worker)", f"Chunk 1 [{pct0:.1f}% - 100%] ({format_bytes(c1.total_bytes)})")
                ]
            else:
                node_names = [
                    ("Node 1 (Laptop - Master)", "Chunk 0 [0% - 50%]"),
                    ("Node 2 (Lab PC - Worker)", "Chunk 1 [50% - 100%]")
                ]
        else:
            if self.metadata and len(self.metadata.chunks) >= 4 and self.metadata.total_size > 0:
                tot = self.metadata.total_size
                chunks = self.metadata.chunks
                node_names = []
                acc = 0.0
                labels = ["PC1 - Master/Wi-Fi", "PC2 - Hotspot Aggregator", "PC3 - Lab PC/Worker", "PC4 - Lab PC/Worker"]
                for i in range(4):
                    pct = (chunks[i].total_bytes / tot) * 100.0
                    node_names.append((f"Node {i+1} ({labels[i]})", f"Chunk {i} [{acc:.1f}% - {acc+pct:.1f}%] ({format_bytes(chunks[i].total_bytes)})"))
                    acc += pct
            else:
                node_names = [
                    ("Node 1 (PC1 - Master/Wi-Fi)", "Chunk 0 [0% - 25%]"),
                    ("Node 2 (PC2 - Hotspot Aggregator)", "Chunk 1 [25% - 50%]"),
                    ("Node 3 (PC3 - Lab PC/Worker)", "Chunk 2 [50% - 75%]"),
                    ("Node 4 (PC4 - Lab PC/Worker)", "Chunk 3 [75% - 100%]")
                ]

        for idx, (title_text, sub_text) in enumerate(node_names):
            row = idx // 2
            col = idx % 2

            card_bg = ctk.CTkFrame(self.grid_frame, fg_color=("#252A34", "#1A1D24"), corner_radius=8)
            card_bg.grid(row=row, column=col, padx=6, pady=6, sticky="nsew")

            card_header = ctk.CTkFrame(card_bg, fg_color="transparent")
            card_header.pack(fill="x", padx=10, pady=(8, 2))

            ctk.CTkLabel(card_header, text=title_text, font=ctk.CTkFont(size=12, weight="bold")).pack(side="left")
            node_status = ctk.CTkLabel(card_header, text="IDLE", font=ctk.CTkFont(size=11, weight="bold"), text_color="gray60")
            node_status.pack(side="right")

            ctk.CTkLabel(card_bg, text=sub_text, font=ctk.CTkFont(size=11), text_color="gray60").pack(anchor="w", padx=10, pady=(0, 4))

            pbar = ctk.CTkProgressBar(card_bg, height=12)
            pbar.set(0.0)
            pbar.pack(fill="x", padx=10, pady=4)

            stats_row = ctk.CTkFrame(card_bg, fg_color="transparent")
            stats_row.pack(fill="x", padx=10, pady=(2, 8))

            bytes_label = ctk.CTkLabel(stats_row, text="0 B / 0 B (0%)", font=ctk.CTkFont(size=11), text_color="gray70")
            bytes_label.pack(side="left")

            speed_label = ctk.CTkLabel(stats_row, text="0.0 KB/s", font=ctk.CTkFont(size=11, weight="bold"), text_color="#64B5F6")
            speed_label.pack(side="right")

            self.node_widgets.append({
                "status": node_status,
                "pbar": pbar,
                "bytes": bytes_label,
                "speed": speed_label
            })

    def _create_log_card(self):
        """Builds Card 4: Scrollable HPC execution log console with monospace formatting."""
        card = ctk.CTkFrame(self.main_container, corner_radius=12)
        card.pack(fill="both", expand=True, pady=(0, 15))

        title_row = ctk.CTkFrame(card, fg_color="transparent")
        title_row.pack(fill="x", padx=16, pady=(12, 6))

        ctk.CTkLabel(title_row, text="4. HPC Engine Execution Log", font=ctk.CTkFont(size=15, weight="bold")).pack(side="left")
        ctk.CTkButton(title_row, text="Clear Log", width=80, height=24, command=self._clear_logs).pack(side="right")

        self.log_textbox = ctk.CTkTextbox(card, height=140, font=ctk.CTkFont(family="Consolas", size=11))
        self.log_textbox.pack(fill="both", expand=True, padx=16, pady=(4, 12))
        self.log_textbox.configure(state="disabled")

    def _create_status_bar(self):
        """Builds bottom dock containing primary lifecycle buttons and status message."""
        status_bar = ctk.CTkFrame(self, height=55, corner_radius=0, fg_color=("#1A1D24", "#12141A"))
        status_bar.pack(fill="x", side="bottom")

        self.start_btn = ctk.CTkButton(
            status_bar,
            text="▶ Start Cooperative Download",
            font=ctk.CTkFont(size=14, weight="bold"),
            fg_color="#00C853",
            hover_color="#00A844",
            height=38,
            width=230,
            command=self._on_start_download
        )
        self.start_btn.pack(side="left", padx=20, pady=8)

        self.cancel_btn = ctk.CTkButton(
            status_bar,
            text="⏹ Cancel",
            font=ctk.CTkFont(size=13),
            fg_color="#D50000",
            hover_color="#AA0000",
            height=38,
            width=100,
            state="disabled",
            command=self._on_cancel_download
        )
        self.cancel_btn.pack(side="left", padx=(0, 10), pady=8)

        self.hash_btn = ctk.CTkButton(
            status_bar,
            text="🔒 Verify SHA-256",
            font=ctk.CTkFont(size=13),
            fg_color="#37474F",
            hover_color="#455A64",
            height=38,
            width=140,
            state="disabled",
            command=self._on_verify_hash
        )
        self.hash_btn.pack(side="left", padx=(0, 10), pady=8)

        self.open_folder_btn = ctk.CTkButton(
            status_bar,
            text="📂 Open File",
            font=ctk.CTkFont(size=13),
            height=38,
            width=120,
            state="disabled",
            command=self._on_open_folder
        )
        self.open_folder_btn.pack(side="left", padx=(0, 10), pady=8)

        self.reset_btn = ctk.CTkButton(
            status_bar,
            text="🔄 New Download",
            font=ctk.CTkFont(size=13, weight="bold"),
            fg_color="#0277BD",
            hover_color="#01579B",
            height=38,
            width=140,
            command=self._on_reset_for_new_download
        )
        self.reset_btn.pack(side="left", padx=(0, 10), pady=8)

        self.footer_status = ctk.CTkLabel(
            status_bar,
            text="Ready. Provide URL to begin.",
            font=ctk.CTkFont(size=12),
            text_color="gray70"
        )
        self.footer_status.pack(side="right", padx=20, pady=8)

    # ========================================================================
    # THREAD-SAFE LOGGING & PERIODIC TELEMETRY DISPATCHERS
    # ========================================================================

    def log(self, message: str):
        """
        Thread-safe logger adding timestamps and placing messages into the queue.
        Can be safely invoked from any background worker thread.
        """
        timestamp = time.strftime("%H:%M:%S")
        self.log_queue.put(f"[{timestamp}] {message}\n")

    def _process_log_queue(self):
        """
        Main-thread consumer draining log messages from the queue into the text console.
        Scheduled recursively via `self.after(100)` to eliminate UI freezing.
        """
        try:
            while not self.log_queue.empty():
                msg = self.log_queue.get_nowait()
                self.log_textbox.configure(state="normal")
                self.log_textbox.insert("end", msg)
                self.log_textbox.see("end")
                self.log_textbox.configure(state="disabled")
        except Exception:
            pass
        self.after(100, self._process_log_queue)

    def _clear_logs(self):
        """Clears all text lines from the HPC console log."""
        self.log_textbox.configure(state="normal")
        self.log_textbox.delete("1.0", "end")
        self.log_textbox.configure(state="disabled")

    def _update_telemetry_loop(self):
        """
        High-Frequency Telemetry Monitoring Loop (runs every 400ms on Main Thread).

        Responsibilities:
        1. Reads live chunk progress and transfer speeds from metadata structures.
        2. Updates progress bars, byte fraction labels, and color-coded status badges.
        3. Computes aggregated cluster bandwidth and speedup multiplier.
        4. Evaluates Cluster Completion Barrier: Automatically detects when all chunks
           have arrived (both local WAN and incoming TCP mesh streams).
        """
        if self.metadata and self.metadata.chunks:
            total_speed = 0.0
            num_c = len(self.metadata.chunks)
            for idx, chunk in enumerate(self.metadata.chunks):
                if idx < len(self.node_widgets):
                    w = self.node_widgets[idx]
                    pct = chunk.progress_pct
                    w["pbar"].set(pct / 100.0)
                    w["bytes"].configure(text=f"{format_bytes(chunk.downloaded_bytes)} / {format_bytes(chunk.total_bytes)} ({pct:.1f}%)")
                    w["speed"].configure(text=format_speed(chunk.speed))
                    w["status"].configure(text=chunk.status)

                    # Dynamic status color coding
                    if chunk.status == "COMPLETED":
                        w["status"].configure(text_color="#00E676")
                    elif chunk.status == "DOWNLOADING":
                        w["status"].configure(text_color="#4FC3F7")
                    elif chunk.status == "STREAMING":
                        w["status"].configure(text_color="#FFD54F")
                    elif chunk.status == "FAILED":
                        w["status"].configure(text_color="#FF5252")
                    else:
                        w["status"].configure(text_color="gray60")

                    total_speed += chunk.speed

            active_count = sum(1 for c in self.metadata.chunks if c.speed > 0)
            self.speedup_badge.configure(text=f"⚡ Aggregated: {format_speed(total_speed)} | Active Streams: {active_count}/{num_c}")

            # Distributed Completion Barrier Check
            if self.is_running and all(c.downloaded_bytes >= c.total_bytes and c.total_bytes > 0 for c in self.metadata.chunks):
                self._on_download_finished()

        self.after(400, self._update_telemetry_loop)

    # ========================================================================
    # CLUSTER ROLE CONFIGURATION & OS NETWORK AUTOMATION
    # ========================================================================

    def _on_browse_dir(self):
        """Opens native OS directory picker to choose target download folder."""
        selected = filedialog.askdirectory(initialdir=self.save_dir_entry.get())
        if selected:
            self.save_dir_entry.delete(0, "end")
            self.save_dir_entry.insert(0, selected)

    def _on_role_change(self, value):
        """Handles user role selection and dynamically updates network configuration UI."""
        self._update_peer_ui_visibility()
        self.log(f"[Topology] Active role switched to: {value}")

    def _update_peer_ui_visibility(self):
        """
        Enforces Role-Based UI State Machine:
        - STANDALONE: Disables peer networking controls (pure local simulation).
        - MASTER (PC1): Enables Hotspot creation button, disables worker IP inputs.
        - AGGREGATOR (PC2): Enables Hotspot creation, acts as staging intermediate node.
        - WORKER (PC3/PC4): Enables Master IP input, chunk selector, and Wi-Fi auto-join.
        """
        role = self.role_var.get()
        if self.worker_beacon_stop:
            self.worker_beacon_stop.set()
            self.worker_beacon_stop = None

        if role == "STANDALONE":
            self.role_tip_label.configure(
                text="💡 Standalone Mode: Simulates parallel WAN download on 1 machine without needing separate PCs."
            )
            self.peer_ip_entry.configure(state="disabled")
            self.worker_chunk_combo.configure(state="disabled")
            self.auto_hotspot_btn.configure(state="disabled")
            self.auto_join_btn.configure(state="disabled")
            self.manual_push_btn.configure(state="disabled")

        elif "Master" in role:
            self.role_tip_label.configure(
                text="👑 Master (PC1): Downloads Chunk 0 over WAN, hosts Hotspot or TCP Server on 192.168.137.1, merges final file."
            )
            self.peer_ip_entry.configure(state="disabled")
            self.worker_chunk_combo.configure(state="disabled")
            self.auto_hotspot_btn.configure(state="normal")
            self.auto_join_btn.configure(state="disabled")
            self.manual_push_btn.configure(state="disabled")

        elif "Aggregator" in role:
            self.role_tip_label.configure(
                text="📡 Aggregator (PC2): Click 'Start Hotspot' to create local mesh. Ingests Worker chunks and serves to Master."
            )
            self.peer_ip_entry.configure(state="disabled")
            self.worker_chunk_combo.configure(state="disabled")
            self.auto_hotspot_btn.configure(state="normal")
            self.auto_join_btn.configure(state="disabled")
            self.manual_push_btn.configure(state="disabled")

        elif "Worker" in role:
            self.role_tip_label.configure(
                text="🛠 Worker Node: Downloads assigned chunk over WAN. Click 'Auto-Join Wi-Fi' or 'Stream to Master' to merge."
            )
            self.peer_ip_entry.configure(state="normal")
            self.worker_chunk_combo.configure(state="normal")
            self.auto_hotspot_btn.configure(state="disabled")
            self.auto_join_btn.configure(state="normal")
            self.manual_push_btn.configure(state="normal")
            # Automatically start UDP worker beacon listener for Zeroconf discovery
            self.worker_beacon_stop = PeerDiscoveryService.start_worker_beacon(on_log=self.log)

    def _on_auto_start_hotspot(self):
        """Executes OS-level Windows Mobile Hotspot activation in a background thread."""
        self.auto_hotspot_btn.configure(state="disabled", text="Starting...")
        def _worker():
            success, msg = NetworkSwitchManager.start_windows_hotspot(on_log=self.log)
            def _done():
                self.auto_hotspot_btn.configure(state="normal", text="⚡ Start Hotspot")
                if success:
                    messagebox.showinfo("Hotspot Active", "Windows Mobile Hotspot is ACTIVE!\n\nDefault Subnet: 192.168.137.1\nSSID: CampusMesh (or Windows Hotspot)")
                else:
                    messagebox.showwarning("Hotspot Notice", f"{msg}\n\nIf needed, turn on Mobile Hotspot in Windows Settings.")
            self.after(0, _done)
        threading.Thread(target=_worker, daemon=True).start()

    def _on_auto_join_wifi(self):
        """Executes automated WPA2 profile registration and Wi-Fi adapter association."""
        self.auto_join_btn.configure(state="disabled", text="Joining...")
        def _worker():
            success, msg = NetworkSwitchManager.connect_to_wifi(DEFAULT_HOTSPOT_SSID, DEFAULT_HOTSPOT_KEY, on_log=self.log)
            def _done():
                self.auto_join_btn.configure(state="normal", text="⚡ Auto-Join Wi-Fi")
                if success:
                    messagebox.showinfo("Wi-Fi Switched", f"Successfully commanded Wi-Fi adapter to join '{DEFAULT_HOTSPOT_SSID}'!")
                else:
                    messagebox.showwarning("Wi-Fi Notice", f"Auto-join response: {msg}\nYou can also connect via Windows Wi-Fi tray.")
            self.after(0, _done)
        threading.Thread(target=_worker, daemon=True).start()

    def _on_inspect_url(self):
        """
        Asynchronously inspects remote file headers over HTTP:
        1. Probes file size via Content-Length or Content-Range.
        2. Validates HTTP/1.1 Range RFC 7233 support.
        3. Initializes 1D domain decomposition chunk structures.
        """
        url = self.url_entry.get().strip()
        if not url:
            messagebox.showwarning("Input Required", "Please enter a valid HTTP/HTTPS file URL.")
            return

        num_c = self._get_num_chunks()
        self.probe_btn.configure(state="disabled", text="Probing...")
        self.file_info_label.configure(text="Connecting to remote server and testing HTTP byte ranges...")
        self.log(f"[Probe] Inspecting {url} with {num_c} chunks...")

        def _worker():
            try:
                meta = fetch_file_metadata(url, num_chunks=num_c)
                self.metadata = meta

                def _update_ui():
                    range_status = "✅ Supported (Parallel Enabled)" if meta.supports_ranges else "❌ Not Supported (Single Stream Only)"
                    self.file_info_label.configure(
                        text=f"Filename: {meta.filename} | Size: {format_bytes(meta.total_size)} | HTTP Range: {range_status}"
                    )
                    self.log(f"[Probe] Metadata resolved: {meta.filename} ({format_bytes(meta.total_size)}). HTTP Range: {meta.supports_ranges}")
                    for c in meta.chunks:
                        self.log(f"   ↳ Chunk {c.chunk_id}: {format_bytes(c.start_byte)} -> {format_bytes(c.end_byte)} ({format_bytes(c.total_bytes)})")

                    save_dir = self.save_dir_entry.get().strip()
                    target_fp = os.path.join(save_dir, meta.filename)
                    self.target_filepath = target_fp

                    # If file already exists and is full size, enable verify & open buttons
                    if os.path.exists(target_fp) and os.path.getsize(target_fp) == meta.total_size:
                        self.hash_btn.configure(state="normal")
                        self.open_folder_btn.configure(state="normal")
                        self.footer_status.configure(text=f"✅ File assembled on disk ({format_bytes(meta.total_size)}).")

                    self.probe_btn.configure(state="normal", text="🔍 Inspect File")

                self.after(0, _update_ui)
            except Exception as e:
                def _error_ui():
                    self.file_info_label.configure(text=f"Probe failed: {e}")
                    self.log(f"[Probe Error] {e}")
                    self.probe_btn.configure(state="normal", text="🔍 Inspect File")
                    messagebox.showerror("Probe Failed", str(e))

                self.after(0, _error_ui)

        threading.Thread(target=_worker, daemon=True).start()

    def _on_auto_distribute_chunks(self):
        """
        Dynamic Heterogeneous Bandwidth Balancing Orchestrator:
        --------------------------------------------------------
        1. Probes local WAN download throughput via micro-probe.
        2. Exchanges capacity metrics across cluster peers via Control Plane (Port 5000).
        3. Solves the makespan minimization equation:
           $$S_i = S_{total} \\times \\frac{B_i^*}{\\sum B_j^*}$$
        4. Reconfigures non-uniform chunk boundaries across all nodes so they finish simultaneously.
        """
        if not self.metadata or self.metadata.total_size <= 0:
            url = self.url_entry.get().strip()
            if not url:
                messagebox.showwarning("URL Required", "Please enter a valid file URL and click 'Inspect File' first.")
                return
            messagebox.showinfo("Inspect Required", "Please click '🔍 Inspect File' first so the remote file size and HTTP ranges are verified.")
            return

        role = self.role_var.get()
        num_c = self._get_num_chunks()

        if role == "STANDALONE":
            self.auto_dist_btn.configure(state="disabled", text="Testing...")
            self.log("[Auto-Balance] Standalone mode: Testing local WAN download throughput...")

            def _standalone_probe():
                speed = measure_wan_bandwidth(self.metadata.url, probe_duration=2.0, on_log=self.log)
                def _done():
                    self.auto_dist_btn.configure(state="normal", text="⚡ Auto-Balance Chunks")
                    self.role_tip_label.configure(
                        text=f"💡 Local WAN Throughput: {format_speed(speed)} | Equal partitioning active across {num_c} streams."
                    )
                    messagebox.showinfo("Bandwidth Probe", f"Measured Local WAN Speed:\n\n{format_speed(speed)}\n\nIn Standalone Mode, all local CPU threads share this WAN pipe equally.")
                self.after(0, _done)
            threading.Thread(target=_standalone_probe, daemon=True).start()

        elif "Master" in role:
            self.auto_dist_btn.configure(state="disabled", text="Balancing...")
            self.log("[Auto-Balance: MASTER] Starting Heterogeneous Auto-Balance workflow...")
            self._master_speed_event.clear()

            # Step 1: Initialize Control Plane Server on TCP port 5000 if not already running
            if not self.control_plane_server:
                def _on_worker_report(req, sock, addr):
                    action = req.get("action")
                    if action == "REPORT_CAPACITY":
                        w_id = req.get("worker_id", 1)
                        w_spd = req.get("measured_speed", 5_000_000.0)
                        self.log(f"[Auto-Balance] Received capacity report from Worker {w_id} ({addr[0]}): {format_speed(w_spd)}")
                        self._worker_speeds[w_id] = w_spd

                        # Wait for Master's own bandwidth probe to finish
                        self._master_speed_event.wait(timeout=3.5)
                        master_spd = self._master_speed if self._master_speed > 0 else 2.5 * 1024 * 1024

                        # Compute optimal makespan chunk partitions
                        all_speeds = {0: master_spd}
                        all_speeds.update(self._worker_speeds)

                        optimal_chunks = compute_optimal_chunks(self.metadata.total_size, all_speeds)
                        self.metadata.chunks = optimal_chunks

                        # Reply to worker with its assigned byte boundaries
                        worker_chunk = next((c for c in optimal_chunks if c.chunk_id == w_id), optimal_chunks[-1])
                        pct_m = (optimal_chunks[0].total_bytes / self.metadata.total_size) * 100.0
                        pct_w = (worker_chunk.total_bytes / self.metadata.total_size) * 100.0

                        send_json(sock, {
                            "status": "OK",
                            "chunk_id": w_id,
                            "start_byte": worker_chunk.start_byte,
                            "end_byte": worker_chunk.end_byte,
                            "total_bytes": worker_chunk.total_bytes,
                            "master_pct": pct_m,
                            "worker_pct": pct_w
                        })
                        sock.close()

                        # Update Master GUI dashboard
                        def _update_master_ui():
                            self._rebuild_telemetry_grid()
                            self.role_tip_label.configure(
                                text=f"⚖ Optimal Split: Laptop {pct_m:.1f}% ({format_bytes(optimal_chunks[0].total_bytes)}) | Lab PC {pct_w:.1f}% ({format_bytes(worker_chunk.total_bytes)})"
                            )
                            self.auto_dist_btn.configure(state="normal", text="⚡ Auto-Balance Chunks")
                            self.log(f"[Auto-Balance] ✅ Optimal distribution computed! Laptop: {pct_m:.1f}% | Lab PC: {pct_w:.1f}%. Ready for download.")
                            messagebox.showinfo("Auto-Balance Complete",
                                                f"Optimal Heterogeneous Split Calculated!\n\n"
                                                f"• Laptop (Master): {pct_m:.1f}% ({format_bytes(optimal_chunks[0].total_bytes)})\n"
                                                f"• Lab PC (Worker): {pct_w:.1f}% ({format_bytes(worker_chunk.total_bytes)})\n\n"
                                                f"Makespan minimized: Both devices will complete download simultaneously!")
                        self.after(0, _update_master_ui)

                self.control_plane_server = ControlPlaneServer(
                    host="0.0.0.0", port=TCP_DISPATCH_PORT,
                    on_worker_reported=_on_worker_report,
                    on_log=self.log
                )
                self.control_plane_server.start()

            # Step 2: Measure Master's own WAN speed via micro-probe
            def _master_probe():
                spd = measure_wan_bandwidth(self.metadata.url, probe_duration=2.0, on_log=self.log)
                self._master_speed = spd
                self._master_speed_event.set()
                self.log(f"[Auto-Balance] Master WAN speed: {format_speed(spd)}. Listening on port {TCP_DISPATCH_PORT} for Worker...")
                def _ready_msg():
                    self.role_tip_label.configure(
                        text=f"👑 Master WAN: {format_speed(spd)}. Click 'Auto-Balance' on Lab PC to negotiate optimal split."
                    )
                self.after(0, _ready_msg)

            threading.Thread(target=_master_probe, daemon=True).start()

        elif "Worker" in role:
            target_ip = self.peer_ip_entry.get().strip()
            if not target_ip:
                messagebox.showerror("Config Error", "Please enter Target Master IP first.")
                return

            self.auto_dist_btn.configure(state="disabled", text="Balancing...")
            self.log(f"[Auto-Balance: WORKER] Probing local WAN speed and negotiating with Master at {target_ip}...")

            def _worker_negotiate():
                spd = measure_wan_bandwidth(self.metadata.url, probe_duration=2.0, on_log=self.log)
                self.log(f"[Auto-Balance] Worker WAN speed: {format_speed(spd)}. Connecting to Master {target_ip}:{TCP_DISPATCH_PORT}...")

                chunk_idx = self.worker_chunk_combo.get()
                try:
                    chunk_id = int(chunk_idx.split()[1])
                except Exception:
                    chunk_id = 1

                resp = ControlPlaneClient.report_capacity_and_get_chunk(
                    target_ip, TCP_DISPATCH_PORT, worker_id=chunk_id, measured_speed=spd, on_log=self.log
                )

                def _apply_worker_result():
                    self.auto_dist_btn.configure(state="normal", text="⚡ Auto-Balance Chunks")
                    if resp and resp.get("status") == "OK":
                        s_byte = resp["start_byte"]
                        e_byte = resp["end_byte"]
                        w_pct = resp.get("worker_pct", 50.0)
                        tot_b = resp.get("total_bytes", 0)

                        if len(self.metadata.chunks) > chunk_id:
                            c = self.metadata.chunks[chunk_id]
                            c.start_byte = s_byte
                            c.end_byte = e_byte
                        if chunk_id == 1 and len(self.metadata.chunks) >= 2:
                            self.metadata.chunks[0].end_byte = s_byte - 1

                        self._rebuild_telemetry_grid()

                        label_val = f"Chunk {chunk_id} ({w_pct:.1f}% - {format_bytes(tot_b)})"
                        self.worker_chunk_combo.configure(values=[label_val])
                        self.worker_chunk_combo.set(label_val)

                        self.role_tip_label.configure(
                            text=f"🛠 Assigned Chunk {chunk_id}: {w_pct:.1f}% ({format_bytes(tot_b)}) | WAN Speed: {format_speed(spd)}"
                        )
                        self.log(f"[Auto-Balance] ✅ Master assigned: Chunk {chunk_id} ({s_byte} -> {e_byte}, {format_bytes(tot_b)}).")
                        messagebox.showinfo("Auto-Balance Complete",
                                            f"Optimal Chunk Configured by Master!\n\n"
                                            f"• Assigned: Chunk {chunk_id} ({w_pct:.1f}%)\n"
                                            f"• Chunk Size: {format_bytes(tot_b)}\n"
                                            f"• Measured WAN: {format_speed(spd)}")
                    else:
                        messagebox.showwarning("Negotiation Notice",
                                               f"Could not negotiate with Master at {target_ip}:{TCP_DISPATCH_PORT}.\n\n"
                                               f"1. Did you click 'Auto-Balance Chunks' on Master first?\n"
                                               f"2. Is Master connected to the same Hotspot ({target_ip})?")

                self.after(0, _apply_worker_result)

            threading.Thread(target=_worker_negotiate, daemon=True).start()

        else:
            messagebox.showinfo("Notice", "Auto-Balance is available for Master and Worker roles.")

    # ========================================================================
    # DOWNLOAD EXECUTION WORKFLOWS (PARALLEL & DISTRIBUTED MODES)
    # ========================================================================

    def _on_start_download(self):
        """
        Primary Download Trigger:
        1. Ensures remote file metadata and range compatibility are resolved.
        2. Disables configuration widgets to maintain state consistency during execution.
        3. Instantiates `ThreadSafeFileWriter` to pre-allocate storage envelope on disk.
        4. Dispatches the appropriate execution routine based on the selected cluster role.
        """
        if not self.metadata:
            self._on_inspect_url()
            if not self.metadata:
                return

        save_dir = self.save_dir_entry.get().strip()
        os.makedirs(save_dir, exist_ok=True)
        self.target_filepath = os.path.join(save_dir, self.metadata.filename)

        self.is_running = True
        self.start_btn.configure(state="disabled")
        self.cancel_btn.configure(state="normal")
        self.hash_btn.configure(state="disabled")
        self.open_folder_btn.configure(state="disabled")
        
        self.cluster_size_menu.configure(state="disabled")
        self.auto_dist_btn.configure(state="disabled")
        self.role_seg.configure(state="disabled")
        self.url_entry.configure(state="disabled")
        self.probe_btn.configure(state="disabled")
        
        self.footer_status.configure(text="Download in progress...")

        with self._downloaders_lock:
            self.active_downloaders.clear()

        role = self.role_var.get()
        self.log(f"[HPC I/O] Preallocating target file: {self.target_filepath} ({format_bytes(self.metadata.total_size)})...")
        self.file_writer = ThreadSafeFileWriter(self.target_filepath, self.metadata.total_size)

        # Mode Dispatcher
        if role == "STANDALONE":
            self._start_standalone_mode()
        elif "Master" in role:
            self._start_master_mode()
        elif "Aggregator" in role:
            self._start_aggregator_mode()
        elif "Worker" in role:
            self._start_worker_mode()

    def _start_standalone_mode(self):
        """
        Mode 1: STANDALONE SIMULATION (Single-Machine Parallel Demo)
        -------------------------------------------------------------
        Spawns N concurrent `WANChunkDownloader` worker threads on the local machine.
        Each thread downloads a separate 1D sub-domain over HTTP/1.1 Range headers
        and commits blocks directly to disk via `ThreadSafeFileWriter.write_at()`.
        Monitors thread completion barrier before transitioning to finished state.
        """
        num_c = len(self.metadata.chunks)
        self.log(f"[Mode: STANDALONE] Launching {num_c} parallel WAN download workers...")

        def _run_worker(chunk: DownloadChunk):
            downloader = WANChunkDownloader(
                url=self.metadata.url,
                chunk=chunk,
                file_writer=self.file_writer,
                on_log=self.log
            )
            with self._downloaders_lock:
                self.active_downloaders.append(downloader)
            downloader.start_download()

        threads = []
        for chunk in self.metadata.chunks:
            t = threading.Thread(target=_run_worker, args=(chunk,), daemon=True)
            threads.append(t)
            t.start()

        def _completion_monitor():
            for t in threads:
                t.join()
            if all(c.status == "COMPLETED" for c in self.metadata.chunks):
                self.after(0, self._on_download_finished)
            else:
                def _fail_ui():
                    self.start_btn.configure(state="normal")
                    messagebox.showerror("Error", "Download failed or incomplete.")
                self.after(0, _fail_ui)

        threading.Thread(target=_completion_monitor, daemon=True).start()

    def _start_master_mode(self):
        """
        Mode 2: MASTER NODE (PC1 — Cluster Coordinator)
        ------------------------------------------------
        1. Starts high-throughput `LocalTCPServer` on port 8888 to ingest incoming peer streams.
        2. Initiates concurrent download of Chunk 0 over university WAN connection.
        3. Upon Chunk 0 completion, evaluates whether worker chunks are still streaming.
           If multi-node cluster is used (e.g. 4 nodes), displays Phase 3 Network Switch Modal
           to guide student to associate with PC2's Hotspot and merge remaining data.
        """
        self.log(f"[Mode: MASTER] Starting local TCP receiver server on port {TCP_STREAM_PORT}...")

        def _check_cluster_done():
            if not self.is_running or not self.metadata:
                return
            if all(c.downloaded_bytes >= c.total_bytes and c.total_bytes > 0 for c in self.metadata.chunks):
                self.after(0, self._on_download_finished)

        def _on_chunk_rcv(chunk_id, rcv, total, spd):
            if chunk_id < len(self.metadata.chunks):
                c = self.metadata.chunks[chunk_id]
                c.downloaded_bytes = rcv
                c.speed = spd
                c.status = "COMPLETED" if rcv >= total else "STREAMING"
                if c.status == "COMPLETED":
                    _check_cluster_done()

        self.tcp_server = LocalTCPServer("0.0.0.0", TCP_STREAM_PORT, self.file_writer, on_chunk_received=_on_chunk_rcv, on_log=self.log)
        self.tcp_server.start()

        chunk0 = self.metadata.chunks[0]
        downloader = WANChunkDownloader(self.metadata.url, chunk0, self.file_writer, on_log=self.log)
        with self._downloaders_lock:
            self.active_downloaders.append(downloader)

        def _master_thread():
            downloader.start_download()
            if chunk0.status != "COMPLETED":
                def _master_fail():
                    if self.is_running:
                        self.start_btn.configure(state="normal")
                        self.cancel_btn.configure(state="disabled")
                        self.is_running = False
                        messagebox.showerror("Error", "Master Chunk 0 download failed.")
                self.after(0, _master_fail)
                return
            self.log("[Mode: MASTER] Chunk 0 completed over WAN. Checking cluster completion...")
            _check_cluster_done()
            if len(self.metadata.chunks) > 2 and chunk0.status == "COMPLETED" and not all(c.status == "COMPLETED" for c in self.metadata.chunks):
                self.after(0, self.prompt_network_switch)

        threading.Thread(target=_master_thread, daemon=True).start()

    def prompt_network_switch(self):
        """
        Phase 3 Interactive Guidance Modal:
        Prompts Master (PC1) student to disconnect from campus Wi-Fi and connect to PC2's
        Hotspot mesh so the remaining aggregated 75% file chunks can be pulled at line-rate speeds.
        """
        dialog = ctk.CTkToplevel(self)
        dialog.title("Phase 3: Network Switch Required")
        dialog.geometry("480x300")
        dialog.attributes("-topmost", True)

        ctk.CTkLabel(
            dialog,
            text="🔄 Phase 3: Switch to Local Mesh Network",
            font=ctk.CTkFont(size=16, weight="bold")
        ).pack(padx=20, pady=(20, 10))

        msg = (
            "1. Disconnect from University Wi-Fi.\n"
            "2. Connect to PC2's Mobile Hotspot.\n"
            "3. Click 'Auto-Switch & Resume' or connect manually."
        )
        ctk.CTkLabel(dialog, text=msg, justify="left", font=ctk.CTkFont(size=13)).pack(padx=20, pady=10)

        btn_row = ctk.CTkFrame(dialog, fg_color="transparent")
        btn_row.pack(pady=15)

        def _auto_switch_and_resume():
            NetworkSwitchManager.connect_to_wifi(DEFAULT_HOTSPOT_SSID, DEFAULT_HOTSPOT_KEY, on_log=self.log)
            dialog.destroy()
            self.log("[Phase 3] Auto Wi-Fi switch initiated. Ingesting aggregated file chunks over local Hotspot...")

        def _manual_resume():
            dialog.destroy()
            self.log("[Phase 3] Manual Wi-Fi switch confirmed. Ingesting aggregated file chunks over local Hotspot...")

        ctk.CTkButton(
            btn_row,
            text="⚡ Auto-Switch & Resume",
            font=ctk.CTkFont(weight="bold"),
            fg_color="#7B1FA2",
            hover_color="#6A1B9A",
            height=36,
            command=_auto_switch_and_resume
        ).pack(side="left", padx=10)

        ctk.CTkButton(
            btn_row,
            text="✔ Resume Merge",
            font=ctk.CTkFont(weight="bold"),
            fg_color="#00C853",
            hover_color="#00A844",
            height=36,
            command=_manual_resume
        ).pack(side="left", padx=10)

    def _start_aggregator_mode(self):
        """
        Mode 3: AGGREGATOR NODE (PC2 — Local Mesh Staging Host)
        --------------------------------------------------------
        1. Starts `LocalTCPServer` on port 8888.
        2. Downloads Chunk 1 concurrently over its independent WAN pipe.
        3. Receives incoming TCP streams from PC3 and PC4 over local Wi-Fi Hotspot (Phase 2),
           writing them directly into the staging file.
        """
        self.log(f"[Mode: AGGREGATOR] Starting Hotspot Edge Server on port {TCP_STREAM_PORT}...")

        def _on_chunk_rcv(chunk_id, rcv, total, spd):
            if chunk_id < len(self.metadata.chunks):
                c = self.metadata.chunks[chunk_id]
                c.downloaded_bytes = rcv
                c.speed = spd
                c.status = "COMPLETED" if rcv >= total else "STREAMING"

        self.tcp_server = LocalTCPServer("0.0.0.0", TCP_STREAM_PORT, self.file_writer, on_chunk_received=_on_chunk_rcv, on_log=self.log)
        self.tcp_server.start()

        chunk1 = self.metadata.chunks[1]
        downloader = WANChunkDownloader(self.metadata.url, chunk1, self.file_writer, on_log=self.log)
        with self._downloaders_lock:
            self.active_downloaders.append(downloader)

        def _agg_thread():
            downloader.start_download()
            self.log("[Mode: AGGREGATOR] Chunk 1 finished. Ingesting Worker streams (Phase 2)...")

        threading.Thread(target=_agg_thread, daemon=True).start()

    def _start_worker_mode(self):
        """
        Mode 4: WORKER NODE (PC2/PC3/PC4 — Contributor)
        ------------------------------------------------
        1. Downloads assigned chunk over campus WAN account.
        2. Upon download completion, establishes TCP connection to Master/Aggregator on port 8888.
        3. Streams disk buffer directly across the local mesh network with exact-byte framing.
        """
        chunk_idx = self.worker_chunk_combo.get()
        try:
            chunk_id = int(chunk_idx.split()[1])
        except Exception:
            chunk_id = 1
        target_ip = self.peer_ip_entry.get().strip()

        if not target_ip:
            messagebox.showerror("Config Error", "Please enter the Target Master IP (shown on Master screen).")
            self.start_btn.configure(state="normal")
            self.is_running = False
            self.cancel_btn.configure(state="disabled")
            if self.file_writer:
                self.file_writer.close()
                self.file_writer = None
            return

        chunk = self.metadata.chunks[chunk_id]
        self.log(f"[Mode: WORKER] Downloading Chunk {chunk_id} over WAN...")

        downloader = WANChunkDownloader(self.metadata.url, chunk, self.file_writer, on_log=self.log)
        with self._downloaders_lock:
            self.active_downloaders.append(downloader)

        def _worker_thread():
            downloader.start_download()
            if chunk.status == "COMPLETED":
                self.log(f"[Mode: WORKER] Chunk {chunk_id} complete. Streaming to Master at {target_ip}:{TCP_STREAM_PORT}...")
                chunk.status = "STREAMING"
                success = LocalTCPClient.stream_chunk_to_peer(
                    target_ip, TCP_STREAM_PORT, chunk, self.file_writer,
                    max_retries=10,
                    retry_delay=1.5,
                    on_progress=lambda cid, sent, tot, spd: setattr(chunk, 'speed', spd),
                    on_log=self.log
                )
                chunk.status = "COMPLETED" if success else "FAILED"
                if success:
                    self.after(0, self._on_download_finished)
                else:
                    def _fail_ui():
                        self.start_btn.configure(state="normal")
                        messagebox.showerror("Error", "Streaming to master failed.")
                    self.after(0, _fail_ui)
            else:
                def _fail_ui():
                    self.start_btn.configure(state="normal")
                    messagebox.showerror("Error", "Chunk download failed.")
                self.after(0, _fail_ui)

        threading.Thread(target=_worker_thread, daemon=True).start()

    def _on_manual_push_stream(self):
        """
        Allows manually pushing or re-transmitting the downloaded chunk to Master/Aggregator
        if automatic transmission was delayed by network switching.
        """
        if not self.metadata or not hasattr(self, 'target_filepath') or not self.target_filepath or not os.path.exists(self.target_filepath):
            messagebox.showwarning("Download Required", "Please click 'Inspect File' and start the download first so the chunk exists on disk.")
            return

        chunk_idx = self.worker_chunk_combo.get()
        try:
            chunk_id = int(chunk_idx.split()[1])
        except Exception:
            chunk_id = 1
        target_ip = self.peer_ip_entry.get().strip()

        if not target_ip:
            messagebox.showerror("Config Error", "Please enter the Target Master IP address.")
            return

        chunk = self.metadata.chunks[chunk_id]
        if chunk.status == "DOWNLOADING":
            messagebox.showwarning("Download In Progress", "Chunk is currently downloading from WAN. Please wait until download completes before streaming to Master.")
            return

        self.log(f"[Manual Stream] Initiating TCP push for Chunk {chunk_id} to {target_ip}:{TCP_STREAM_PORT}...")

        if not self.file_writer:
            self.file_writer = ThreadSafeFileWriter(self.target_filepath, self.metadata.total_size)

        def _push_worker():
            chunk.status = "STREAMING"
            success = LocalTCPClient.stream_chunk_to_peer(
                target_ip, TCP_STREAM_PORT, chunk, self.file_writer,
                max_retries=8,
                retry_delay=1.5,
                on_progress=lambda cid, sent, tot, spd: setattr(chunk, 'speed', spd),
                on_log=self.log
            )
            chunk.status = "COMPLETED" if success else "FAILED"
            if success:
                self.after(0, lambda: messagebox.showinfo("Stream Success", f"Chunk {chunk_id} was successfully streamed and merged into Master!"))
            else:
                self.after(0, lambda: messagebox.showerror("Stream Failed", f"Could not connect to Master at {target_ip}:{TCP_STREAM_PORT}.\n\nCheck:\n1. Is Laptop connected to the same Hotspot?\n2. Did you enter the correct Hotspot IP (e.g. 192.168.137.1)?\n3. Is Windows Firewall unblocked?"))

        threading.Thread(target=_push_worker, daemon=True).start()

    def _on_download_finished(self):
        """
        Cluster Completion Barrier Handler:
        Executed when 100% of all chunk bytes have been successfully assembled on disk.
        Stops control/data socket servers, closes storage file handles, and unlocks verification.
        """
        with self._finished_lock:
            if not self.is_running:
                return
            self.is_running = False

        if self.control_plane_server:
            self.control_plane_server.stop()
            self.control_plane_server = None

        if self.tcp_server:
            self.tcp_server.stop()
            self.tcp_server = None

        if self.file_writer:
            self.file_writer.close()
            self.file_writer = None

        # Re-enable UI configuration controls
        self.start_btn.configure(state="normal")
        self.cancel_btn.configure(state="disabled")
        self.hash_btn.configure(state="normal")
        self.open_folder_btn.configure(state="normal")
        
        self.cluster_size_menu.configure(state="normal")
        self.auto_dist_btn.configure(state="normal", text="⚡ Auto-Balance Chunks")
        self.role_seg.configure(state="normal")
        self.url_entry.configure(state="normal")
        self.probe_btn.configure(state="normal")
        
        role = self.role_var.get()
        if "Worker" in role:
            self.footer_status.configure(text="✅ Chunk streamed to Master. Click '🔄 New Download' to start another.")
            self.log("🎉 [HPC Engine] Chunk download & Master stream complete!")
            messagebox.showinfo("Success", f"Your assigned chunk was downloaded and successfully streamed to Master!")
        else:
            self.footer_status.configure(text="✅ Complete. Click '🔄 New Download' to start another.")
            self.log("🎉 [HPC Engine] Download and Zero-Copy assembly complete!")
            messagebox.showinfo("Success", f"File downloaded and assembled successfully:\n{self.target_filepath}")

    def _on_cancel_download(self):
        """User Cancellation Handler: Immediately signals worker threads and halts listeners."""
        with self._downloaders_lock:
            for d in self.active_downloaders:
                d.cancel()
        if self.control_plane_server:
            self.control_plane_server.stop()
            self.control_plane_server = None
        if self.tcp_server:
            self.tcp_server.stop()
            self.tcp_server = None

        def _cleanup():
            time.sleep(0.3)
            if self.file_writer:
                self.file_writer.close()
                self.file_writer = None

        threading.Thread(target=_cleanup, daemon=True).start()

        self.is_running = False
        self.start_btn.configure(state="normal")
        self.cancel_btn.configure(state="disabled")
        
        self.cluster_size_menu.configure(state="normal")
        self.auto_dist_btn.configure(state="normal", text="⚡ Auto-Balance Chunks")
        self.role_seg.configure(state="normal")
        self.url_entry.configure(state="normal")
        self.probe_btn.configure(state="normal")
        
        self.footer_status.configure(text="Download cancelled.")
        self.log("[Engine] Download cancelled by user.")

    def _on_reset_for_new_download(self):
        """Resets application state, UI inputs, and telemetry grid for a fresh download."""
        if self.is_running:
            if not messagebox.askyesno("Download In Progress", "A download is currently running.\n\nDo you want to cancel it and start a new download?"):
                return
            self._on_cancel_download()

        # Stop lingering servers/threads
        if self.control_plane_server:
            self.control_plane_server.stop()
            self.control_plane_server = None
        self._master_speed = 0.0
        self._worker_speeds.clear()

        if self.tcp_server:
            self.tcp_server.stop()
            self.tcp_server = None

        if self.file_writer:
            self.file_writer.close()
            self.file_writer = None

        with self._downloaders_lock:
            self.active_downloaders.clear()

        # Reset application state
        self.metadata = None
        self.target_filepath = ""
        self.is_running = False

        # Reset URL & File Info
        self.url_entry.delete(0, "end")
        self.file_info_label.configure(
            text="File Status: Awaiting inspection...",
            text_color="gray60"
        )

        # Reset telemetry grid, chunk options & speedup badge
        self._update_chunk_options()
        self._rebuild_telemetry_grid()
        self.speedup_badge.configure(text="⚡ Aggregated Speed: 0.0 MB/s | Speedup: 1.0x")

        # Re-enable configuration controls
        self.cluster_size_menu.configure(state="normal")
        self.auto_dist_btn.configure(state="normal", text="⚡ Auto-Balance Chunks")
        self.role_seg.configure(state="normal")
        self.url_entry.configure(state="normal")
        self.probe_btn.configure(state="normal", text="🔍 Inspect File")
        self._update_peer_ui_visibility()

        # Reset status bar buttons
        self.start_btn.configure(state="normal", text="▶ Start Cooperative Download")
        self.cancel_btn.configure(state="disabled")
        self.hash_btn.configure(state="disabled", text="🔒 Verify SHA-256")
        self.open_folder_btn.configure(state="disabled")
        self.footer_status.configure(text="Ready. Enter a URL to begin a new download.")

        self.log("────────────────────────────────────────────────────────")
        self.log("[Session] Reset complete. Ready for new download.")

    def _on_verify_hash(self):
        """Asynchronously computes SHA-256 checksum to verify assembled file integrity."""
        if not self.target_filepath or not os.path.exists(self.target_filepath):
            return

        self.hash_btn.configure(state="disabled", text="Computing...")
        self.log(f"[Integrity] Computing SHA-256 for {self.target_filepath}...")

        def _hash_worker():
            h = calculate_file_hash(self.target_filepath, "sha256")
            self.log(f"🔑 [SHA-256]: {h}")

            def _done():
                self.hash_btn.configure(state="normal", text="🔒 Verify SHA-256")
                messagebox.showinfo("File Integrity Checksum", f"SHA-256 Hash:\n\n{h}")

            self.after(0, _done)

        threading.Thread(target=_hash_worker, daemon=True).start()

    def _on_open_folder(self):
        """Opens native OS file explorer pointing directly to the assembled download file."""
        if self.target_filepath and os.path.exists(self.target_filepath):
            if sys.platform == "win32":
                subprocess.Popen(['explorer', '/select,', os.path.abspath(self.target_filepath)])
            else:
                subprocess.Popen(['open', os.path.dirname(self.target_filepath)])


# ============================================================================
# APPLICATION ENTRYPOINT
# ============================================================================

if __name__ == "__main__":
    app = EdgeMeshApp()
    app.mainloop()
