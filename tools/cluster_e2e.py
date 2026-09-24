"""
End-to-end cluster test: runs the real GUI as separate Master and Worker processes on one machine.

The orchestrator serves a random payload from a local RFC 7233 server (each connection capped,
like one campus account), launches one EdgeMeshApp process per node, drives the same buttons a
student would click, and checks that the file the Master assembled matches the payload's SHA-256.

    python tools/cluster_e2e.py --nodes 2 --balance   # Master + 1 Worker, Auto-Balance split
    python tools/cluster_e2e.py --nodes 4             # Master + 3 Workers streaming to Master
    python tools/cluster_e2e.py --nodes 4 --balance   # 3 Workers negotiating one split

Every node runs on 127.0.0.1, so the app's fixed ports (5000, 8888, UDP 5005) must be free.
Needs a display: on headless Linux run it under `xvfb-run -a`.
"""

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from socketserver import ThreadingMixIn

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MIB = 1024 * 1024

# Ensure Windows CP1252 terminal handles Unicode safely
if sys.stdout and hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")


# ============================================================================
# ORCHESTRATOR: THROTTLED RANGE SERVER + NODE PROCESSES
# ============================================================================

def start_server(payload: bytes, rate_bps: float):
    class Handler(BaseHTTPRequestHandler):
        def do_HEAD(self):
            self.send_response(200)
            self.send_header("Content-Length", str(len(payload)))
            self.send_header("Accept-Ranges", "bytes")
            self.end_headers()

        def do_GET(self):
            view = memoryview(payload)
            rng = self.headers.get("Range")
            if rng:
                start, end = rng.split("=")[1].split("-")
                start, end = int(start), min(int(end) if end else len(payload) - 1, len(payload) - 1)
                self.send_response(206)
                self.send_header("Content-Range", f"bytes {start}-{end}/{len(payload)}")
                data = view[start:end + 1]
            else:
                self.send_response(200)
                data = view
            self.send_header("Content-Length", str(len(data)))
            self.send_header("Accept-Ranges", "bytes")
            self.end_headers()
            t0, sent = time.perf_counter(), 0
            for i in range(0, len(data), 64 * 1024):
                block = data[i:i + 64 * 1024]
                sent += len(block)
                delay = t0 + sent / rate_bps - time.perf_counter()
                if delay > 0:
                    time.sleep(delay)
                try:
                    self.wfile.write(block)
                except OSError:
                    return

        def log_message(self, *args):
            pass

    class Server(ThreadingMixIn, HTTPServer):
        daemon_threads = True

    server = Server(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server


def orchestrate(args) -> int:
    payload = os.urandom(args.size_mib * MIB)
    expected = hashlib.sha256(payload).hexdigest()
    server = start_server(payload, args.rate_mib * MIB)
    url = f"http://127.0.0.1:{server.server_port}/e2e_payload.bin"
    work = tempfile.mkdtemp(prefix="cluster_e2e_")
    mode = f"{args.nodes} nodes, {'Auto-Balance' if args.balance else 'equal split'}"
    print(f"[e2e] {mode} | {args.size_mib} MiB payload at {args.rate_mib} MiB/s per connection | sha256 {expected[:16]}...")

    def launch(role, chunk):
        save = os.path.join(work, f"{role}{chunk}")
        os.makedirs(save)
        cmd = [sys.executable, os.path.abspath(__file__), "--node", role, "--chunk", str(chunk),
               "--nodes", str(args.nodes), "--url", url, "--save", save, "--timeout", str(args.timeout)]
        if args.balance:
            cmd.append("--balance")
        env = dict(os.environ, PYTHONIOENCODING="utf-8")
        return subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
                                encoding="utf-8", errors="replace", env=env)

    procs = {"master": launch("master", 0)}
    time.sleep(2.0)  # let the Master bind its ports before Workers start talking to it
    for w in range(1, args.nodes):
        procs[f"worker{w}"] = launch("worker", w)

    results, logs = {}, {}
    for name, proc in procs.items():
        try:
            out, _ = proc.communicate(timeout=args.timeout + 30)
        except subprocess.TimeoutExpired:
            proc.kill()
            out, _ = proc.communicate()
        logs[name] = out
        for line in out.splitlines():
            if line.startswith("RESULT "):
                results[name] = json.loads(line[len("RESULT "):])
    server.shutdown()
    shutil.rmtree(work, ignore_errors=True)

    master = results.get("master", {})
    hash_ok = master.get("sha256") == expected
    all_done = all(results.get(n, {}).get("done") for n in procs)
    ok = hash_ok and all_done
    for name in procs:
        r = results.get(name, {})
        print(f"[e2e] {name:<8} done={r.get('done')} | {r.get('footer', 'no result')}")
    print(f"[e2e] Master file sha256 {str(master.get('sha256'))[:16]}... "
          f"{'matches' if hash_ok else 'DIFFERS from'} payload | all nodes finished: {all_done} "
          f"-> {'PASS' if ok else 'FAIL'}")
    if not ok:
        for name, out in logs.items():
            print(f"\n----- {name} log -----\n{out[-4000:]}")
    return 0 if ok else 1


# ============================================================================
# NODE PROCESS: DRIVES ONE EdgeMeshApp THROUGH THE STUDENT WORKFLOW
# ============================================================================

def run_node(args) -> int:
    sys.path.insert(0, REPO)
    import main
    from tkinter import messagebox

    name = "master" if args.node == "master" else f"worker{args.chunk}"
    for kind in ("showinfo", "showwarning", "showerror"):
        setattr(messagebox, kind, lambda title, msg="", _k=kind, **kw: print(f"[{name}] dialog {_k}: {title}: {msg}", flush=True))

    app = main.EdgeMeshApp()
    original_log = app.log
    app.log = lambda m: (print(f"[{name}] {m}", flush=True), original_log(m))
    app.url_entry.delete(0, "end")
    app.url_entry.insert(0, args.url)
    app.save_dir_entry.delete(0, "end")
    app.save_dir_entry.insert(0, args.save)
    if args.nodes == 4:
        app.cluster_size_var.set("4 Nodes (25% each)")
        app._on_cluster_size_change(None)
    app.role_var.set("Master (PC1)" if args.node == "master" else "Worker (PC2/PC3/PC4)")
    app._update_peer_ui_visibility()  # enables the Worker's IP and chunk fields
    if args.node == "worker":
        app.peer_ip_entry.delete(0, "end")
        app.peer_ip_entry.insert(0, "127.0.0.1")
        app.worker_chunk_combo.set(f"Chunk {args.chunk}")

    deadline = time.time() + args.timeout

    def finish(done: bool):
        fp = app.target_filepath
        digest = None
        if args.node == "master" and fp and os.path.exists(fp):
            with open(fp, "rb") as f:
                digest = hashlib.sha256(f.read()).hexdigest()
        print("RESULT " + json.dumps({"done": done, "footer": app.footer_status.cget("text"), "sha256": digest}), flush=True)
        app._on_close()

    def wait_for(cond, then):
        """Polls `cond` on the Tk loop, then runs `then`; gives up at the deadline."""
        def poll():
            if time.time() > deadline:
                finish(False)
            elif cond():
                then()
            else:
                app.after(200, poll)
        app.after(200, poll)

    def start_and_wait():
        app._on_start_download()
        done_text = "Complete" if args.node == "master" else "streamed"
        wait_for(lambda: done_text in app.footer_status.cget("text"), lambda: finish(True))

    def after_inspect():
        if args.node == "master":
            if args.balance:
                app._on_auto_distribute_chunks()
                wait_for(lambda: app.role_tip_label.cget("text").startswith("⚖ Optimal Split"), start_and_wait)
            else:
                start_and_wait()
        elif args.balance:
            # Give the Master time to open its control port, then report capacity
            def balance():
                app._on_auto_distribute_chunks()
                # The combo label gains a percentage once the Master has assigned this Worker's range
                wait_for(lambda: "%" in app.worker_chunk_combo.get(), lambda: app.after(1500, start_and_wait))
            app.after(3000, balance)
        else:
            app.after(1000, start_and_wait)

    app.after(300, app._on_inspect_url)
    wait_for(lambda: app.metadata is not None, after_inspect)
    app.mainloop()
    return 0


def main_cli() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[1], formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--nodes", type=int, choices=[2, 4], default=2)
    parser.add_argument("--balance", action="store_true", help="negotiate chunk sizes with Auto-Balance first")
    parser.add_argument("--size-mib", type=int, default=16)
    parser.add_argument("--rate-mib", type=float, default=4.0, help="per-connection cap of the test server (MiB/s)")
    parser.add_argument("--timeout", type=float, default=90.0)
    # Internal: set when the orchestrator launches a node process
    parser.add_argument("--node", choices=["master", "worker"], help=argparse.SUPPRESS)
    parser.add_argument("--chunk", type=int, default=0, help=argparse.SUPPRESS)
    parser.add_argument("--url", help=argparse.SUPPRESS)
    parser.add_argument("--save", help=argparse.SUPPRESS)
    args = parser.parse_args()
    return run_node(args) if args.node else orchestrate(args)


if __name__ == "__main__":
    sys.exit(main_cli())
