"""
EdTech Pipeline — Local Control Console Server
================================================
Run this on your MacBook:
    pip install flask
    python console_server.py

Then open http://localhost:5050 in your browser.
generator.py must be in the same folder.
"""

import json
import os
import queue
import re
import subprocess
import sys
import threading
import time
from flask import Flask, Response, jsonify, request, send_from_directory

app = Flask(__name__, static_folder=".")

# ── State ──────────────────────────────────────────────────────────────────────
generator_proc: subprocess.Popen | None = None
proc_lock = threading.Lock()
log_queue: queue.Queue = queue.Queue(maxsize=500)

stats = {
    "sent": 0,
    "failed": 0,
    "rate_actual": 0.0,
    "status": "idle",           # idle | running | stopping
    "mode": "-",
    "started_at": None,
    "events_target_rate": 0,
    # ── Class roster (populated from SIM_STATE / CLASS_STAT log lines) ──
    "active_students": 0,       # students in class or in session
    "offline_users":   0,
    "online_users":    0,       # logged in but not in a class
    "total_classes":   0,
    "classes": {},              # {class_id: {name, category, difficulty, students}}
}
stats_lock = threading.Lock()

# ── Class-state accumulation (assembled across SIM_STATE + CLASS_STAT lines) ──
_cls_buf: dict   = {}    # accumulates CLASS_STAT entries for the current snapshot
_cls_expected: int = 0   # how many CLASS_STAT lines we expect after a SIM_STATE
_cls_received: int = 0   # how many we've seen so far

GENERATOR_PATH = os.path.join(os.path.dirname(__file__), "generator.py")


# ── Helpers ────────────────────────────────────────────────────────────────────
def push_log(msg: str, level: str = "INFO"):
    ts = time.strftime("%H:%M:%S")
    entry = {"ts": ts, "level": level, "msg": msg.rstrip()}
    try:
        log_queue.put_nowait(entry)
    except queue.Full:
        log_queue.get_nowait()
        log_queue.put_nowait(entry)


def parse_generator_line(line: str):
    global _cls_buf, _cls_expected, _cls_received

    # ── sent/failed counter ────────────────────────────────────────────────────
    if "Events sent=" in line:
        try:
            for part in line.split():
                if part.startswith("sent="):
                    with stats_lock:
                        stats["sent"] = int(part.split("=")[1])
                if part.startswith("failed="):
                    with stats_lock:
                        stats["failed"] = int(part.split("=")[1])
        except Exception:
            pass

    # ── SIM_STATE: overall user-state snapshot ─────────────────────────────────
    # Format: "SIM_STATE classes=5 active_students=47 offline=80 online=20"
    if "SIM_STATE" in line:
        try:
            m_cls = re.search(r"classes=(\d+)",         line)
            m_stu = re.search(r"active_students=(\d+)", line)
            m_off = re.search(r"offline=(\d+)",         line)
            m_onl = re.search(r"online=(\d+)",          line)

            n_classes = int(m_cls.group(1)) if m_cls else 0
            _cls_buf      = {}
            _cls_expected = n_classes
            _cls_received = 0

            with stats_lock:
                if m_stu: stats["active_students"] = int(m_stu.group(1))
                if m_off: stats["offline_users"]   = int(m_off.group(1))
                if m_onl: stats["online_users"]    = int(m_onl.group(1))

            # Edge case: no active classes → swap immediately
            if n_classes == 0:
                with stats_lock:
                    stats["classes"]      = {}
                    stats["total_classes"] = 0
        except Exception:
            pass

    # ── CLASS_STAT: one line per active class ──────────────────────────────────
    # Format: "CLASS_STAT id=csg527 students=12 name=Advanced_Algorithms category=CS difficulty=hard"
    elif "CLASS_STAT" in line:
        try:
            m_id   = re.search(r"id=(\S+)",         line)
            m_stu  = re.search(r"students=(\d+)",   line)
            m_nm   = re.search(r"name=(\S+)",        line)
            m_cat  = re.search(r"category=(\S+)",   line)
            m_diff = re.search(r"difficulty=(\S+)", line)

            if m_id and m_stu and m_nm:
                cid = m_id.group(1)
                _cls_buf[cid] = {
                    "name":       m_nm.group(1).replace("_", " "),
                    "category":   m_cat.group(1).replace("_", " ") if m_cat else "",
                    "difficulty": m_diff.group(1) if m_diff else "",
                    "students":   int(m_stu.group(1)),
                }
                _cls_received += 1

                # Atomic swap once we have all expected CLASS_STAT lines
                if _cls_received >= _cls_expected:
                    with stats_lock:
                        stats["classes"]       = dict(_cls_buf)
                        stats["total_classes"] = len(_cls_buf)
        except Exception:
            pass


def stream_output(proc: subprocess.Popen):
    """Read stdout+stderr from subprocess and push to log queue."""
    for line in proc.stdout:
        if not line:
            break
        text  = line.strip()
        level = "ERROR" if "ERROR" in text else "WARN" if "WARNING" in text else "INFO"
        push_log(text, level)
        parse_generator_line(text)

    proc.wait()
    with stats_lock:
        stats["status"]     = "idle"
        stats["started_at"] = None
        stats["classes"]    = {}
        stats["total_classes"]   = 0
        stats["active_students"] = 0
    push_log("Generator process exited.", "INFO")


def build_cmd(cfg: dict) -> list[str]:
    cmd  = [sys.executable, GENERATOR_PATH]
    cmd += ["--mode",   cfg.get("mode", "continuous")]
    cmd += ["--rate",   str(cfg.get("rate", 10))]
    cmd += ["--stream", cfg.get("stream", "edtech-events-stream")]
    cmd += ["--region", cfg.get("region", "ap-south-1")]

    events = cfg.get("events", [])
    if events:
        cmd += ["--events"] + events

    if cfg.get("dry_run"):
        cmd += ["--dry-run"]

    mode = cfg.get("mode", "continuous")
    if mode == "spike":
        cmd += ["--spike-rate",          str(cfg.get("spike_rate", 50))]
        cmd += ["--spike-duration",      str(cfg.get("spike_duration", 60))]
        cmd += ["--pre-spike-duration",  str(cfg.get("pre_spike_duration", 60))]
        cmd += ["--post-spike-duration", str(cfg.get("post_spike_duration", 60))]
    elif mode == "failure":
        cmd += ["--failure-after", str(cfg.get("failure_after", 60))]
        cmd += ["--failure-pause", str(cfg.get("failure_pause", 30))]
    elif mode == "file":
        cmd += ["--count",  str(cfg.get("count", 10000))]
        cmd += ["--output", cfg.get("output", "events.json")]

    return cmd


# ── Routes ─────────────────────────────────────────────────────────────────────
@app.get("/")
def index():
    return send_from_directory(".", "console.html")


@app.post("/start")
def start():
    global generator_proc
    cfg = request.json or {}

    with proc_lock:
        if generator_proc and generator_proc.poll() is None:
            return jsonify({"ok": False, "error": "Generator already running"}), 400

        cmd = build_cmd(cfg)
        push_log("Starting: " + " ".join(cmd), "INFO")

        generator_proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )

        with stats_lock:
            stats.update({
                "sent": 0, "failed": 0,
                "status": "running",
                "mode": cfg.get("mode", "continuous"),
                "started_at": time.time(),
                "events_target_rate": cfg.get("rate", 10),
                "classes": {},
                "total_classes": 0,
                "active_students": 0,
                "offline_users": 0,
                "online_users": 0,
            })

        t = threading.Thread(target=stream_output, args=(generator_proc,), daemon=True)
        t.start()

    return jsonify({"ok": True, "pid": generator_proc.pid})


@app.post("/stop")
def stop():
    global generator_proc
    with proc_lock:
        if not generator_proc or generator_proc.poll() is not None:
            return jsonify({"ok": False, "error": "Not running"}), 400
        generator_proc.terminate()
        with stats_lock:
            stats["status"] = "stopping"
        push_log("Stop signal sent to generator.", "WARN")
    return jsonify({"ok": True})


@app.get("/status")
def status():
    with stats_lock:
        s = dict(stats)
        # Deep-copy classes so the lock isn't held during JSON serialisation
        s["classes"] = dict(s["classes"])
    if s["started_at"]:
        s["uptime"] = round(time.time() - s["started_at"])
    else:
        s["uptime"] = 0
    return jsonify(s)


@app.get("/logs")
def logs():
    """Server-Sent Events stream — one JSON object per line."""
    def generate():
        yield "retry: 1000\n\n"
        while True:
            try:
                entry = log_queue.get(timeout=1.0)
                yield f"data: {json.dumps(entry)}\n\n"
            except queue.Empty:
                yield ": keep-alive\n\n"

    return Response(generate(), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


if __name__ == "__main__":
    print("EdTech Pipeline Console → http://localhost:5050")
    app.run(host="0.0.0.0", port=5050, debug=False, threaded=True)