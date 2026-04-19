"""
EdTech Pipeline — Local Control Console Server
===============================================
Run locally:
    pip install flask kafka-python
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
proc_lock  = threading.Lock()
log_queue: queue.Queue = queue.Queue(maxsize=500)

stats = {
    "sent":               0,
    "failed":             0,
    "status":             "idle",    # idle | running | stopping
    "mode":               "-",
    "started_at":         None,
    "events_target_rate": 0,
    "users":              0,
    # ── Simulation roster ────────────────────────────────────────────────────
    "active_students":    0,
    "offline_users":      0,
    "online_users":       0,
    "total_classes":      0,
    "classes":            {},
    # ── Intelligence ─────────────────────────────────────────────────────────
    "surge_detected":     False,
    # ── Rate tracking ────────────────────────────────────────────────────────
    "_last_sent":         0,
    "_last_ts":           0.0,
    "rate_actual":        0.0,
}
stats_lock = threading.Lock()

_cls_buf:      dict = {}
_cls_expected: int  = 0
_cls_received: int  = 0

GENERATOR_PATH = os.path.join(os.path.dirname(__file__), "generator.py")


# ── Helpers ────────────────────────────────────────────────────────────────────
def push_log(msg: str, level: str = "INFO"):
    ts    = time.strftime("%H:%M:%S")
    entry = {"ts": ts, "level": level, "msg": msg.rstrip()}
    try:
        log_queue.put_nowait(entry)
    except queue.Full:
        log_queue.get_nowait()
        log_queue.put_nowait(entry)


def parse_generator_line(line: str):
    global _cls_buf, _cls_expected, _cls_received

    # ── Surge detection ────────────────────────────────────────────────────────
    if "SURGE" in line and "detected" in line.lower():
        with stats_lock:
            stats["surge_detected"] = True
    elif "surge=False" in line or ("NORMAL" in line and "restored" in line.lower()):
        with stats_lock:
            stats["surge_detected"] = False

    # ── Phase detection ────────────────────────────────────────────────────────
    # These are parsed by the browser JS directly from the log stream.
    # No server-side action needed beyond forwarding to the log queue.

    # ── Event counters ─────────────────────────────────────────────────────────
    if "Events sent=" in line:
        try:
            now_ts = time.time()
            new_sent = None
            for part in line.split():
                if part.startswith("sent="):
                    new_sent = int(part.split("=")[1])
                if part.startswith("failed="):
                    with stats_lock:
                        stats["failed"] = int(part.split("=")[1])

            if new_sent is not None:
                with stats_lock:
                    prev_sent = stats["sent"]
                    prev_ts   = stats["_last_ts"] or now_ts
                    dt = now_ts - prev_ts
                    if dt > 0 and new_sent > prev_sent:
                        stats["rate_actual"] = round((new_sent - prev_sent) / dt, 1)
                    stats["sent"]       = new_sent
                    stats["_last_sent"] = new_sent
                    stats["_last_ts"]   = now_ts
        except Exception:
            pass

    # ── SIM_STATE (one line per tick with aggregate counts) ────────────────────
    if "SIM_STATE" in line:
        try:
            m_cls = re.search(r"classes=(\d+)",         line)
            m_stu = re.search(r"active_students=(\d+)", line)
            m_off = re.search(r"offline=(\d+)",          line)
            m_onl = re.search(r"online=(\d+)",           line)

            n_classes     = int(m_cls.group(1)) if m_cls else 0
            _cls_buf      = {}
            _cls_expected = n_classes
            _cls_received = 0

            with stats_lock:
                if m_stu: stats["active_students"] = int(m_stu.group(1))
                if m_off: stats["offline_users"]   = int(m_off.group(1))
                if m_onl: stats["online_users"]    = int(m_onl.group(1))

            if n_classes == 0:
                with stats_lock:
                    stats["classes"]       = {}
                    stats["total_classes"] = 0
        except Exception:
            pass

    # ── CLASS_STAT (one line per live class, follows SIM_STATE) ────────────────
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

                if _cls_received >= _cls_expected:
                    with stats_lock:
                        stats["classes"]       = dict(_cls_buf)
                        stats["total_classes"] = len(_cls_buf)
        except Exception:
            pass


def stream_output(proc: subprocess.Popen):
    for line in proc.stdout:
        if not line:
            break
        text  = line.strip()
        level = "ERROR" if "ERROR" in text else "WARN" if "WARNING" in text else "INFO"
        push_log(text, level)
        parse_generator_line(text)

    proc.wait()
    with stats_lock:
        stats["status"]          = "idle"
        stats["started_at"]      = None
        stats["classes"]         = {}
        stats["total_classes"]   = 0
        stats["active_students"] = 0
        stats["rate_actual"]     = 0.0
    push_log("Generator process exited.", "INFO")


def build_cmd(cfg: dict) -> list[str]:
    cmd  = [sys.executable, GENERATOR_PATH]
    cmd += ["--mode",  cfg.get("mode",  "continuous")]
    cmd += ["--rate",  str(cfg.get("rate",  10))]
    cmd += ["--users", str(cfg.get("users", 200))]

    # Default is localhost (generator runs on EC2-1 itself).
    # Override via the UI or set bootstrap_servers in the request body.
    cmd += ["--bootstrap-servers", cfg.get("bootstrap_servers", "localhost:9092")]
    cmd += ["--topic",             cfg.get("topic", "edtech-events")]

    sec = cfg.get("security_protocol", "PLAINTEXT")
    if sec != "PLAINTEXT":
        cmd += ["--security-protocol", sec]
    if cfg.get("sasl_mechanism"):
        cmd += ["--sasl-mechanism", cfg["sasl_mechanism"]]
        cmd += ["--sasl-username",  cfg.get("sasl_username", "")]
        cmd += ["--sasl-password",  cfg.get("sasl_password", "")]

    events = cfg.get("events", [])
    if events:
        cmd += ["--events"] + events

    if cfg.get("dry_run"):
        cmd += ["--dry-run"]

    mode = cfg.get("mode", "continuous")
    if mode == "spike":
        cmd += ["--spike-rate",          str(cfg.get("spike_rate",          100))]
        cmd += ["--spike-duration",      str(cfg.get("spike_duration",       60))]
        cmd += ["--pre-spike-duration",  str(cfg.get("pre_spike_duration",   60))]
        cmd += ["--post-spike-duration", str(cfg.get("post_spike_duration",  60))]
    elif mode == "failure":
        cmd += ["--failure-after", str(cfg.get("failure_after", 60))]
        cmd += ["--failure-pause", str(cfg.get("failure_pause", 30))]
    elif mode == "file":
        cmd += ["--count",  str(cfg.get("count",  10000))]
        cmd += ["--output", cfg.get("output", "events.json")]

    if cfg.get("regions"):
        cmd += ["--regions", cfg["regions"]]
    if cfg.get("courses"):
        cmd += ["--courses", cfg["courses"]]
    if cfg.get("difficulty") and cfg["difficulty"] != "all":
        cmd += ["--difficulty", cfg["difficulty"]]
    if cfg.get("subscription_split") is not None:
        cmd += ["--subscription-split", str(cfg["subscription_split"])]
    if cfg.get("device_weights"):
        cmd += ["--device-weights", cfg["device_weights"]]
    if cfg.get("max_classes"):
        cmd += ["--max-classes", str(cfg["max_classes"])]

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
                "status":             "running",
                "mode":               cfg.get("mode", "continuous"),
                "started_at":         time.time(),
                "events_target_rate": cfg.get("rate", 10),
                "users":              cfg.get("users", 200),
                "classes":            {},
                "total_classes":      0,
                "active_students":    0,
                "offline_users":      0,
                "online_users":       0,
                "surge_detected":     False,
                "rate_actual":        0.0,
                "_last_sent":         0,
                "_last_ts":           time.time(),
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
        s["classes"] = dict(s["classes"])
    # Strip internal keys
    s.pop("_last_sent", None)
    s.pop("_last_ts",   None)
    s["uptime"] = round(time.time() - s["started_at"]) if s["started_at"] else 0
    return jsonify(s)


@app.get("/logs")
def logs():
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
