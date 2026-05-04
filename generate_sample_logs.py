"""generate_sample_logs.py
Generates ./data/sample.log with realistic mixed-severity log entries.
Safe to re-run — appends if file exists, creates if not.
Called automatically by main.py when sample.log is missing.
"""
import random, time
from pathlib import Path
from datetime import datetime, timedelta

MESSAGES = {
    "info":     ["Service started", "Connection established", "Heartbeat OK",
                 "Config reloaded", "Backup completed", "Sensor online",
                 "Calibration success", "Queue flushed"],
    "warning":  ["High temperature detected: 87°C", "Response timeout on pump_01",
                 "Retry attempt 2/3", "Slow query: 1.4s", "Buffer 85% full",
                 "High vibration: 62 Hz", "Memory usage at 78%"],
    "error":    ["Connection failed to modbus device", "Sensor read error on CH3",
                 "Queue overflow — dropping oldest", "Write failed: disk full",
                 "Authentication error", "Unexpected sensor value: -999"],
    "critical": ["Motor overheat CRITICAL: 112°C", "FATAL: watchdog timeout",
                 "kernel panic — not syncing", "EMERG: pressure relief valve open",
                 "OOM killer invoked on process 1234"],
}
SOURCES = ["pump_01", "compressor_01", "conveyor_01", "app_log", "watchdog", "modbus_gw"]

def generate(n_lines=500, out_path="./data/sample.log"):
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    now = datetime.now()
    lines = []
    for i in range(n_lines):
        ts = now - timedelta(seconds=(n_lines - i) * 6)
        r = random.random()
        if r < 0.60:   sev = "INFO"
        elif r < 0.82: sev = "WARNING"
        elif r < 0.96: sev = "ERROR"
        else:          sev = "CRITICAL"
        src = random.choice(SOURCES)
        msg = random.choice(MESSAGES[sev.lower()])
        lines.append(f"{ts.strftime('%Y-%m-%d %H:%M:%S')} [{sev}] {src}: {msg}")

    mode = "a" if Path(out_path).exists() else "w"
    with open(out_path, mode) as f:
        f.write("\n".join(lines) + "\n")
    print(f"[generate_sample_logs] wrote {n_lines} lines → {out_path}")

if __name__ == "__main__":
    generate()
