"""generate_sample_logs.py
Generates ./data/sample.log with realistic mixed-severity log entries.
Safe to re-run — appends if file exists, creates if not.
Called automatically by main.py when sample.log is missing.
"""
import random, time
from pathlib import Path
from datetime import datetime, timedelta

def _gen_numeric_value(metric_type):
    """Generate realistic numeric values for different metric types."""
    ranges = {
        "temperature_c": (20, 110),
        "motor_rpm": (800, 2000),
        "vibration_hz": (20, 80),
        "pressure_bar": (3, 10),
        "current_a": (5, 20),
        "speed_ms": (0.5, 3),
        "belt_temp_c": (30, 80),
        "ram_used_mb": (100, 8000),
        "cpu_wait_pct": (0, 80),
    }
    if metric_type in ranges:
        low, high = ranges[metric_type]
        return round(random.uniform(low, high), 1)
    return round(random.uniform(50, 100), 1)

MESSAGES = {
    "info":     [
        "Service started successfully",
        "Connection established to device",
        "Heartbeat OK - all metrics nominal",
        "Config reloaded successfully",
        "Backup completed - {value} items archived",
        "Sensor online: {metric}={value}",
        "Calibration success - offset {value}°C",
        "Queue flushed - {value} items processed",
    ],
    "warning":  [
        "High temperature detected: {value}°C",
        "Response timeout on pump_01 - {value}ms",
        "Retry attempt {value}/3",
        "Slow query: {value}s",
        "Buffer {value}% full",
        "High vibration: {value} Hz",
        "Memory usage at {value}%",
        "Pressure rising: {value} bar",
    ],
    "error":    [
        "Connection failed to modbus device - timeout {value}s",
        "Sensor read error on CH{value}",
        "Queue overflow — dropping oldest {value} items",
        "Write failed: disk {value}% full",
        "Authentication error - attempt {value}",
        "Unexpected sensor value: {value}",
        "High timeout_rate on gateway: {value}%",
        "packet_loss_pct above {value}%",
    ],
    "critical": [
        "Motor overheat CRITICAL: {value}°C",
        "FATAL: watchdog timeout after {value}s",
        "kernel panic — not syncing: voltage {value}V",
        "EMERG: pressure relief valve {value} bar",
        "OOM killer invoked - {value}MB freed",
        "POWER SPIKE: current_draw_amps > {value}A",
    ],
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
        msg_template = random.choice(MESSAGES[sev.lower()])
        
        # Fill in placeholders with realistic values
        value = _gen_numeric_value("temperature_c")
        metric_types = ["temperature_c", "motor_rpm", "vibration_hz", "pressure_bar"]
        metric = random.choice(metric_types)
        msg = msg_template.format(value=value, metric=metric)
        
        lines.append(f"{ts.strftime('%Y-%m-%d %H:%M:%S')} [{sev}] {src}: {msg}")

    mode = "a" if Path(out_path).exists() else "w"
    with open(out_path, mode, encoding='utf-8') as f:
        f.write("\n".join(lines) + "\n")
    print(f"[generate_sample_logs] wrote {n_lines} lines -> {out_path}")

if __name__ == "__main__":
    generate()

