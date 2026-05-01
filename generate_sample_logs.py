"""
tools/generate_sample_logs.py — Test data generator

Writes realistic IoT log lines to ./data/sample.log so you can test
the ingestion layer without any physical hardware connected.

Usage:
    python tools/generate_sample_logs.py          # default: 1 line/sec
    python tools/generate_sample_logs.py --rate 5  # 5 lines/sec
"""

from __future__ import annotations

import argparse
import random
import time
from datetime import datetime
from pathlib import Path

MACHINES = ["pump_01", "motor_02", "compressor_03", "conveyor_04", "valve_05"]
LEVELS   = ["INFO"] * 10 + ["WARNING"] * 4 + ["ERROR"] * 2 + ["CRITICAL"] * 1

MESSAGES = {
    "INFO":     [
        "System heartbeat OK",
        "Temperature within normal range: {val:.1f}°C",
        "RPM stable at {val:.0f}",
        "Pressure normal: {val:.2f} bar",
        "Sensor read cycle complete",
        "Connection alive",
    ],
    "WARNING":  [
        "Temperature elevated: {val:.1f}°C — threshold 75°C",
        "Vibration spike detected: {val:.1f} Hz",
        "Response latency high: {val:.0f}ms",
        "Retry attempt {val:.0f} for register 0x0002",
        "Buffer utilisation at {val:.0f}%",
    ],
    "ERROR":    [
        "Sensor read FAILED — register 0x0003 timeout",
        "Motor stall detected — RPM dropped to {val:.0f}",
        "Communication error: CRC mismatch",
        "Over-temperature limit exceeded: {val:.1f}°C",
    ],
    "CRITICAL": [
        "EMERGENCY STOP triggered — safety interlock activated",
        "Catastrophic vibration: {val:.1f} Hz — shutting down",
        "Power supply failure detected",
    ],
}


def gen_line(machine: str, level: str) -> str:
    ts    = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
    tmpl  = random.choice(MESSAGES[level])
    val   = random.uniform(10, 120)
    msg   = tmpl.format(val=val)
    return f"{ts} [{level:8s}] {machine}: {msg}"


def main(rate: float, output: str) -> None:
    path = Path(output)
    path.parent.mkdir(parents=True, exist_ok=True)
    interval = 1.0 / rate
    print(f"Writing to {path} at {rate} lines/sec. Ctrl+C to stop.")
    with open(path, "a") as f:
        while True:
            machine = random.choice(MACHINES)
            level   = random.choice(LEVELS)
            line    = gen_line(machine, level)
            f.write(line + "\n")
            f.flush()
            print(line)
            time.sleep(interval)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--rate",   type=float, default=1.0,               help="Lines per second")
    parser.add_argument("--output", type=str,   default="./data/sample.log", help="Output log file")
    args = parser.parse_args()
    main(args.rate, args.output)
