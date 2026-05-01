"""
analysis/anomaly_detector.py — Lightweight anomaly detection
Z-score on numeric values + rule-based severity escalation.
Uses fixed-size deque → capped RAM.
"""
from __future__ import annotations
import logging
from collections import deque
from dataclasses import dataclass
from typing import Optional
import numpy as np

log = logging.getLogger(__name__)


@dataclass(slots=True)
class AnomalyResult:
    is_anomaly  : bool
    score       : float   # z-score or 0
    reason      : str
    severity    : str


class AnomalyDetector:
    """One detector instance per source/tag pair."""

    def __init__(self, window: int = 60, z_thresh: float = 3.0):
        self._buf    : deque[float] = deque(maxlen=window)
        self._z      = z_thresh
        self._counts = {"info":0,"warning":0,"error":0,"critical":0}

    # ── numeric check ─────────────────────────────────────────────────────────

    def check_value(self, value: float, severity: str = "info") -> AnomalyResult:
        self._buf.append(value)
        self._counts[severity] = self._counts.get(severity, 0) + 1

        if len(self._buf) < 10:
            return AnomalyResult(False, 0.0, "warming up", severity)

        arr  = np.array(self._buf)
        mean, std = arr.mean(), arr.std()
        z = abs(value - mean) / (std + 1e-9)

        if z > self._z:
            return AnomalyResult(True, round(z,2),
                f"Value {value:.2f} is {z:.1f}σ from mean {mean:.2f}", "error")

        # rule: critical/error spike in last 10 events
        recent = list(self._buf)[-10:]
        if severity in ("critical","error"):
            return AnomalyResult(True, z, f"Severity escalation: {severity}", severity)

        return AnomalyResult(False, round(z,2), "", severity)

    # ── text/event check ──────────────────────────────────────────────────────

    def check_parsed(self, parsed) -> AnomalyResult:
        """Check a ParsedLine. Uses first numeric value if present."""
        sev = parsed.severity
        if parsed.numbers:
            return self.check_value(parsed.numbers[0], sev)
        # no number — escalate on severity alone
        is_anom = sev in ("error", "critical")
        return AnomalyResult(is_anom, 0.0,
            f"Log severity: {sev}" if is_anom else "", sev)

    @property
    def error_rate(self) -> float:
        total = sum(self._counts.values())
        if total == 0: return 0.0
        return (self._counts["error"] + self._counts["critical"]) / total


class DetectorRegistry:
    """Manages one AnomalyDetector per (source_id, tag) pair."""

    def __init__(self, window: int = 60, z_thresh: float = 3.0):
        self._window   = window
        self._z        = z_thresh
        self._registry : dict[str, AnomalyDetector] = {}

    def get(self, source_id: str, tag: str = "default") -> AnomalyDetector:
        key = f"{source_id}:{tag}"
        if key not in self._registry:
            self._registry[key] = AnomalyDetector(self._window, self._z)
        return self._registry[key]

    def check(self, source_id: str, tag: str, value_or_parsed) -> AnomalyResult:
        det = self.get(source_id, tag)
        if isinstance(value_or_parsed, (int, float)):
            return det.check_value(float(value_or_parsed))
        return det.check_parsed(value_or_parsed)

    @property
    def summary(self) -> dict:
        return {k: {"error_rate": v.error_rate} for k, v in self._registry.items()}
