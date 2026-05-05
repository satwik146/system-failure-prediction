"""
notifier.py — Windows Toast Notification module for IoT Failure Predictor
Place this file in the project root alongside main.py.

Dependencies (install once):
    pip install winotify

Usage:
    from notifier import AnomalyNotifier
    notifier = AnomalyNotifier()
    notifier.send(source_id="Machine-1", severity="CRITICAL", message="CPU spike detected")
"""

from __future__ import annotations

import logging
import os
import sys
import threading
import time
from collections import defaultdict
from datetime import datetime
from typing import Optional

log = logging.getLogger("iot.notifier")

# ──────────────────────────────────────────────
# Platform guard
# ──────────────────────────────────────────────
if os.name != "nt":
    log.warning(
        "AnomalyNotifier: Windows toast notifications are only supported on Windows. "
        "On this platform, anomalies will be logged to console instead."
    )


# ──────────────────────────────────────────────
# Severity → icon mapping
# ──────────────────────────────────────────────
SEVERITY_ICON = {
    "CRITICAL": "⛔",
    "HIGH":     "🔴",
    "MEDIUM":   "🟡",
    "LOW":      "🟢",
    "INFO":     "ℹ️",
}

SEVERITY_AUDIO = {
    "CRITICAL": "ms-winsoundevent:Notification.Looping.Alarm",
    "HIGH":     "ms-winsoundevent:Notification.Looping.Alarm2",
    "MEDIUM":   "ms-winsoundevent:Notification.Default",
    "LOW":      "ms-winsoundevent:Notification.Default",
    "INFO":     "ms-winsoundevent:Notification.Default",
}


class AnomalyNotifier:
    """
    Sends Windows Action Center (toast) notifications when an anomaly is detected.

    Parameters
    ----------
    app_id : str
        The application identifier shown in Action Center.
    cooldown_seconds : int
        Minimum seconds between notifications for the *same* source/severity
        combination. Prevents notification storms.
    min_severity : str
        Minimum severity level to notify. One of INFO < LOW < MEDIUM < HIGH < CRITICAL.
    """

    SEVERITY_ORDER = ["INFO", "LOW", "MEDIUM", "HIGH", "CRITICAL"]

    def __init__(
        self,
        app_id: str = "IoT Failure Predictor",
        cooldown_seconds: int = 30,
        min_severity: str = "MEDIUM",
    ):
        self.app_id = app_id
        self.cooldown = cooldown_seconds
        self.min_severity_index = self._severity_index(min_severity)
        self._last_sent: dict[str, float] = defaultdict(float)
        self._lock = threading.Lock()
        self._winotify_available = self._check_winotify()

    # ── public API ──────────────────────────────

    def send(
        self,
        source_id: str,
        severity: str = "HIGH",
        message: str = "Anomaly detected",
        predicted_value: Optional[float] = None,
        threshold: Optional[float] = None,
    ) -> bool:
        """
        Send a Windows toast notification for an anomaly.

        Returns True if a notification was actually sent, False if suppressed
        (cooldown, below min_severity, or non-Windows).
        """
        severity = severity.upper()

        # Severity filter
        if self._severity_index(severity) < self.min_severity_index:
            log.debug("Notification suppressed (below min_severity): %s / %s", source_id, severity)
            return False

        # Cooldown filter (per source+severity key)
        key = f"{source_id}::{severity}"
        now = time.monotonic()
        with self._lock:
            if now - self._last_sent[key] < self.cooldown:
                log.debug("Notification suppressed (cooldown): %s", key)
                return False
            self._last_sent[key] = now

        title, body = self._build_content(source_id, severity, message, predicted_value, threshold)

        if os.name == "nt" and self._winotify_available:
            self._send_toast(title, body, severity)
        else:
            # Fallback: pretty console log
            self._console_fallback(title, body, severity)

        return True

    def reset_cooldown(self, source_id: Optional[str] = None) -> None:
        """Reset cooldown timers (all sources, or a specific one)."""
        with self._lock:
            if source_id is None:
                self._last_sent.clear()
            else:
                keys = [k for k in self._last_sent if k.startswith(f"{source_id}::")]
                for k in keys:
                    del self._last_sent[k]

    # ── internal helpers ─────────────────────────

    @staticmethod
    def _check_winotify() -> bool:
        try:
            import winotify  # noqa: F401
            return True
        except ImportError:
            if os.name == "nt":
                log.warning(
                    "winotify not installed. Run: pip install winotify\n"
                    "Falling back to console output for anomaly alerts."
                )
            return False

    def _send_toast(self, title: str, body: str, severity: str) -> None:
        """Fire a Windows 10/11 Action Center toast notification."""
        try:
            from winotify import Notification, audio

            toast = Notification(
                app_id=self.app_id,
                title=title,
                msg=body,
                duration="long" if severity in ("CRITICAL", "HIGH") else "short",
                icon=self._icon_path(),
            )

            # Attach sound based on severity
            sound_uri = SEVERITY_AUDIO.get(severity, SEVERITY_AUDIO["INFO"])
            try:
                toast.set_audio(audio.LoopingAlarm if severity == "CRITICAL" else audio.Default, loop=False)
            except Exception:
                pass  # audio is optional

            toast.show()
            log.info("🔔 Toast notification sent: [%s] %s", severity, title)

        except Exception as exc:
            log.error("Failed to send toast notification: %s", exc)
            self._console_fallback(title, body, severity)

    @staticmethod
    def _build_content(
        source_id: str,
        severity: str,
        message: str,
        predicted_value: Optional[float],
        threshold: Optional[float],
    ) -> tuple[str, str]:
        icon = SEVERITY_ICON.get(severity, "⚠️")
        timestamp = datetime.now().strftime("%H:%M:%S")
        title = f"{icon} [{severity}] Anomaly — {source_id}"

        lines = [message, f"Time: {timestamp}"]
        if predicted_value is not None:
            lines.append(f"Predicted value: {predicted_value:.4f}")
        if threshold is not None:
            lines.append(f"Threshold: {threshold:.4f}")

        return title, "\n".join(lines)

    @staticmethod
    def _console_fallback(title: str, body: str, severity: str) -> None:
        border = "=" * 60
        print(f"\n{border}", file=sys.stderr)
        print(f"  {title}", file=sys.stderr)
        print(border, file=sys.stderr)
        for line in body.splitlines():
            print(f"  {line}", file=sys.stderr)
        print(f"{border}\n", file=sys.stderr)

    @staticmethod
    def _icon_path() -> str:
        """Return path to a notification icon if one exists next to main.py."""
        candidates = ["icon.ico", "icon.png", "assets/icon.ico"]
        for c in candidates:
            if os.path.isfile(c):
                return os.path.abspath(c)
        return ""

    def _severity_index(self, severity: str) -> int:
        try:
            return self.SEVERITY_ORDER.index(severity.upper())
        except ValueError:
            return 0
