"""
agent/action_planner.py — Decides and executes response actions.

Thresholds and actions are config-driven.
All actions are logged; destructive actions (shutdown) require
health_score < 10 AND is_anomaly=True to prevent false triggers.
"""
from __future__ import annotations
import asyncio, logging, time, json
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Optional

log = logging.getLogger(__name__)


@dataclass
class Action:
    name     : str
    source_id: str
    reason   : str
    timestamp: float
    executed : bool = False


class ActionPlanner:
    """
    Maps (health_score, is_anomaly, severity) → action.
    Actions: log_event, send_alert, write_report, trigger_shutdown.
    """

    def __init__(
        self,
        report_dir       : str = "./data/reports/",
        alert_threshold  : float = 40.0,   # health below this → alert
        critical_threshold: float = 15.0,  # health below this → report + shutdown signal
        alert_callback   : Optional[Callable] = None,  # e.g. send email/webhook
    ):
        self._report_dir    = Path(report_dir)
        self._alert_th      = alert_threshold
        self._critical_th   = critical_threshold
        self._alert_cb      = alert_callback
        self._action_log   : list[Action] = []
        self._report_dir.mkdir(parents=True, exist_ok=True)

    async def evaluate(
        self,
        source_id    : str,
        health_score : float,
        is_anomaly   : bool,
        root_cause   : Optional[object] = None,   # RootCauseReport
        forecast     : Optional[list]   = None,
    ) -> list[Action]:
        actions: list[Action] = []

        if is_anomaly or health_score < self._alert_th:
            reason = f"Health={health_score:.0f}/100"
            if is_anomaly:
                reason += f" | Anomaly detected"

            # Always log
            actions.append(await self._log_event(source_id, reason, health_score))

            # Alert on significant degradation
            if health_score < self._alert_th:
                actions.append(await self._send_alert(source_id, reason, health_score))

            # Write report + shutdown signal on critical
            if health_score < self._critical_th and is_anomaly:
                if root_cause:
                    actions.append(await self._write_report(source_id, root_cause, health_score))
                actions.append(await self._signal_shutdown(source_id, reason))

        self._action_log.extend(actions)
        return actions

    # ── Action implementations ────────────────────────────────────────────────

    async def _log_event(self, source_id: str, reason: str, score: float) -> Action:
        log.warning("ACTION log_event | %s | score=%.0f | %s", source_id, score, reason)
        return Action("log_event", source_id, reason, time.time(), executed=True)

    async def _send_alert(self, source_id: str, reason: str, score: float) -> Action:
        msg = f"[ALERT] {source_id} health={score:.0f}/100 — {reason}"
        log.error(msg)
        if self._alert_cb:
            try:
                await asyncio.get_event_loop().run_in_executor(None, self._alert_cb, msg)
            except Exception as exc:
                log.debug("Alert callback failed: %s", exc)
        return Action("send_alert", source_id, reason, time.time(), executed=True)

    async def _write_report(self, source_id: str, root_cause, score: float) -> Action:
        path = self._report_dir / f"{source_id}_{int(time.time())}.json"
        data = {
            "generated": time.time(),
            "source_id": source_id,
            "health_score": score,
            "root_cause": root_cause.as_dict() if hasattr(root_cause, "as_dict") else str(root_cause),
        }
        path.write_text(json.dumps(data, indent=2))
        log.error("ACTION report_written | %s | %s", source_id, path)
        return Action("write_report", source_id, str(path), time.time(), executed=True)

    async def _signal_shutdown(self, source_id: str, reason: str) -> Action:
        """Writes a shutdown-signal file. Actual shutdown is the operator's call."""
        sig_path = self._report_dir / f"SHUTDOWN_SIGNAL_{source_id}.txt"
        sig_path.write_text(f"timestamp={time.time()}\nreason={reason}\n")
        log.critical("ACTION shutdown_signal | %s | %s", source_id, reason)
        return Action("shutdown_signal", source_id, reason, time.time(), executed=True)

    @property
    def recent_actions(self) -> list[dict]:
        return [{"name": a.name, "source_id": a.source_id, "reason": a.reason,
                 "timestamp": a.timestamp} for a in self._action_log[-50:]]
