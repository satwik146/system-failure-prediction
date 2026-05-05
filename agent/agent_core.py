"""
agent/agent_core.py — AI Agent Orchestrator

Connects the ingestion EventBus to the LSTM predictor and integrity checker.
Runs as a single asyncio task — no threads, minimal CPU.

Flow:
  EventBus.consume()
    → FeatureEngineer.transform_single()
    → LSTMPredictor.ingest() + predict()
    → IntegrityChecker.update()
    → publish PredictionResult to prediction_queue
"""

from __future__ import annotations

import asyncio
import logging
import time
from pathlib import Path

from ingestion.event_bus import EventBus, Event, SourceType
from agent.failure_predictor import LSTMPredictor, PredictionResult
from analysis.feature_engineer import FeatureEngineer

log = logging.getLogger(__name__)


class AgentCore:
    """
    Reads events from the ingestion bus, extracts features,
    runs LSTM prediction, and scores system integrity.

    Prediction results are placed on `prediction_queue` for
    the dashboard API and action planner to consume.
    """

    def __init__(
        self,
        model_dir        : str   = "./agent/models/",
        predict_every_n  : int   = 1,      # run prediction on every event per source (for responsive forecasts)
        min_health_alert : float = 40.0,   # health score below this → WARNING log
    ):
        self.model_dir        = Path(model_dir)
        self.predict_every_n  = predict_every_n
        self.min_health_alert = min_health_alert

        self.prediction_queue: asyncio.Queue[PredictionResult] = asyncio.Queue(maxsize=256)
        self._event_count: dict[str, int] = {}

        # Lazy-loaded components
        self._predictor : LSTMPredictor   = None
        self._feature_eng: FeatureEngineer = None
        self._latest: dict[str, PredictionResult] = {}   # source_id → last result

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def start(self, bus: EventBus) -> None:
        """Boot components and begin consuming the event bus."""
        self._load_components()
        log.info("AgentCore started. Consuming event bus…")
        await self._consume_loop(bus)

    def _load_components(self) -> None:
        """Load feature engineer and LSTM model from disk."""
        fe_path = self.model_dir / "feature_engineer.pkl"
        if fe_path.exists():
            try:
                self._feature_eng = FeatureEngineer.load(str(fe_path))
                log.info("FeatureEngineer loaded from %s", fe_path)
            except Exception as exc:
                log.warning("Could not load FeatureEngineer: %s — using fresh instance", exc)
                self._feature_eng = FeatureEngineer()
        else:
            log.warning("No fitted FeatureEngineer found. Run agent/train.py first.")
            self._feature_eng = FeatureEngineer()

        self._predictor = LSTMPredictor(
            model_yaml_path = str(self.model_dir / "model1.yaml"),
            model_h5_path   = str(self.model_dir / "model1.h5"),
        )
        loaded = self._predictor.load_model()
        if not loaded:
            log.warning("LSTM model not found — running feature-only mode.")

    # ── Main loop ─────────────────────────────────────────────────────────────

    async def _consume_loop(self, bus: EventBus) -> None:
        """Read events forever, process, optionally predict."""
        log.info("🔄 AgentCore _consume_loop started! Waiting for events from bus...")
        event_count = 0
        while True:
            try:
                event: Event = await bus.consume()
                event_count += 1
                await self._process_event(event)
                bus.task_done()
                
                # Log progress every 10 events
                if event_count % 10 == 0:
                    log.info(f"✓ Processed {event_count} events. Latest predictions: {len(self._latest)}")
            except asyncio.CancelledError:
                log.info("AgentCore cancelled.")
                return
            except Exception as exc:
                log.error("AgentCore event error: %s", exc)

    async def _process_event(self, event: Event) -> None:
        """Handle one event: extract scalar, ingest into predictor, maybe predict."""
        scalar = self._extract_scalar(event)
        if scalar is None:
            return

        source_id = event.source_id
        tag = event.tag or "default"
        stream_id = f"{source_id}::{tag}"

        self._predictor.ingest(stream_id, scalar)

        count = self._event_count.get(stream_id, 0) + 1
        self._event_count[stream_id] = count

        # Only predict every N events to save CPU
        if count % self.predict_every_n != 0:
            return

        # Buffer must be full before prediction is meaningful
        fill = self._predictor.buffer_fill(stream_id)
        if fill < 1.0:
            if count % 10 == 0:  # Log every 10th event
                log.debug("%s buffer %.0f%% full (count=%d) — collecting…", stream_id, fill * 100, count)
            return

        # Buffer is full, make prediction
        result = await self._predictor.predict(stream_id)
        if result is None:
            log.warning("%s prediction returned None", stream_id)
            return
            
        # Ensure result uses original source_id
        result.source_id = source_id
        
        self._latest[stream_id] = result
        self._log_result(result)
        log.info(f"✨ Prediction generated for {stream_id}: health={result.health_score:.1f}, anomaly={result.is_anomaly}")

        # Publish to prediction queue (non-blocking drop if full)
        if not self.prediction_queue.full():
            await self.prediction_queue.put(result)
        else:
            log.debug("Prediction queue full, dropping result for %s", stream_id)

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _extract_scalar(self, event: Event) -> float | None:
        """
        Pull a numeric scalar from an event value.
        For log strings, extract the most likely data value (not timestamp/register).
        For complex dict payloads, take the first numeric field.
        """
        val = event.value
        if isinstance(val, (int, float)):
            return float(val)
        if isinstance(val, dict):
            for v in val.values():
                if isinstance(v, (int, float)):
                    return float(v)
        if isinstance(val, str):
            # For log strings, extract a meaningful number (avoid timestamps)
            import re
            # Look for patterns like "value X" or "X Hz/°C/bar" etc
            # Try these patterns in order of preference:
            patterns = [
                r':\s*(-?\d+\.?\d*)',        # "value: 45.3"
                r'(\d+\.?\d*)\s*(?:Hz|°C|bar|rpm|mb|pct|ms)',  # "45.3 Hz"
                r'(\d+\.?\d*)\s*(?:for|of|to)',                # "45.3 for"
                r'-?\d+\.?\d*',              # Any number
            ]
            for pattern in patterns:
                matches = re.findall(pattern, val)
                if matches:
                    try:
                        return float(matches[-1])  # Take last match (usually most relevant)
                    except (ValueError, IndexError):
                        pass
            # Last resort: parse whole string
            try:
                return float(val.strip())
            except ValueError:
                pass
        return None

    def _log_result(self, result: PredictionResult) -> None:
        """Log prediction with appropriate severity."""
        h = result.health_score
        eta_str = f" | ETA: {result.minutes_to_event}min" if result.minutes_to_event else ""
        msg = (
            f"[{result.source_id}] health={h:.0f}/100 | "
            f"forecast_mean={sum(result.forecast)/len(result.forecast):.1f}"
            f"{eta_str}"
        )
        if result.is_anomaly:
            log.warning("ANOMALY %s | %s", msg, result.anomaly_reason)
        elif h < self.min_health_alert:
            log.warning("LOW HEALTH %s", msg)
        else:
            log.debug(msg)

    # ── Public read API ───────────────────────────────────────────────────────

    def latest_results(self) -> dict:
        # Aggregate by source_id, keeping the worst/anomalous prediction
        agg = {}
        for stream_id, r in self._latest.items():
            sid = r.source_id
            if sid not in agg or r.is_anomaly or r.health_score < agg[sid].health_score:
                agg[sid] = r
                
        return {sid: {
            "health_score"    : r.health_score,
            "is_anomaly"      : r.is_anomaly,
            "anomaly_reason"  : r.anomaly_reason,
            "current_value"   : r.current_value,
            "forecast"        : r.forecast,   # return full 20 steps, not just 5
            "minutes_to_event": r.minutes_to_event,
            "timestamp"       : r.timestamp,
            "buffer_fill"     : 1.0,
        } for sid, r in agg.items()}
    
    def _get_buffer_fill(self) -> dict:
        """Return buffer fill % (0.0-1.0) for each source (for dashboard)."""
        if not self._predictor:
            return {}
        
        fill_status = {}
        # Check all buffers that have been created
        for stream_id in list(self._predictor._buffers.keys()):
            source_id = stream_id.split("::")[0] if "::" in stream_id else stream_id
            fill = self._predictor.buffer_fill(stream_id)
            # Keep the minimum fill (most restrictive) for each source
            if source_id not in fill_status:
                fill_status[source_id] = fill
            else:
                fill_status[source_id] = min(fill_status[source_id], fill)
        
        return fill_status

    def system_health(self) -> float:
        """Aggregate health score across all monitored sources.
        
        Returns minimum health score. Anomalies reduce by 30 points.
        If no predictions yet, returns 100 (default healthy).
        """
        if not self._latest:
            return 100.0
        
        adjusted_scores = []
        for r in self._latest.values():
            score = r.health_score
            # If anomaly detected, penalize the health score significantly
            if r.is_anomaly:
                score = max(0.0, score - 30)
            adjusted_scores.append(score)
        
        result = round(min(adjusted_scores), 1) if adjusted_scores else 100.0
        if len(self._latest) > 0:
            log.debug(f"system_health: {len(self._latest)} sources, scores={[r.health_score for r in self._latest.values()]}, result={result}")
        return result
