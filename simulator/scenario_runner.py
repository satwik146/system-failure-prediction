"""
simulator/scenario_runner.py — Runs named fault scenarios from YAML files.

A scenario is a sequence of fault injections with delays between them.
Results are compared against the LSTM prediction to validate accuracy.
"""
from __future__ import annotations
import asyncio, logging, time, yaml
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional
from simulator.digital_twin import DigitalTwin
from simulator.fault_injector import FaultInjector, InjectionHandle

log = logging.getLogger(__name__)

SCENARIOS_DIR = Path(__file__).parent / "scenarios"


@dataclass
class ScenarioResult:
    name          : str
    started_at    : float
    finished_at   : float
    steps_run     : int
    predicted_ok  : bool          # did agent detect the fault?
    detection_time: Optional[float] = None   # seconds from inject to detection
    notes         : str = ""

    def as_dict(self) -> dict:
        return self.__dict__


class ScenarioRunner:
    """
    Loads and executes fault scenarios.
    Optionally watches a prediction_queue to measure detection latency.
    """

    def __init__(self, twin: DigitalTwin, injector: FaultInjector):
        self._twin     = twin
        self._injector = injector
        self._results  : list[ScenarioResult] = []

    # ── Public ────────────────────────────────────────────────────────────────

    def list_scenarios(self) -> list[str]:
        return [p.stem for p in SCENARIOS_DIR.glob("*.yaml")]

    async def run(self, scenario_name: str,
                  prediction_queue: Optional[asyncio.Queue] = None) -> ScenarioResult:
        path = SCENARIOS_DIR / f"{scenario_name}.yaml"
        if not path.exists():
            raise FileNotFoundError(f"Scenario not found: {path}")

        cfg = yaml.safe_load(path.read_text())
        log.info("Running scenario: %s — %s", scenario_name, cfg.get("description",""))

        started    = time.time()
        handles    : list[InjectionHandle] = []
        steps_run  = 0
        detected   = False
        detect_time= None

        for step in cfg.get("steps", []):
            delay = step.get("delay_before", 0)
            if delay:
                await asyncio.sleep(delay)

            source_id  = step["source_id"]
            tag        = step["tag"]
            fault_type = step["fault_type"]
            kwargs     = {k: v for k, v in step.items()
                          if k not in ("source_id","tag","fault_type","delay_before")}

            h = self._injector.inject(source_id, tag, fault_type, **kwargs)
            handles.append(h)
            steps_run += 1

            # Wait up to step duration + 5s for detection
            if prediction_queue:
                deadline = time.time() + kwargs.get("duration", 30) + 5
                while time.time() < deadline:
                    try:
                        pred = prediction_queue.get_nowait()
                        if pred.is_anomaly and pred.source_id == source_id:
                            detected    = True
                            detect_time = time.time() - started
                            log.info("Scenario '%s': anomaly detected in %.1fs", scenario_name, detect_time)
                            break
                    except asyncio.QueueEmpty:
                        await asyncio.sleep(0.5)

        # Cancel all running injections
        for h in handles:
            h.cancel()

        result = ScenarioResult(
            name           = scenario_name,
            started_at     = started,
            finished_at    = time.time(),
            steps_run      = steps_run,
            predicted_ok   = detected,
            detection_time = detect_time,
            notes          = cfg.get("description", ""),
        )
        self._results.append(result)
        log.info("Scenario '%s' complete. Detected: %s", scenario_name, detected)
        return result

    @property
    def results(self) -> list[dict]:
        return [r.as_dict() for r in self._results]
