"""
agent/root_cause_agent.py — Root cause analyser

Rule-based by default (zero dependencies, instant).
If Ollama is running locally, uses mistral/llama3 for richer analysis.
Falls back to rules silently if LLM unavailable.
"""
from __future__ import annotations
import logging, time
from dataclasses import dataclass, field
from typing import Optional

log = logging.getLogger(__name__)


@dataclass
class RootCauseReport:
    timestamp     : float
    source_id     : str
    probable_cause: str
    evidence      : list[str]
    recommended   : list[str]
    confidence    : float        # 0–1
    used_llm      : bool = False

    def as_dict(self) -> dict:
        return self.__dict__


# ── Rule knowledge base ───────────────────────────────────────────────────────

_RULES: list[dict] = [
    {
        "id": "memory_pressure",
        "signals": ["swap", "oom", "out of memory", "memfree", "inactive"],
        "cause":   "Memory exhaustion — swap usage high or OOM events detected",
        "actions": ["Restart high-memory processes", "Increase swap space",
                    "Add RAM or reduce process count"],
        "confidence": 0.85,
    },
    {
        "id": "disk_io_saturation",
        "signals": ["diskbusy", "iowait", "disk.*full", "no space left", "diskxfer"],
        "cause":   "Disk I/O saturation or storage full",
        "actions": ["Check disk usage with df -h", "Identify heavy writers with iotop",
                    "Archive or delete old logs"],
        "confidence": 0.80,
    },
    {
        "id": "cpu_overload",
        "signals": ["cpu.*100", "load average", "wait%", "runqueue"],
        "cause":   "CPU overload — sustained high utilisation or runqueue buildup",
        "actions": ["Identify top process with top/htop", "Check for runaway threads",
                    "Scale horizontally or throttle workload"],
        "confidence": 0.78,
    },
    {
        "id": "network_fault",
        "signals": ["connection refused", "timeout", "unreachable", "eth1", "packet loss"],
        "cause":   "Network connectivity or bandwidth issue",
        "actions": ["Ping gateway and DNS", "Check interface stats with ip -s link",
                    "Verify firewall rules"],
        "confidence": 0.75,
    },
    {
        "id": "sensor_fault",
        "signals": ["crc mismatch", "register.*timeout", "modbus", "serial.*error"],
        "cause":   "Sensor or fieldbus communication fault",
        "actions": ["Check sensor wiring and power", "Verify Modbus unit ID and baud rate",
                    "Replace sensor if hardware fault confirmed"],
        "confidence": 0.82,
    },
    {
        "id": "thermal",
        "signals": ["temperature", "overheat", "thermal", "fan", "°c"],
        "cause":   "Thermal event — component temperature exceeded safe range",
        "actions": ["Check cooling fans", "Reduce ambient temperature",
                    "Lower workload or throttle device"],
        "confidence": 0.88,
    },
    {
        "id": "power_spike",
        "signals": ["power surge", "spike", "current_draw", "voltage drop", "power", "amps", "power spike"],
        "cause":   "Electrical power surge or instability detected",
        "actions": ["Verify UPS status", "Check electrical grounding",
                    "Isolate affected power phases"],
        "confidence": 0.86,
    },
    {
        "id": "network_partition",
        "signals": ["timeout_rate", "packet_loss", "flood_errors", "gateway unreachable"],
        "cause":   "Severe network packet loss or partition",
        "actions": ["Reset network gateway switch", "Check physical cabling",
                    "Failover to redundant network link"],
        "confidence": 0.89,
    },
]


class RootCauseAgent:

    def __init__(self, ollama_model: str = "mistral", ollama_url: str = "http://localhost:11434"):
        self._ollama_model = ollama_model
        self._ollama_url   = ollama_url
        self._ollama_ok    : Optional[bool] = None   # None = not tested yet

    # ── Public ────────────────────────────────────────────────────────────────

    def analyse(self, source_id: str, events: list[str],
                anomaly_reason: str = "", health_score: float = 100.0) -> RootCauseReport:
        """
        Analyse a list of recent log/event strings for a source.
        Returns a RootCauseReport synchronously.
        """
        context = " ".join(events[-20:]).lower()   # last 20 events, lowercased
        if anomaly_reason:
            context += " " + anomaly_reason.lower()

        # Try rules first (fast)
        report = self._rule_match(source_id, context, events)

        # Try LLM enrichment if available
        if self._ollama_available():
            try:
                llm_cause, llm_actions = self._llm_analyse(context, report)
                report.probable_cause = llm_cause
                report.recommended    = llm_actions
                report.used_llm       = True
            except Exception as exc:
                log.debug("LLM analysis failed: %s", exc)

        return report

    # ── Rules ─────────────────────────────────────────────────────────────────

    def _rule_match(self, source_id: str, context: str, events: list[str]) -> RootCauseReport:
        import re
        best_rule  = None
        best_score = 0

        for rule in _RULES:
            hits = sum(1 for sig in rule["signals"] if re.search(sig, context))
            if hits > best_score:
                best_score = hits
                best_rule  = rule

        if best_rule and best_score > 0:
            evidence = [e for e in events[-10:]
                        if any(s in e.lower() for s in best_rule["signals"])]
            return RootCauseReport(
                timestamp      = time.time(),
                source_id      = source_id,
                probable_cause = best_rule["cause"],
                evidence       = evidence[:5],
                recommended    = best_rule["actions"],
                confidence     = min(0.95, best_rule["confidence"] + best_score * 0.02),
            )

        return RootCauseReport(
            timestamp      = time.time(),
            source_id      = source_id,
            probable_cause = "Unknown — insufficient pattern match",
            evidence       = events[-3:],
            recommended    = ["Review raw logs manually", "Check system metrics dashboard"],
            confidence     = 0.2,
        )

    # ── LLM (optional) ────────────────────────────────────────────────────────

    def _ollama_available(self) -> bool:
        if self._ollama_ok is not None:
            return self._ollama_ok
        try:
            import urllib.request
            urllib.request.urlopen(f"{self._ollama_url}/api/tags", timeout=1)
            self._ollama_ok = True
        except Exception:
            self._ollama_ok = False
        return self._ollama_ok

    def _llm_analyse(self, context: str, base: RootCauseReport) -> tuple[str, list[str]]:
        import urllib.request, urllib.error, json
        prompt = (
            f"You are a system reliability engineer. Analyse this IoT system event context:\n"
            f"---\n{context[:800]}\n---\n"
            f"Current diagnosis: {base.probable_cause}\n"
            f"Give a concise root cause (1 sentence) and 3 bullet remediation steps.\n"
            f"Reply in JSON: {{\"cause\": \"...\", \"steps\": [\"...\",\"...\",\"...\"]}}"
        )
        body = json.dumps({
            "model": self._ollama_model, "prompt": prompt,
            "stream": False, "options": {"num_predict": 200}
        }).encode()
        req  = urllib.request.Request(
            f"{self._ollama_url}/api/generate",
            data=body, headers={"Content-Type": "application/json"}
        )
        with urllib.request.urlopen(req, timeout=10) as r:
            resp = json.loads(r.read())
        data = json.loads(resp["response"].strip())
        return data["cause"], data["steps"]
