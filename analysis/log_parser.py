"""
analysis/log_parser.py — Log line parser
Extracts timestamp, severity, source, message from raw text lines.
"""
from __future__ import annotations
import re, logging
from dataclasses import dataclass, field
from typing import Optional

log = logging.getLogger(__name__)

# Common log patterns (first match wins)
_PATTERNS = [
    # ISO timestamp: 2019-01-06 05:10:04 [ERROR] source: msg
    re.compile(r"(?P<ts>\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2})"
               r"[\s\[\]]*(?P<level>CRITICAL|ERROR|WARN(?:ING)?|INFO|DEBUG)[\s\[\]]*"
               r"(?P<source>\S+)?[:\s]+(?P<msg>.+)"),
    # syslog: Jan  6 05:10:04 host process[pid]: msg
    re.compile(r"(?P<ts>[A-Z][a-z]{2}\s+\d+\s+\d{2}:\d{2}:\d{2})\s+"
               r"\S+\s+(?P<source>\S+?)(?:\[\d+\])?:\s+(?P<msg>.+)"),
    # bare: [ERROR] msg
    re.compile(r"\[(?P<level>CRITICAL|ERROR|WARN(?:ING)?|INFO|DEBUG)\]\s+(?P<msg>.+)"),
]

_LEVEL_MAP = {"WARNING": "warning", "WARN": "warning", "ERROR": "error",
              "CRITICAL": "critical", "DEBUG": "debug", "INFO": "info"}

_KEYWORDS = {
    "critical": ["critical","fatal","emerg","kernel panic","segfault","oom","out of memory"],
    "error":    ["error","err ","fail","exception","refused","unreachable","corrupt"],
    "warning":  ["warn","timeout","retry","slow","high","exceeded","spike"],
}

_ERROR_CODE = re.compile(r"\b(E\d{3,6}|0x[0-9a-fA-F]{2,8}|errno\s*\d+)\b")
_NUMBER     = re.compile(r"[-+]?\d+\.?\d*")


@dataclass(slots=True)
class ParsedLine:
    raw      : str
    severity : str = "info"
    source   : str = ""
    message  : str = ""
    timestamp: str = ""
    error_codes: list = field(default_factory=list)
    numbers  : list  = field(default_factory=list)

    def as_dict(self) -> dict:
        return {k: getattr(self, k) for k in
                ("raw","severity","source","message","timestamp","error_codes","numbers")}


def parse_line(line: str) -> ParsedLine:
    p = ParsedLine(raw=line)
    for pat in _PATTERNS:
        m = pat.search(line)
        if m:
            gd = m.groupdict()
            p.timestamp = gd.get("ts", "")
            p.source    = (gd.get("source") or "").strip("[]:")
            p.message   = (gd.get("msg") or line).strip()
            lvl = (gd.get("level") or "").upper()
            p.severity  = _LEVEL_MAP.get(lvl, _infer_severity(line))
            break
    else:
        p.message  = line
        p.severity = _infer_severity(line)

    p.error_codes = _ERROR_CODE.findall(line)
    p.numbers     = [float(n) for n in _NUMBER.findall(p.message)]
    return p


def _infer_severity(line: str) -> str:
    lower = line.lower()
    for level, kws in _KEYWORDS.items():
        if any(k in lower for k in kws):
            return level
    return "info"
