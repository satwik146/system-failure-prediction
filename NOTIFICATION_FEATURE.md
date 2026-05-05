# Windows Anomaly Notification — Integration Guide

## What was added

Two files were created / modified:

| File | Change |
|---|---|
| `notifier.py` | **New file** — the self-contained notification module |
| `main.py` | **Modified** — wires the notifier into the agent pipeline |

---

## How it works

```
Anomaly detected
      │
      ▼
AgentCore  ──(wrapped method / queue.put)──▶  AnomalyNotifier.send()
      │                                               │
      │                                    cooldown + severity filter
      │                                               │
      ▼                                               ▼
_anomaly_poll_loop (safety-net, every 5 s)   Windows Action Center toast
```

1. **Method wrapping** — on startup, `_patch_agent_for_notifications()` wraps the first matching agent method (`_on_anomaly`, `_emit_prediction`, etc.) so every anomaly automatically calls the notifier.
2. **Queue wrapping** — if no method match is found, `prediction_queue.put` is wrapped instead.
3. **Polling safety net** — a background coroutine checks `agent.recent_anomalies` every 5 seconds, catching any anomalies that slip through code paths not covered by the wrappers.

---

## Installation

```bash
pip install winotify
```

> **Note:** `winotify` is Windows-only. On Linux/macOS the notifier falls back to a clearly formatted console print — no crash, no error.

---

## Running

```bash
# Default (notify on MEDIUM, HIGH, CRITICAL with 30 s cooldown)
python main.py

# Notify only on CRITICAL
python main.py --notify-severity CRITICAL

# Suppress repeat alerts for 2 minutes
python main.py --notify-cooldown 120

# Turn notifications off entirely
python main.py --no-notify
```

---

## Customising the notifier directly

```python
from notifier import AnomalyNotifier

notifier = AnomalyNotifier(
    app_id="My Monitor",      # Name shown in Action Center
    cooldown_seconds=60,      # Minimum gap between same-source alerts
    min_severity="HIGH",      # Ignore INFO / LOW / MEDIUM
)

# Fire a notification manually
notifier.send(
    source_id="Machine-1",
    severity="CRITICAL",
    message="CPU utilisation exceeded safe bounds",
    predicted_value=0.9821,
    threshold=0.75,
)
```

---

## Notification anatomy

```
┌─────────────────────────────────────────────┐
│  ⛔ [CRITICAL] Anomaly — Machine-1           │
│  CPU utilisation exceeded safe bounds        │
│  Time: 14:35:02                              │
│  Predicted value: 0.9821                     │
│  Threshold: 0.7500                           │
└─────────────────────────────────────────────┘
```

Severity icons:

| Severity | Icon |
|---|---|
| CRITICAL | ⛔ |
| HIGH | 🔴 |
| MEDIUM | 🟡 |
| LOW | 🟢 |
| INFO | ℹ️ |

---

## Troubleshooting

| Symptom | Fix |
|---|---|
| No notifications appear | Run `pip install winotify` and ensure Windows Focus Assist is off |
| Too many popups | Increase `--notify-cooldown` or raise `--notify-severity` |
| "winotify not installed" warning | Install the package; fallback console output is still active |
| Notifications appear but no sound | Expected for LOW/MEDIUM; CRITICAL/HIGH use the Alarm sound |
