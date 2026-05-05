"""
main.py — IoT Failure Predictor entrypoint  (+ Windows anomaly notifications)

Run:  python main.py
      python main.py --daemon  (Linux/macOS background mode)

New flags:
  --no-notify          Disable Windows toast notifications
  --notify-severity    Minimum severity to notify: INFO|LOW|MEDIUM|HIGH|CRITICAL  (default: MEDIUM)
  --notify-cooldown    Seconds between repeat alerts for same source (default: 30)
"""

from __future__ import annotations

import argparse, asyncio, logging, os, signal, sys, time
from pathlib import Path

import yaml

log = logging.getLogger("iot")


def load_config(path="config.yaml") -> dict:
    with open(path) as f:
        return yaml.safe_load(f)


# ─── notification helper ──────────────────────────────────────────────────────

def _make_notifier(args) -> "AnomalyNotifier | None":
    """Instantiate the notifier unless --no-notify was passed."""
    if getattr(args, "no_notify", False):
        log.info("Windows notifications disabled via --no-notify flag.")
        return None
    try:
        from notifier import AnomalyNotifier
        notifier = AnomalyNotifier(
            app_id="IoT Failure Predictor",
            cooldown_seconds=getattr(args, "notify_cooldown", 30),
            min_severity=getattr(args, "notify_severity", "MEDIUM"),
        )
        log.info(
            "Windows notifications enabled (min_severity=%s, cooldown=%ds).",
            getattr(args, "notify_severity", "MEDIUM"),
            getattr(args, "notify_cooldown", 30),
        )
        return notifier
    except Exception as exc:
        log.warning("Could not initialise notifier: %s", exc)
        return None


def _patch_agent_for_notifications(agent, notifier) -> None:
    """
    Monkey-patch the AgentCore so every anomaly/prediction it emits also
    fires a Windows toast notification.

    Works by wrapping the method that enqueues predictions.  The exact method
    name may differ slightly between project versions; we try the most common
    ones.
    """
    if notifier is None:
        return

    # Try to wrap `_on_anomaly`, `_emit_prediction`, or `on_prediction` -------
    wrapped = False
    for method_name in ("_on_anomaly", "_emit_prediction", "on_prediction", "_handle_prediction"):
        original = getattr(agent, method_name, None)
        if original is None:
            continue

        def _make_wrapper(orig):
            def wrapper(*args, **kwargs):
                result = orig(*args, **kwargs)
                _notify_from_call(notifier, args, kwargs)
                return result
            return wrapper

        setattr(agent, method_name, _make_wrapper(original))
        log.debug("Wrapped agent method '%s' for notifications.", method_name)
        wrapped = True
        break

    # Fallback: wrap the prediction_queue.put / put_nowait --------------------
    if not wrapped and hasattr(agent, "prediction_queue"):
        q = agent.prediction_queue
        original_put = q.put

        async def _put_with_notify(item):
            await original_put(item)
            _notify_from_prediction(notifier, item)

        q.put = _put_with_notify
        log.debug("Wrapped prediction_queue.put for notifications.")


def _notify_from_call(notifier, args, kwargs):
    """Extract anomaly details from positional/keyword args and notify."""
    # Best-effort extraction — adapt if your AgentCore uses different arg names
    severity  = kwargs.get("severity") or (args[1] if len(args) > 1 else "HIGH")
    source_id = kwargs.get("source_id") or (args[0] if len(args) > 0 else "unknown")
    message   = kwargs.get("message", "Anomaly detected by prediction agent")
    predicted = kwargs.get("predicted_value") or kwargs.get("value")
    threshold = kwargs.get("threshold")
    try:
        notifier.send(
            source_id=str(source_id),
            severity=str(severity),
            message=str(message),
            predicted_value=float(predicted) if predicted is not None else None,
            threshold=float(threshold) if threshold is not None else None,
        )
    except Exception as exc:
        log.debug("Notifier send error: %s", exc)


def _notify_from_prediction(notifier, item):
    """Extract details from a prediction queue item (dict or object)."""
    try:
        if isinstance(item, dict):
            source_id = item.get("source_id") or item.get("machine") or "unknown"
            severity  = item.get("severity", "HIGH")
            message   = item.get("message", "Anomaly detected")
            predicted = item.get("predicted_value") or item.get("value")
            threshold = item.get("threshold")
        else:
            source_id = getattr(item, "source_id", "unknown")
            severity  = getattr(item, "severity", "HIGH")
            message   = getattr(item, "message", "Anomaly detected")
            predicted = getattr(item, "predicted_value", None)
            threshold = getattr(item, "threshold", None)

        notifier.send(
            source_id=str(source_id),
            severity=str(severity),
            message=str(message),
            predicted_value=float(predicted) if predicted is not None else None,
            threshold=float(threshold) if threshold is not None else None,
        )
    except Exception as exc:
        log.debug("Notifier send error: %s", exc)


# ─── background poller (safety net) ──────────────────────────────────────────

async def _anomaly_poll_loop(agent, notifier, db, poll_interval: float = 5.0):
    """
    Periodically scan the DB / agent state for new anomalies and notify.
    Acts as a safety net if the method-wrapping above misses any code paths.
    """
    if notifier is None:
        return

    seen_ids: set = set()

    while True:
        await asyncio.sleep(poll_interval)
        try:
            # Try agent's built-in anomaly list (name may vary)
            anomalies = (
                getattr(agent, "recent_anomalies", None)
                or getattr(agent, "anomaly_log", None)
                or []
            )
            for a in anomalies:
                uid = (
                    a.get("id") or a.get("timestamp") or id(a)
                    if isinstance(a, dict)
                    else getattr(a, "id", id(a))
                )
                if uid in seen_ids:
                    continue
                seen_ids.add(uid)
                _notify_from_prediction(notifier, a)
        except Exception as exc:
            log.debug("Poll loop error: %s", exc)


# ─── main ─────────────────────────────────────────────────────────────────────

async def main(cfg: dict, args) -> None:
    level = cfg.get("app", {}).get("log_level", "INFO")
    logging.basicConfig(
        level=getattr(logging, level, logging.INFO),
        format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
        datefmt="%H:%M:%S",
    )
    log.info("Starting IoT Failure Predictor…")

    try:
        from generate_sample_logs import generate
        log_path = Path(cfg.get("app", {}).get("data_dir", "./data")) / "sample.log"
        if not log_path.exists():
            log.info("Auto-generating sample.log because it is missing...")
            generate(out_path=str(log_path))
    except Exception as e:
        log.error(f"Could not generate sample logs: {e}")

    # Notifier (new) ----------------------------------------------------------
    notifier = _make_notifier(args)

    # Storage -----------------------------------------------------------------
    from storage.timeseries_db import TimeSeriesDB

    perf = cfg.get("performance", {})
    db = TimeSeriesDB(
        db_path=str(Path(cfg["app"]["data_dir"]) / "events.db"),
        flush_interval=perf.get("batch_flush_interval", 5.0),
        batch_max=perf.get("batch_max_size", 64),
    )
    await db.start()

    # Ingestion ---------------------------------------------------------------
    from ingestion.ingestion_router import start_ingestion

    bus = await start_ingestion(cfg)

    # Agent -------------------------------------------------------------------
    from agent.agent_core import AgentCore

    agent = AgentCore()
    agent._load_components()

    # Patch agent for notifications (new) -------------------------------------
    _patch_agent_for_notifications(agent, notifier)

    # Simulator ---------------------------------------------------------------
    from simulator import Simulator

    sim = Simulator(prediction_queue=agent.prediction_queue)

    # Wire tasks --------------------------------------------------------------
    asyncio.create_task(consume_to_db(bus, db, sim.twin), name="consumer")
    asyncio.create_task(agent._consume_loop(bus), name="agent")
    asyncio.create_task(sim.start_background_traffic(bus, cfg), name="sim_bg")

    # Polling safety-net (new) ------------------------------------------------
    asyncio.create_task(
        _anomaly_poll_loop(agent, notifier, db, poll_interval=5.0),
        name="notifier_poll",
    )

    # API ---------------------------------------------------------------------
    from api.server import app, init_api

    init_api(bus, db, agent, sim)

    import uvicorn

    api_cfg = cfg.get("api", {})
    uvi = uvicorn.Config(
        app,
        host="127.0.0.1",
        port=api_cfg.get("port", 8000),
        log_level="warning",
        loop="asyncio",
        access_log=False,
    )
    server = uvicorn.Server(uvi)
    loop = asyncio.get_running_loop()
    stop = asyncio.Event()

    if os.name != "nt":
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, stop.set)

    log.info("Dashboard → http://127.0.0.1:%d", api_cfg.get("port", 8000))
    log.info("Ctrl+C to stop.")

    api_task = asyncio.create_task(server.serve(), name="api")
    try:
        await stop.wait()
    except (asyncio.CancelledError, KeyboardInterrupt):
        log.info("Received shutdown signal.")
    
    log.info("Shutting down…")
    server.should_exit = True
    
    # Wait for server to shut down gracefully (max 5 seconds)
    try:
        await asyncio.wait_for(api_task, timeout=5.0)
    except asyncio.TimeoutError:
        log.warning("Server shutdown timeout, forcing cancel.")
        api_task.cancel()
    except asyncio.CancelledError:
        pass
    
    # Cancel all remaining background tasks (agent, ingestion, simulator, etc.)
    for task in asyncio.all_tasks():
        if task is not asyncio.current_task() and not task.done():
            task.cancel()
    
    # Give tasks a moment to handle cancellation
    await asyncio.sleep(0.1)
    
    await db.close()


async def consume_to_db(bus, db, twin=None) -> None:
    try:
        while True:
            event = await bus.consume()
            await db.write(event.as_dict())
            if twin and isinstance(event.value, (int, float)):
                twin.update(event.source_id, event.tag, float(event.value), event.severity)
            bus.task_done()
    except asyncio.CancelledError:
        log.debug("Database consumer cancelled.")
        raise


# ─── daemon / entry ──────────────────────────────────────────────────────────

def daemonise():
    if os.name == "nt":
        print("Daemon not supported on Windows.")
        sys.exit(1)
    pid = os.fork()
    if pid > 0:
        print(f"Daemon started (PID {pid})")
        sys.exit(0)
    os.setsid()
    Path("./data").mkdir(exist_ok=True)
    sys.stdout = sys.stderr = open("./data/predictor.log", "a")


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="IoT Failure Predictor")
    p.add_argument("--config", default="config.yaml")
    p.add_argument("--daemon", action="store_true", help="Run as background daemon (Linux/macOS only)")
    # ── notification flags (new) ──
    p.add_argument("--no-notify", action="store_true", help="Disable Windows toast notifications")
    p.add_argument(
        "--notify-severity",
        default="MEDIUM",
        choices=["INFO", "LOW", "MEDIUM", "HIGH", "CRITICAL"],
        help="Minimum anomaly severity that triggers a notification (default: MEDIUM)",
    )
    p.add_argument(
        "--notify-cooldown",
        type=int,
        default=30,
        metavar="SECONDS",
        help="Cooldown between repeat alerts for the same source (default: 30s)",
    )

    args = p.parse_args()
    if args.daemon:
        daemonise()
    
    try:
        asyncio.run(main(load_config(args.config), args))
    except KeyboardInterrupt:
        log.info("Interrupted by user.")
        sys.exit(0)
