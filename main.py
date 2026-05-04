"""main.py — IoT Failure Predictor entrypoint
Run: python main.py
     python main.py --daemon   (Linux/macOS only)
"""
from __future__ import annotations
import argparse, asyncio, logging, os, signal, sys
from pathlib import Path
import yaml

log = logging.getLogger("iot")

def load_config(path="config.yaml") -> dict:
    with open(path) as f:
        return yaml.safe_load(f)

async def consume_to_db(bus, db, twin=None) -> None:
    while True:
        event = await bus.consume()
        await db.write(event.as_dict())
        if twin and isinstance(event.value, (int, float)):
            twin.update(event.source_id, event.tag, float(event.value), event.severity)
        bus.task_done()

async def main(cfg: dict) -> None:
    level = cfg.get("app", {}).get("log_level", "INFO")
    logging.basicConfig(
        level=getattr(logging, level, logging.INFO),
        format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
        datefmt="%H:%M:%S",
    )
    log.info("Starting IoT Failure Predictor…")

    # Ensure data dir exists
    Path(cfg["app"]["data_dir"]).mkdir(parents=True, exist_ok=True)

    # Auto-generate sample.log if missing
    sample_log = Path(cfg["app"]["data_dir"]) / "sample.log"
    if not sample_log.exists():
        log.info("sample.log not found — generating…")
        try:
            import generate_sample_logs
            generate_sample_logs.generate()
        except Exception as e:
            log.warning("Could not auto-generate sample.log: %s", e)

    # Storage
    from storage.timeseries_db import TimeSeriesDB
    perf = cfg.get("performance", {})
    db = TimeSeriesDB(
        db_path=str(Path(cfg["app"]["data_dir"]) / "events.db"),
        flush_interval=perf.get("batch_flush_interval", 5.0),
        batch_max=perf.get("batch_max_size", 64),
    )
    await db.start()

    # Ingestion
    from ingestion.ingestion_router import start_ingestion
    bus = await start_ingestion(cfg)

    # Agent — run in executor so LSTM load doesn't block the event loop
    from agent.agent_core import AgentCore
    agent = AgentCore()
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, agent._load_components)

    # Simulator
    from simulator import Simulator
    sim = Simulator(prediction_queue=agent.prediction_queue)

    # Wire tasks
    asyncio.create_task(consume_to_db(bus, db, sim.twin), name="consumer")
    asyncio.create_task(agent._consume_loop(bus), name="agent")

    # API + serve index.html as static root
    from api.server import app as fastapi_app, init_api
    from fastapi.staticfiles import StaticFiles
    from fastapi.responses import FileResponse

    init_api(bus, db, agent, sim)

    # Serve index.html at "/" so the dashboard works via http://127.0.0.1:8000
    index_path = Path(__file__).parent / "index.html"

    @fastapi_app.get("/", include_in_schema=False)
    async def serve_dashboard():
        return FileResponse(str(index_path))

    import uvicorn
    api_cfg = cfg.get("api", {})
    port = api_cfg.get("port", 8000)
    uvi = uvicorn.Config(
        fastapi_app,
        host="127.0.0.1",
        port=port,
        log_level="warning",
        loop="asyncio",
        access_log=False,
    )
    server = uvicorn.Server(uvi)

    log.info("Dashboard → http://127.0.0.1:%d", port)
    log.info("Ctrl+C to stop.")

    # ── Clean cross-platform shutdown ──────────────────────────────────────
    stop = asyncio.Event()

    if os.name != "nt":
        # Unix: use proper signal handlers
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, stop.set)
        api_task = asyncio.create_task(server.serve(), name="api")
        await stop.wait()
    else:
        # Windows: just run the server; KeyboardInterrupt breaks out naturally
        try:
            await server.serve()
        except KeyboardInterrupt:
            pass
        stop.set()

    log.info("Shutting down…")
    server.should_exit = True
    await db.close()


def daemonise(config_path: str):
    """Fork into background (Unix only)."""
    if os.name == "nt":
        print("Daemon mode not supported on Windows.")
        sys.exit(1)
    pid = os.fork()
    if pid > 0:
        print(f"Daemon started (PID {pid})")
        sys.exit(0)
    os.setsid()
    Path("./data").mkdir(exist_ok=True)
    sys.stdout = sys.stderr = open("./data/predictor.log", "a")
    asyncio.run(main(load_config(config_path)))  # was missing before


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--config", default="config.yaml")
    p.add_argument("--daemon", action="store_true")
    args = p.parse_args()

    if args.daemon:
        daemonise(args.config)
    else:
        try:
            asyncio.run(main(load_config(args.config)))
        except KeyboardInterrupt:
            print("\nStopped.")
