"""
main.py — IoT Failure Predictor entrypoint
Run: python main.py
     python main.py --daemon   (Linux/macOS background mode)
"""
from __future__ import annotations
import argparse, asyncio, logging, os, signal, sys, time
from pathlib import Path
import yaml

log = logging.getLogger("iot")

def load_config(path="config.yaml") -> dict:
    with open(path) as f: return yaml.safe_load(f)

async def consume_to_db(bus, db, twin=None) -> None:
    while True:
        event = await bus.consume()
        await db.write(event.as_dict())
        if twin and isinstance(event.value, (int, float)):
            twin.update(event.source_id, event.tag, float(event.value), event.severity)
        bus.task_done()

async def main(cfg: dict) -> None:
    level = cfg.get("app",{}).get("log_level","INFO")
    logging.basicConfig(level=getattr(logging, level, logging.INFO),
        format="%(asctime)s %(levelname)-8s %(name)s — %(message)s", datefmt="%H:%M:%S")
    log.info("Starting IoT Failure Predictor…")

    # Storage
    from storage.timeseries_db import TimeSeriesDB
    perf = cfg.get("performance",{})
    db = TimeSeriesDB(
        db_path=str(Path(cfg["app"]["data_dir"]) / "events.db"),
        flush_interval=perf.get("batch_flush_interval", 5.0),
        batch_max=perf.get("batch_max_size", 64),
    )
    await db.start()

    # Ingestion
    from ingestion.ingestion_router import start_ingestion
    bus = await start_ingestion(cfg)

    # Agent
    from agent.agent_core import AgentCore
    agent = AgentCore()
    agent._load_components()

    # Simulator
    from simulator import Simulator
    sim = Simulator(prediction_queue=agent.prediction_queue)

    # Wire simulator twin to ingestion
    asyncio.create_task(consume_to_db(bus, db, sim.twin), name="consumer")
    asyncio.create_task(agent._consume_loop(bus), name="agent")

    # API
    from api.server import app, init_api
    init_api(bus, db, agent, sim)

    import uvicorn
    api_cfg = cfg.get("api", {})
    uvi = uvicorn.Config(app, host="127.0.0.1", port=api_cfg.get("port", 8000),
                         log_level="warning", loop="asyncio", access_log=False)
    server = uvicorn.Server(uvi)

    loop = asyncio.get_running_loop()
    stop = asyncio.Event()
    # Signal handlers only work on Unix-like systems
    if os.name != 'nt':
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, stop.set)

    log.info("Dashboard → http://127.0.0.1:%d", api_cfg.get("port", 8000))
    log.info("Ctrl+C to stop.")

    api_task = asyncio.create_task(server.serve(), name="api")
    await stop.wait()
    log.info("Shutting down…")
    server.should_exit = True
    await db.close()

def daemonise():
    if os.name == "nt": print("Daemon not supported on Windows."); sys.exit(1)
    pid = os.fork()
    if pid > 0:
        print(f"Daemon started (PID {pid})")
        sys.exit(0)
    os.setsid()
    Path("./data").mkdir(exist_ok=True)
    sys.stdout = sys.stderr = open("./data/predictor.log","a")

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--config", default="config.yaml")
    p.add_argument("--daemon", action="store_true")
    args = p.parse_args()
    if args.daemon: daemonise()
    asyncio.run(main(load_config(args.config)))
