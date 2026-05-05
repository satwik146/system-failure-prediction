"""
api/server.py — FastAPI backend
Serves REST endpoints + WebSocket for live dashboard push.
"""
from __future__ import annotations
import asyncio, json, logging, time
from contextlib import asynccontextmanager
from typing import Optional
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pathlib import Path

log = logging.getLogger(__name__)

_bus = _db = _agent = _simulator = None

def init_api(bus, db, agent, simulator):
    global _bus, _db, _agent, _simulator
    _bus, _db, _agent, _simulator = bus, db, agent, simulator

class _WSManager:
    def __init__(self): self._clients: list[WebSocket] = []
    async def connect(self, ws):
        await ws.accept(); self._clients.append(ws)
    def disconnect(self, ws):
        self._clients = [c for c in self._clients if c is not ws]
    async def broadcast(self, data):
        dead, msg = [], json.dumps(data)
        for ws in self._clients:
            try: await ws.send_text(msg)
            except: dead.append(ws)
        for ws in dead: self.disconnect(ws)

ws_manager = _WSManager()

async def _push_loop():
    try:
        while True:
            await asyncio.sleep(2)
            if not ws_manager._clients:
                continue
            try:
                bus_stats = {}
                if _bus:
                    s = getattr(_bus, 'stats', None)
                    bus_stats = s() if callable(s) else (s or {})

                predictions = _agent.latest_results() if _agent else {}
                health = _agent.system_health() if _agent else 100.0
                planner = getattr(_agent, '_planner', None)
                actions = getattr(planner, 'recent_actions', [])[-5:] if planner else []
                sim = _simulator.status() if _simulator else {}

                msg = {
                    "timestamp": time.time(),
                    "bus": bus_stats,
                    "system_health": health,
                    "buffer_fill": _agent._get_buffer_fill() if _agent else {},
                    "predictions": predictions,
                    "simulator": sim,
                    "actions": actions,
                }
                await ws_manager.broadcast(msg)
                log.debug(f"WS broadcast: health={health}, predictions={len(predictions)}, clients={len(ws_manager._clients)}")
            except Exception as e:
                log.error("WS push error: %s", e, exc_info=True)
    except asyncio.CancelledError:
        log.debug("WS push loop cancelled.")

_push_task = None

@asynccontextmanager
async def _lifespan(app):
    global _push_task
    _push_task = asyncio.create_task(_push_loop(), name="ws-push")
    try:
        yield
    finally:
        if _push_task and not _push_task.done():
            _push_task.cancel()
            try:
                await _push_task
            except asyncio.CancelledError:
                pass

app = FastAPI(title="IoT Failure Predictor", lifespan=_lifespan)

DASH = Path(__file__).parent.parent / "dashboard"
if (DASH / "static").exists():
    app.mount("/static", StaticFiles(directory=str(DASH / "static")), name="static")

@app.get("/")
async def root():
    for candidate in [
        Path(__file__).parent.parent / "index.html",
        DASH / "index.html",
    ]:
        if candidate.exists():
            return FileResponse(str(candidate))
    return {"status": "ok"}

@app.get("/health")
async def health():
    return {"status": "ok", "time": time.time()}

@app.get("/api/status")
async def api_status():
    try:
        bus_stats = {}
        if _bus:
            s = getattr(_bus, 'stats', None)
            bus_stats = s() if callable(s) else (s or {})
        return {
            "bus": bus_stats,
            "system_health": _agent.system_health() if _agent else 100.0,
            "predictions": _agent.latest_results() if _agent else {},
            "simulator": _simulator.status() if _simulator else {},
        }
    except Exception as e:
        log.error("Status error: %s", e)
        return {"bus": {}, "system_health": 100.0, "predictions": {}, "simulator": {}}

@app.get("/api/events")
async def events(source_id: Optional[str] = None, limit: int = 100):
    return await _db.query_recent(source_id=source_id, limit=limit) if _db else []

@app.get("/api/predictions")
async def predictions():
    return _agent.latest_results() if _agent else {}

@app.get("/api/reports")
async def reports():
    d = Path("./data/reports")
    if not d.exists():
        return []
    out = []
    for f in sorted(d.glob("*.json"), key=lambda p: p.stat().st_mtime, reverse=True)[:20]:
        try:
            out.append(json.loads(f.read_text()))
        except:
            pass
    return out

@app.get("/api/simulator/scenarios")
async def list_scenarios():
    try:
        if _simulator and hasattr(_simulator, 'runner'):
            return _simulator.runner.list_scenarios()
        return []
    except Exception as e:
        return []

@app.post("/api/simulator/run/{name}")
async def run_scenario(name: str):
    if not _simulator:
        return {"error": "no simulator"}
    
    # Check if scenario exists before creating task
    from pathlib import Path
    scenario_path = Path(__file__).parent.parent / "simulator" / "scenarios" / f"{name}.yaml"
    if not scenario_path.exists():
        return {"error": f"Scenario not found: {name}", "available": _simulator.runner.list_scenarios()}
    
    try:
        asyncio.create_task(_simulator.run_scenario(name), name=f"scenario-{name}")
        return {"status": "started", "scenario": name}
    except Exception as e:
        log.error("Scenario error: %s", e)
        return {"error": f"Scenario failed: {e}"}

@app.post("/api/simulator/inject")
async def inject(body: dict):
    if not _simulator:
        return {"error": "no simulator"}
    try:
        h = _simulator.injector.inject(
            body.get("source_id", "unknown"),
            body.get("tag", "value"),
            body.get("fault_type", "spike"),
            **{k: v for k, v in body.items() if k not in ("source_id", "tag", "fault_type")},
        )
        log.info("Injection created: %s", h.fault_id)
        return {"status": "injected", "fault_id": h.fault_id, "active_count": len(_simulator.injector._active)}
    except Exception as e:
        log.error("Injection error: %s", e)
        return {"error": str(e)}

@app.post("/api/simulator/cancel")
async def cancel():
    if _simulator:
        count = len(_simulator.injector._active)
        _simulator.injector.cancel_all()
        log.info("Cancelled all %d injections", count)
        return {"status": "cancelled", "count": count}
    return {"status": "no simulator"}

@app.get("/api/twin")
async def twin():
    try:
        if _simulator and hasattr(_simulator, 'twin'):
            return _simulator.twin.snapshot()
        return {}
    except Exception as e:
        return {}

@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    await ws_manager.connect(ws)
    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        ws_manager.disconnect(ws)