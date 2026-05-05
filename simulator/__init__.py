"""
simulator — Digital twin and fault injection simulator
"""
from simulator.digital_twin import DigitalTwin, TwinState
from simulator.fault_injector import FaultInjector
from simulator.scenario_runner import ScenarioRunner
import asyncio


class Simulator:
    """
    Wrapper orchestrating the digital twin, fault injector, and scenario runner.
    Provides a unified interface for the agent to query system state and inject faults.
    """

    def __init__(self, prediction_queue: asyncio.Queue = None):
        self.twin = DigitalTwin(history_len=200)
        self.injector = FaultInjector(self.twin)
        self.runner = ScenarioRunner(self.twin, self.injector)
        self.prediction_queue = prediction_queue

    async def inject_fault(self, source_id: str, tag: str, fault_type: str, **kwargs):
        """Inject a fault into the simulator."""
        return self.injector.inject(source_id, tag, fault_type, **kwargs)

    def get_state(self, source_id: str = None):
        """Get current state of the twin."""
        if source_id:
            return self.twin._states.get(source_id)
        return self.twin._states
    
    def status(self) -> dict:
        """Get simulator status."""
        return {
            "twin_sources": len(self.twin._states),
            "active_injections": len(self.injector._active),
            "scenarios_available": self.runner.list_scenarios(),
            "latest_states": {
                sid: {
                    "timestamp": state.timestamp,
                    "severity": state.severity,
                    "metrics": state.metrics,
                }
                for sid, state in self.twin._states.items()
            }
        }

    async def run_scenario(self, scenario_name: str):
        """Run a predefined failure scenario."""
        return await self.runner.run(scenario_name, prediction_queue=self.prediction_queue)

    async def start_background_traffic(self, bus, cfg: dict):
        """Generates continuous virtual sensor data and pushes to bus."""
        import random
        from ingestion.event_bus import Event, SourceType
        
        sim_cfg = cfg.get("simulator", {})
        if not sim_cfg.get("enabled", False):
            return
            
        virtual_sources = sim_cfg.get("virtual_sources", [])
        tick_interval = sim_cfg.get("tick_interval", 2.0)
        
        try:
            while True:
                for src in virtual_sources:
                    sid = src["id"]
                    state = self.twin.get_state(sid)
                    
                    if state and state.is_injected:
                        # Fault injector is controlling this source
                        # Push the current (faulty) twin metrics to the bus so agent sees them
                        for tag, val in state.metrics.items():
                            await bus.publish(Event(SourceType.SYSTEM, sid, tag, val, severity=state.severity))
                    else:
                        # Generate normal baseline data with noise
                        for tag, meta in src.get("tags", {}).items():
                            base = meta.get("base", 0.0)
                            noise = meta.get("noise", 0.0)
                            val = base + random.uniform(-noise, noise)
                            self.twin.update(sid, tag, val, severity="info", injected=False)
                            await bus.publish(Event(SourceType.SYSTEM, sid, tag, val, severity="info"))
                
                await asyncio.sleep(tick_interval)
        except asyncio.CancelledError:
            import logging
            logging.getLogger(__name__).debug("Background traffic simulator cancelled.")
            raise

