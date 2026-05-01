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
