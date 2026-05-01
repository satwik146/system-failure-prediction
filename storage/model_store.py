"""
storage/model_store.py — Save and load model artefacts.
Simple file-based store — no database needed.
"""
from __future__ import annotations
import json, pickle, logging
from pathlib import Path

log = logging.getLogger(__name__)


class ModelStore:
    def __init__(self, base_dir: str = "./agent/models/"):
        self._dir = Path(base_dir)
        self._dir.mkdir(parents=True, exist_ok=True)

    def save_pickle(self, obj, name: str) -> Path:
        p = self._dir / f"{name}.pkl"
        with open(p, "wb") as f:
            pickle.dump(obj, f)
        log.info("Saved %s → %s", name, p)
        return p

    def load_pickle(self, name: str):
        p = self._dir / f"{name}.pkl"
        if not p.exists():
            return None
        with open(p, "rb") as f:
            return pickle.load(f)

    def save_meta(self, meta: dict, name: str) -> Path:
        p = self._dir / f"{name}.json"
        p.write_text(json.dumps(meta, indent=2))
        return p

    def load_meta(self, name: str) -> dict:
        p = self._dir / f"{name}.json"
        return json.loads(p.read_text()) if p.exists() else {}

    def list_models(self) -> list[str]:
        return [f.stem for f in self._dir.glob("*.pkl")]
