"""
Model registry: persist and restore trained model artifacts via joblib.
Artifacts are stored under data/models/ relative to the project root.
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Optional

logger = logging.getLogger(__name__)

# data/models/ lives two directories up from this file (modeling/ → ok_weather_model/ → project root)
_REGISTRY_DIR = Path(__file__).resolve().parent.parent.parent / "data" / "models"


def _artifact_path(name: str) -> Path:
    _REGISTRY_DIR.mkdir(parents=True, exist_ok=True)
    return _REGISTRY_DIR / f"{name}.joblib"


def save_model(name: str, model: Any) -> Path:
    """Serialize model to disk. Returns the saved path."""
    import joblib
    path = _artifact_path(name)
    joblib.dump(model, path)
    logger.info("Saved model '%s' → %s", name, path)
    return path


def load_model(name: str) -> Optional[Any]:
    """Deserialize model from disk. Returns None if artifact not found."""
    import joblib
    path = _artifact_path(name)
    if not path.exists():
        return None
    model = joblib.load(path)
    logger.info("Loaded model '%s' from %s", name, path)
    return model


def list_models() -> list[str]:
    """Return names of all saved model artifacts (without .joblib extension)."""
    if not _REGISTRY_DIR.exists():
        return []
    return sorted(p.stem for p in _REGISTRY_DIR.glob("*.joblib"))
