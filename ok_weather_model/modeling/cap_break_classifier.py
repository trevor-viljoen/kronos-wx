"""
cap_break_classifier.py — logistic regression for per-county cap-break probability.

Label definitions:
  positive (y=1): EARLY_EROSION + CLEAN_EROSION  — cap thermodynamically eroded
  negative (y=0): NO_EROSION                      — cap held through the day

Features are the subset of the 28-feature space available from both soundings
(training) and HRRR county snapshots (inference).  Boundary-forcing terms
(convergence_score, alarm_bell) are applied as post-processing adjustments so
they don't need historical replay data to train.

Trained: sklearn LogisticRegression(class_weight='balanced')
Saved  : registry key 'cap_break_prob_model'  (joblib via registry.py)
"""

from __future__ import annotations

import logging
from typing import Optional

import numpy as np

logger = logging.getLogger(__name__)

# Features available from both 12Z soundings (training) and HRRR (inference).
CAP_BREAK_FEATURES = [
    "MLCAPE",
    "MLCIN",
    "SRH_0_1km",
    "EHI",
    "BWD_0_6km",
    "lapse_rate_700_500",
]

_model = None   # cached after first load


def build_training_data():
    """Return (X, y) arrays from the historical case database."""
    from ok_weather_model.storage.database import Database
    from ok_weather_model.models import CapBehavior
    from ok_weather_model.modeling.features import extract_features

    db = Database()
    pos = (
        db.get_cases_by_cap_behavior(CapBehavior.EARLY_EROSION)
        + db.get_cases_by_cap_behavior(CapBehavior.CLEAN_EROSION)
    )
    neg = db.get_cases_by_cap_behavior(CapBehavior.NO_EROSION)

    rows, labels = [], []
    for cases, label in [(pos, 1), (neg, 0)]:
        for c in cases:
            loaded = db.load_case(c.case_id)
            if not loaded:
                continue
            feats = extract_features(loaded)
            if not feats:
                continue
            rows.append([feats.get(k, np.nan) for k in CAP_BREAK_FEATURES])
            labels.append(label)

    return np.array(rows, dtype=float), np.array(labels, dtype=int)


def train() -> None:
    """Train, evaluate (5-fold AUC), and save the model."""
    from sklearn.impute import SimpleImputer
    from sklearn.linear_model import LogisticRegression
    from sklearn.model_selection import cross_val_score
    from sklearn.pipeline import Pipeline
    from ok_weather_model.modeling.registry import save_model

    X, y = build_training_data()
    logger.info("Training cap_break_prob model: %d pos / %d neg", y.sum(), (y == 0).sum())

    pipe = Pipeline([
        ("imp", SimpleImputer(strategy="median")),
        ("lr",  LogisticRegression(class_weight="balanced", max_iter=500, C=1.0)),
    ])

    scores = cross_val_score(pipe, X, y, cv=5, scoring="roc_auc")
    logger.info("5-fold ROC-AUC: %.3f ± %.3f", scores.mean(), scores.std())
    print(f"cap_break_prob model  5-fold ROC-AUC: {scores.mean():.3f} ± {scores.std():.3f}")

    pipe.fit(X, y)
    save_model("cap_break_prob_model", pipe)
    logger.info("Saved cap_break_prob_model to registry")
    print("Saved → data/models/cap_break_prob_model.joblib")


def _load() -> Optional[object]:
    """Load model from registry (cached in module-level _model)."""
    global _model
    if _model is not None:
        return _model
    try:
        from ok_weather_model.modeling.registry import load_model
        _model = load_model("cap_break_prob_model")
        return _model
    except FileNotFoundError:
        return None
    except Exception as e:
        logger.warning("cap_break_prob_model load failed: %s", e)
        return None


def predict(feature_dict: dict, convergence_score: float = 0.0, alarm_bell: bool = False) -> float:
    """
    Return initiation probability in [0, 1].

    feature_dict should contain the keys in CAP_BREAK_FEATURES (missing → NaN).
    convergence_score and alarm_bell are applied as post-processing boosts on top
    of the model's base thermodynamic estimate.
    """
    pipe = _load()
    if pipe is None:
        return _physics_fallback(feature_dict, convergence_score, alarm_bell)

    row = np.array([[feature_dict.get(k, np.nan) for k in CAP_BREAK_FEATURES]])
    base_prob = float(pipe.predict_proba(row)[0, 1])

    # Boundary forcing adjustment (not in training data — applied post-model).
    if alarm_bell:
        base_prob = min(1.0, base_prob + 0.20)
    elif convergence_score > 0:
        base_prob = min(1.0, base_prob + 0.10 * convergence_score)

    return round(base_prob, 3)


def _physics_fallback(features: dict, convergence_score: float, alarm_bell: bool) -> float:
    """Original physics formula, used when no trained model exists."""
    mlcape = features.get("MLCAPE", 0.0) or 0.0
    mlcin  = features.get("MLCIN", 300.0) or 300.0
    srh    = features.get("SRH_0_1km", 0.0) or 0.0

    if mlcape < 200:
        return 0.0

    effective_cin = mlcin * max(0.3, 1.0 - 0.5 * convergence_score)
    if alarm_bell:
        effective_cin = min(effective_cin, 50.0)

    cape_score = min(1.0, (mlcape - 200) / 2800.0)
    cin_score  = max(0.0, 1.0 - effective_cin / 300.0)
    srh_score  = min(1.0, srh / 300.0)

    raw = 0.35 * cin_score + 0.30 * cape_score + 0.20 * srh_score + 0.15 * convergence_score
    if convergence_score > 0:
        raw += 0.15
    return round(min(1.0, raw), 3)
