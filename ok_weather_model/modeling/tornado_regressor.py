"""
Tornado count regressor: predicts expected tornado count from the 12Z
pre-convective environment.

Model: GradientBoostingRegressor on log1p(tornado_count) to handle the
heavy right tail (most days have 1–5 tornadoes; rare days have 30+).

Evaluation uses leave-one-year-out cross-validation.
"""
from __future__ import annotations

import logging
import math
from typing import Optional

import numpy as np
import pandas as pd

from .features import FEATURE_NAMES, extract_features, extract_features_from_indices, build_feature_matrix
from ..models.sounding import ThermodynamicIndices
from ..models.kinematic import KinematicProfile
from ..models.case import HistoricalCase

logger = logging.getLogger(__name__)


def _make_pipeline():
    from sklearn.pipeline import Pipeline
    from sklearn.impute import SimpleImputer
    from sklearn.ensemble import GradientBoostingRegressor

    return Pipeline([
        ("imputer", SimpleImputer(strategy="median")),
        ("reg", GradientBoostingRegressor(
            n_estimators=300,
            learning_rate=0.05,
            max_depth=4,
            min_samples_leaf=5,
            subsample=0.8,
            random_state=42,
        )),
    ])


class TornadoRegressor:
    """
    Predicts expected tornado count from 12Z sounding environment.

    Usage::

        reg = TornadoRegressor()
        metrics = reg.train(cases)
        result = reg.predict(indices, kinematics, ctg)
        # {'expected_count': 4.2, 'prediction_interval': (1.0, 12.0)}
    """

    def __init__(self):
        self._pipeline = None
        self.feature_importances_: Optional[pd.Series] = None
        self.n_training_cases_: int = 0
        self._train_residual_std: float = 1.0

    # ── Training ──────────────────────────────────────────────────────────────

    def train(self, cases: list[HistoricalCase]) -> dict:
        """
        Fit on all provided cases. Returns training metrics.
        """
        X, y = build_feature_matrix(cases, target="log_tornado_count")
        if len(X) < 10:
            raise ValueError(f"Need at least 10 cases to train, got {len(X)}")

        self._pipeline = _make_pipeline()
        self._pipeline.fit(X, y)
        self.n_training_cases_ = len(X)

        reg = self._pipeline.named_steps["reg"]
        self.feature_importances_ = pd.Series(
            reg.feature_importances_, index=FEATURE_NAMES
        ).sort_values(ascending=False)

        preds_log = self._pipeline.predict(X)
        residuals = y.values - preds_log
        self._train_residual_std = float(np.std(residuals))

        preds = np.expm1(preds_log)
        actual = np.expm1(y.values)
        rmse = float(np.sqrt(((preds - actual) ** 2).mean()))
        mae = float(np.abs(preds - actual).mean())

        return {
            "n_cases": len(X),
            "train_rmse": round(rmse, 2),
            "train_mae": round(mae, 2),
            "residual_std_log": round(self._train_residual_std, 3),
        }

    # ── Inference ─────────────────────────────────────────────────────────────

    def predict(
        self,
        indices: ThermodynamicIndices,
        kinematics: KinematicProfile,
        convective_temp_gap: Optional[float] = None,
    ) -> dict:
        """
        Predict tornado count and a rough 80% prediction interval.

        Returns dict with 'expected_count', 'interval_low', 'interval_high'.
        Interval is derived from training residual std in log-space.
        """
        if self._pipeline is None:
            raise RuntimeError("Model not trained. Call train() or load from registry.")

        feat = extract_features_from_indices(indices, kinematics, convective_temp_gap)
        X = pd.DataFrame([feat], columns=FEATURE_NAMES)
        log_pred = float(self._pipeline.predict(X)[0])

        # 80% interval: ±1.28 × residual_std in log-space
        z = 1.28
        log_lo = log_pred - z * self._train_residual_std
        log_hi = log_pred + z * self._train_residual_std

        return {
            "expected_count": round(max(0.0, float(np.expm1(log_pred))), 1),
            "interval_low":   round(max(0.0, float(np.expm1(log_lo))), 1),
            "interval_high":  round(max(0.0, float(np.expm1(log_hi))), 1),
        }

    # ── Evaluation ────────────────────────────────────────────────────────────

    def evaluate(self, cases: list[HistoricalCase]) -> dict:
        """
        Leave-one-year-out cross-validation on tornado count (back-transformed
        from log-space for interpretable error metrics).
        """
        rows, targets, years = [], [], []
        for case in cases:
            feat = extract_features(case)
            if feat is None:
                continue
            rows.append(feat)
            targets.append(math.log1p(case.tornado_count))
            years.append(case.date.year)

        if len(rows) < 10:
            return {"error": "Insufficient data for LOYO evaluation"}

        X = pd.DataFrame(rows, columns=FEATURE_NAMES)
        y = np.array(targets)
        years_arr = np.array(years)

        all_true_log, all_pred_log = [], []
        skipped_years = []
        for yr in sorted(set(years)):
            train_mask = years_arr != yr
            test_mask = years_arr == yr
            if train_mask.sum() < 10:
                skipped_years.append(yr)
                continue

            pipe = _make_pipeline()
            pipe.fit(X[train_mask], y[train_mask])
            all_true_log.extend(y[test_mask].tolist())
            all_pred_log.extend(pipe.predict(X[test_mask]).tolist())

        if not all_true_log:
            return {"error": "No LOYO folds produced predictions"}

        true_counts = np.expm1(all_true_log)
        pred_counts = np.expm1(all_pred_log)

        return {
            "loyo_mae_counts":  round(float(np.abs(true_counts - pred_counts).mean()), 2),
            "loyo_rmse_counts": round(float(np.sqrt(((true_counts - pred_counts) ** 2).mean())), 2),
            "loyo_mae_log":     round(float(np.abs(np.array(all_true_log) - np.array(all_pred_log)).mean()), 3),
            "n_folds":          len(set(years)) - len(skipped_years),
            "n_predictions":    len(all_true_log),
        }
