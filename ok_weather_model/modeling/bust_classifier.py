"""
Bust classifier: predicts NULL_BUST vs tornado-producing day from the 12Z
pre-convective environment.

Operationally: given that SPC has painted a threat area over Oklahoma, will
the cap hold all day (bust) or will tornadoes actually occur?

Training labels
---------------
    0 = NULL_BUST   — SPC ≥10% probability, 0 OK tornadoes
    1 = OUTBREAK    — any day with ≥1 OK tornado (SIG, WEAK, ISOLATED_SIG)

Class balance: ~444 NULL_BUST vs ~458 OUTBREAK (≈1:1 — no class weighting needed).

Model: RandomForestClassifier, LOYO cross-validation, calibrated threshold.
"""
from __future__ import annotations

import logging
from typing import Optional

import numpy as np
import pandas as pd

from .features import FEATURE_NAMES, extract_features, extract_features_from_indices, build_feature_matrix
from ..models.sounding import ThermodynamicIndices
from ..models.kinematic import KinematicProfile
from ..models.case import HistoricalCase

logger = logging.getLogger(__name__)

_BUST_CLASSES = {0: "NULL_BUST", 1: "OUTBREAK"}
_TORNADO_EVENT_CLASSES = {"SIGNIFICANT_OUTBREAK", "WEAK_OUTBREAK", "ISOLATED_SIGNIFICANT", "SURPRISING_OUTBREAK"}


def _make_pipeline():
    from sklearn.pipeline import Pipeline
    from sklearn.impute import SimpleImputer
    from sklearn.ensemble import RandomForestClassifier

    return Pipeline([
        ("imputer", SimpleImputer(strategy="median", keep_empty_features=True)),
        ("clf", RandomForestClassifier(
            n_estimators=300,
            max_features="sqrt",
            min_samples_leaf=3,
            random_state=42,
            n_jobs=-1,
        )),
    ])


class BustClassifier:
    """
    Binary classifier: NULL_BUST (0) vs OUTBREAK (1).

    With ~1:1 class balance, no class weighting is applied — the natural
    prior is already balanced. threshold_ is calibrated via 5-fold CV to
    maximise F1 on NULL_BUST (the operationally costly miss).

    Usage::

        clf = BustClassifier()
        metrics = clf.train(cases)
        probs = clf.predict_proba(indices, kinematics)
        label = clf.predict(indices, kinematics)
        # {'bust': 0.72, 'outbreak': 0.28}  /  'NULL_BUST'
    """

    def __init__(self):
        self._pipeline = None
        self.feature_importances_: Optional[pd.Series] = None
        self.n_training_cases_: int = 0
        self.threshold_: float = 0.5

    # ── Training ──────────────────────────────────────────────────────────────

    def train(self, cases: list[HistoricalCase]) -> dict:
        """
        Fit on all provided cases. Returns summary training metrics.
        Also calibrates threshold_ via 5-fold CV F1 on NULL_BUST class.
        """
        from sklearn.model_selection import cross_val_predict
        from sklearn.metrics import precision_recall_curve

        rows, targets = [], []
        for case in cases:
            feat = extract_features(case)
            if feat is None:
                continue
            if case.event_class.value == "NULL_BUST":
                t = 0
            elif case.event_class.value in _TORNADO_EVENT_CLASSES:
                t = 1
            else:
                continue
            rows.append(feat)
            targets.append(t)

        X = pd.DataFrame(rows, columns=FEATURE_NAMES)
        y = np.array(targets)

        if len(X) < 20:
            raise ValueError(f"Need at least 20 cases to train, got {len(X)}")

        self._pipeline = _make_pipeline()
        self._pipeline.fit(X, y)
        self.n_training_cases_ = len(X)

        clf = self._pipeline.named_steps["clf"]
        self.feature_importances_ = pd.Series(
            clf.feature_importances_, index=FEATURE_NAMES
        ).sort_values(ascending=False)

        # Calibrate threshold: maximise F1 on NULL_BUST (class 0)
        # Use class-0 probability = 1 - class-1 probability
        cv_probs_1 = cross_val_predict(
            _make_pipeline(), X, y, cv=5, method="predict_proba", n_jobs=-1
        )[:, 1]
        cv_probs_bust = 1.0 - cv_probs_1  # probability of NULL_BUST
        y_bust = (y == 0).astype(int)     # 1 = bust, 0 = outbreak
        prec, rec, thresholds = precision_recall_curve(y_bust, cv_probs_bust)
        f1 = np.where(
            (prec[:-1] + rec[:-1]) > 0,
            2 * prec[:-1] * rec[:-1] / (prec[:-1] + rec[:-1]),
            0.0,
        )
        best_idx = int(np.argmax(f1))
        # threshold_ is applied to bust probability (1 - outbreak_prob)
        self.threshold_ = float(round(thresholds[best_idx], 3))

        preds = self._pipeline.predict(X)
        return {
            "n_cases": len(X),
            "n_bust": int((y == 0).sum()),
            "n_outbreak": int((y == 1).sum()),
            "train_accuracy": round(float((preds == y).mean()), 3),
            "calibrated_threshold": self.threshold_,
        }

    # ── Inference ─────────────────────────────────────────────────────────────

    def predict_proba(
        self,
        indices: ThermodynamicIndices,
        kinematics: KinematicProfile,
        convective_temp_gap: Optional[float] = None,
        surface_dewpoint_f: Optional[float] = None,
        moisture_return_gradient_f: Optional[float] = None,
        gulf_moisture_fraction: Optional[float] = None,
        modified_MLCAPE: Optional[float] = None,
        modified_MLCIN: Optional[float] = None,
    ) -> dict[str, float]:
        """
        Return probability estimates for a live environment snapshot.

        Returns dict: {'bust': 0.0–1.0, 'outbreak': 0.0–1.0}
        """
        if self._pipeline is None:
            raise RuntimeError("Model not trained. Call train() or load from registry.")

        feat = extract_features_from_indices(
            indices, kinematics, convective_temp_gap,
            surface_dewpoint_f=surface_dewpoint_f,
            moisture_return_gradient_f=moisture_return_gradient_f,
            gulf_moisture_fraction=gulf_moisture_fraction,
            modified_MLCAPE=modified_MLCAPE,
            modified_MLCIN=modified_MLCIN,
        )
        X = pd.DataFrame([feat], columns=FEATURE_NAMES)
        probs = self._pipeline.predict_proba(X)[0]
        prob_map = dict(zip(self._pipeline.classes_, probs))

        return {
            "bust":     round(float(prob_map.get(0, 0.0)), 3),
            "outbreak": round(float(prob_map.get(1, 0.0)), 3),
        }

    def predict(
        self,
        indices: ThermodynamicIndices,
        kinematics: KinematicProfile,
        convective_temp_gap: Optional[float] = None,
        surface_dewpoint_f: Optional[float] = None,
        moisture_return_gradient_f: Optional[float] = None,
        gulf_moisture_fraction: Optional[float] = None,
        modified_MLCAPE: Optional[float] = None,
        modified_MLCIN: Optional[float] = None,
    ) -> str:
        """Return 'NULL_BUST' or 'OUTBREAK' using calibrated threshold_."""
        probs = self.predict_proba(
            indices, kinematics, convective_temp_gap,
            surface_dewpoint_f, moisture_return_gradient_f,
            gulf_moisture_fraction, modified_MLCAPE, modified_MLCIN,
        )
        return "NULL_BUST" if probs["bust"] >= self.threshold_ else "OUTBREAK"

    # ── Evaluation ────────────────────────────────────────────────────────────

    def evaluate(self, cases: list[HistoricalCase]) -> dict:
        """
        Leave-one-year-out cross-validation.

        Reports metrics at both default 0.5 and F1-optimised threshold for
        NULL_BUST recall, since missing a bust (predicting outbreak when cap
        holds) is the operationally costly error.
        """
        from sklearn.metrics import (
            accuracy_score, roc_auc_score, classification_report,
            precision_recall_curve,
        )

        rows, targets, years = [], [], []
        for case in cases:
            feat = extract_features(case)
            if feat is None:
                continue
            if case.event_class.value == "NULL_BUST":
                t = 0
            elif case.event_class.value in _TORNADO_EVENT_CLASSES:
                t = 1
            else:
                continue
            rows.append(feat)
            targets.append(t)
            years.append(case.date.year)

        if len(rows) < 20:
            return {"error": "Insufficient data for LOYO evaluation"}

        X = pd.DataFrame(rows, columns=FEATURE_NAMES)
        y = np.array(targets)
        years_arr = np.array(years)

        all_true, all_pred, all_prob = [], [], []
        skipped_years = []
        for yr in sorted(set(years)):
            train_mask = years_arr != yr
            test_mask = years_arr == yr
            if train_mask.sum() < 20:
                skipped_years.append(yr)
                continue

            pipe = _make_pipeline()
            pipe.fit(X[train_mask], y[train_mask])
            all_true.extend(y[test_mask].tolist())
            all_pred.extend(pipe.predict(X[test_mask]).tolist())
            # class 1 = outbreak probability; bust = 1 - this
            all_prob.extend(pipe.predict_proba(X[test_mask])[:, 1].tolist())

        if not all_true:
            return {"error": "No LOYO folds produced predictions"}

        all_true_arr = np.array(all_true)
        all_prob_arr = np.array(all_prob)
        bust_prob_arr = 1.0 - all_prob_arr
        y_bust = (all_true_arr == 0).astype(int)

        prec, rec, thresholds = precision_recall_curve(y_bust, bust_prob_arr)
        f1 = np.where(
            (prec[:-1] + rec[:-1]) > 0,
            2 * prec[:-1] * rec[:-1] / (prec[:-1] + rec[:-1]),
            0.0,
        )
        best_idx = int(np.argmax(f1))
        tuned_thr = float(round(thresholds[best_idx], 3))
        # Apply tuned threshold on bust probability
        all_pred_tuned = np.where(bust_prob_arr >= tuned_thr, 0, 1).tolist()

        return {
            "loyo_accuracy":  round(accuracy_score(all_true, all_pred), 3),
            "loyo_roc_auc":   round(roc_auc_score(all_true_arr, all_prob_arr), 3),
            "n_folds":        len(set(years)) - len(skipped_years),
            "n_predictions":  len(all_true),
            "tuned_threshold": tuned_thr,
            "classification_report": classification_report(
                all_true, all_pred,
                target_names=["NULL_BUST", "OUTBREAK"],
            ),
            "classification_report_tuned": classification_report(
                all_true, all_pred_tuned,
                target_names=["NULL_BUST", "OUTBREAK"],
            ),
        }
