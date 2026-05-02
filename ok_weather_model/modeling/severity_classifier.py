"""
Severity classifier: predicts SIGNIFICANT_OUTBREAK vs WEAK_OUTBREAK from
the 12Z pre-convective environment.

Model: RandomForestClassifier with balanced class weights and median
imputation for missing optional features (LFC_height, EML_depth, EHI, STP,
SCP, LLJ_speed).

Evaluation uses leave-one-year-out (LOYO) cross-validation to respect
temporal autocorrelation in the training data.
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

_CLASSES = {0: "WEAK_OUTBREAK", 1: "SIGNIFICANT_OUTBREAK"}


def _make_pipeline():
    from sklearn.pipeline import Pipeline
    from sklearn.impute import SimpleImputer
    from sklearn.ensemble import RandomForestClassifier

    return Pipeline([
        ("imputer", SimpleImputer(strategy="median", keep_empty_features=True)),
        ("clf", RandomForestClassifier(
            n_estimators=300,
            class_weight="balanced",
            max_features="sqrt",
            min_samples_leaf=3,
            random_state=42,
            n_jobs=-1,
        )),
    ])


class SeverityClassifier:
    """
    Binary classifier: SIGNIFICANT_OUTBREAK (1) vs WEAK_OUTBREAK (0).

    Usage::

        clf = SeverityClassifier()
        metrics = clf.train(cases)          # fits + calibrates threshold_
        probs = clf.predict_proba(indices, kinematics, ctg)
        label = clf.predict(indices, kinematics, ctg)
        # {'significant': 0.72, 'weak': 0.28}  /  'SIGNIFICANT_OUTBREAK'

    threshold_ is the decision boundary calibrated during training via 5-fold
    cross-validation to maximise F1 on the minority SIGNIFICANT_OUTBREAK class.
    Default 0.5 before train() is called.
    """

    def __init__(self):
        self._pipeline = None
        self.feature_importances_: Optional[pd.Series] = None
        self.n_training_cases_: int = 0
        self.threshold_: float = 0.5

    # ── Training ──────────────────────────────────────────────────────────────

    def train(self, cases: list[HistoricalCase]) -> dict:
        """
        Fit on all provided cases. Returns summary training metrics (optimistic —
        use evaluate() for honest leave-one-year-out performance).

        Also calibrates threshold_ via 5-fold cross-validation by maximising
        F1 on the minority SIGNIFICANT_OUTBREAK class.
        """
        from sklearn.model_selection import cross_val_predict
        from sklearn.metrics import precision_recall_curve

        X, y = build_feature_matrix(cases, target="is_significant")
        if len(X) < 10:
            raise ValueError(f"Need at least 10 cases to train, got {len(X)}")

        self._pipeline = _make_pipeline()
        self._pipeline.fit(X, y)
        self.n_training_cases_ = len(X)

        clf = self._pipeline.named_steps["clf"]
        self.feature_importances_ = pd.Series(
            clf.feature_importances_, index=FEATURE_NAMES
        ).sort_values(ascending=False)

        # Calibrate decision threshold via 5-fold CV to maximise F1 on SIGNIFICANT
        cv_probs = cross_val_predict(
            _make_pipeline(), X, y, cv=5, method="predict_proba", n_jobs=-1
        )[:, 1]
        prec, rec, thresholds = precision_recall_curve(y, cv_probs)
        f1 = np.where(
            (prec[:-1] + rec[:-1]) > 0,
            2 * prec[:-1] * rec[:-1] / (prec[:-1] + rec[:-1]),
            0.0,
        )
        best_idx = int(np.argmax(f1))
        self.threshold_ = float(round(thresholds[best_idx], 3))

        preds = self._pipeline.predict(X)
        return {
            "n_cases": len(X),
            "n_significant": int(y.sum()),
            "n_weak": int((y == 0).sum()),
            "train_accuracy": round(float((preds == y).mean()), 3),
            "positive_rate": round(float(y.mean()), 3),
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

        The Mesonet moisture parameters are optional; when provided they
        improve prediction by incorporating the current return flow state.

        Returns dict: {'significant': 0.0–1.0, 'weak': 0.0–1.0}
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
            "significant": round(float(prob_map.get(1, 0.0)), 3),
            "weak":        round(float(prob_map.get(0, 0.0)), 3),
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
        """Return 'SIGNIFICANT_OUTBREAK' or 'WEAK_OUTBREAK' using threshold_."""
        probs = self.predict_proba(
            indices, kinematics, convective_temp_gap,
            surface_dewpoint_f, moisture_return_gradient_f,
            gulf_moisture_fraction, modified_MLCAPE, modified_MLCIN,
        )
        return (
            "SIGNIFICANT_OUTBREAK"
            if probs["significant"] >= self.threshold_
            else "WEAK_OUTBREAK"
        )

    # ── Evaluation ────────────────────────────────────────────────────────────

    def evaluate(self, cases: list[HistoricalCase]) -> dict:
        """
        Leave-one-year-out cross-validation.

        Returns accuracy, ROC-AUC, and classification reports at both the
        default 0.5 threshold and the F1-optimised threshold (tuned_threshold).
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
            if case.event_class.value not in {"SIGNIFICANT_OUTBREAK", "WEAK_OUTBREAK"}:
                continue
            rows.append(feat)
            targets.append(1 if case.event_class.value == "SIGNIFICANT_OUTBREAK" else 0)
            years.append(case.date.year)

        if len(rows) < 10:
            return {"error": "Insufficient data for LOYO evaluation"}

        X = pd.DataFrame(rows, columns=FEATURE_NAMES)
        y = np.array(targets)
        years_arr = np.array(years)

        all_true, all_pred, all_prob = [], [], []
        skipped_years = []
        for yr in sorted(set(years)):
            train_mask = years_arr != yr
            test_mask = years_arr == yr
            if train_mask.sum() < 10:
                skipped_years.append(yr)
                continue

            pipe = _make_pipeline()
            pipe.fit(X[train_mask], y[train_mask])
            all_true.extend(y[test_mask].tolist())
            all_pred.extend(pipe.predict(X[test_mask]).tolist())
            all_prob.extend(pipe.predict_proba(X[test_mask])[:, 1].tolist())

        if not all_true:
            return {"error": "No LOYO folds produced predictions"}

        all_true_arr = np.array(all_true)
        all_prob_arr = np.array(all_prob)

        # Find OOF-optimal threshold
        prec, rec, thresholds = precision_recall_curve(all_true_arr, all_prob_arr)
        f1 = np.where(
            (prec[:-1] + rec[:-1]) > 0,
            2 * prec[:-1] * rec[:-1] / (prec[:-1] + rec[:-1]),
            0.0,
        )
        best_idx = int(np.argmax(f1))
        tuned_thr = float(round(thresholds[best_idx], 3))
        all_pred_tuned = (all_prob_arr >= tuned_thr).astype(int).tolist()

        return {
            "loyo_accuracy": round(accuracy_score(all_true, all_pred), 3),
            "loyo_roc_auc":  round(roc_auc_score(all_true_arr, all_prob_arr), 3),
            "n_folds": len(set(years)) - len(skipped_years),
            "n_predictions": len(all_true),
            "tuned_threshold": tuned_thr,
            "classification_report": classification_report(
                all_true, all_pred,
                target_names=["WEAK_OUTBREAK", "SIGNIFICANT_OUTBREAK"],
            ),
            "classification_report_tuned": classification_report(
                all_true, all_pred_tuned,
                target_names=["WEAK_OUTBREAK", "SIGNIFICANT_OUTBREAK"],
            ),
        }
