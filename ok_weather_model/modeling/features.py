"""
Feature extraction for KRONOS-WX forecast models.

Builds a flat numeric feature vector from sounding-derived indices and
kinematic profiles.  Missing optional fields are represented as NaN so that
downstream estimators can apply imputation (scikit-learn SimpleImputer).

Two entry points:
    extract_features(case)               — training path (HistoricalCase)
    extract_features_from_indices(...)   — inference path (live sounding objects)
"""
from __future__ import annotations

import math
from typing import Optional

import pandas as pd

from ..models.sounding import ThermodynamicIndices
from ..models.kinematic import KinematicProfile
from ..models.case import HistoricalCase


# Canonical feature order — model artifacts depend on this order being stable.
FEATURE_NAMES: list[str] = [
    # Thermodynamic — instability
    "MLCAPE",
    "MLCIN",
    "SBCAPE",
    "SBCIN",
    "MUCAPE",
    # Parcel trajectory / cap
    "LCL_height",
    "LFC_height",        # Optional — NaN when cap never breaks
    "cap_strength",
    # EML
    "EML_depth",         # Optional — NaN when EML absent
    # Lapse rates / moisture
    "lapse_rate_700_500",
    "lapse_rate_850_500",
    "precipitable_water",
    "wet_bulb_zero",
    # Kinematics
    "SRH_0_1km",
    "SRH_0_3km",
    "BWD_0_1km",
    "BWD_0_6km",
    "EHI",               # Optional
    "STP",               # Optional
    "SCP",               # Optional
    "LLJ_speed",         # Optional
    "mean_wind_0_6km",
    # Case-level derived
    "convective_temp_gap_12Z",  # Optional — NaN for real-time without Tc
    # Surface moisture (Mesonet) — return flow diagnostics
    "surface_dewpoint_f",           # Optional — current statewide mean Td (°F)
    "moisture_return_gradient_f",   # Optional — south OK minus north OK Td (°F)
    "gulf_moisture_fraction",       # Optional — fraction of stations with Td ≥ 60°F
    # Daytime modified CAPE (12Z sounding aloft + Mesonet surface Td)
    "modified_MLCAPE",              # Optional — afternoon MLCAPE with current surface moisture
    "modified_MLCIN",               # Optional — afternoon MLCIN with current surface moisture
]


def extract_features_from_indices(
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
    Build a feature dict from pre-computed sounding indices.

    The Mesonet moisture parameters are optional — they are NaN when not
    available (historical training) and populated for real-time inference
    via compute_moisture_return() and compute_modified_indices().

    Missing optional fields become NaN for downstream imputation.
    """
    def _f(v) -> float:
        return float(v) if v is not None else float("nan")

    return {
        "MLCAPE":                       float(indices.MLCAPE),
        "MLCIN":                        float(indices.MLCIN),
        "SBCAPE":                       float(indices.SBCAPE),
        "SBCIN":                        float(indices.SBCIN),
        "MUCAPE":                       float(indices.MUCAPE),
        "LCL_height":                   float(indices.LCL_height),
        "LFC_height":                   _f(indices.LFC_height),
        "cap_strength":                 float(indices.cap_strength),
        "EML_depth":                    _f(indices.EML_depth),
        "lapse_rate_700_500":           float(indices.lapse_rate_700_500),
        "lapse_rate_850_500":           float(indices.lapse_rate_850_500),
        "precipitable_water":           float(indices.precipitable_water),
        "wet_bulb_zero":                float(indices.wet_bulb_zero),
        "SRH_0_1km":                    float(kinematics.SRH_0_1km),
        "SRH_0_3km":                    float(kinematics.SRH_0_3km),
        "BWD_0_1km":                    float(kinematics.BWD_0_1km),
        "BWD_0_6km":                    float(kinematics.BWD_0_6km),
        "EHI":                          _f(kinematics.EHI),
        "STP":                          _f(kinematics.STP),
        "SCP":                          _f(kinematics.SCP),
        "LLJ_speed":                    _f(kinematics.LLJ_speed),
        "mean_wind_0_6km":              float(kinematics.mean_wind_0_6km),
        "convective_temp_gap_12Z":      _f(convective_temp_gap),
        "surface_dewpoint_f":           _f(surface_dewpoint_f),
        "moisture_return_gradient_f":   _f(moisture_return_gradient_f),
        "gulf_moisture_fraction":       _f(gulf_moisture_fraction),
        "modified_MLCAPE":              _f(modified_MLCAPE),
        "modified_MLCIN":               _f(modified_MLCIN),
    }


def extract_features(case: HistoricalCase) -> Optional[dict[str, float]]:
    """
    Extract features from a HistoricalCase.
    Returns None if sounding_12Z or kinematics_12Z is missing.

    Mesonet moisture features (surface_dewpoint_f, moisture_return_gradient_f,
    modified_MLCAPE, etc.) are NaN in the training path — historical cases
    don't have a stored Mesonet snapshot linked.  These features only have
    values during real-time inference via extract_features_from_indices().
    """
    if case.sounding_12Z is None or case.kinematics_12Z is None:
        return None
    return extract_features_from_indices(
        case.sounding_12Z,
        case.kinematics_12Z,
        case.convective_temp_gap_12Z,
    )


def build_feature_matrix(
    cases: list[HistoricalCase],
    target: str = "event_class",
) -> tuple[pd.DataFrame, pd.Series]:
    """
    Build (X, y) from a list of HistoricalCase objects.

    target options
    --------------
    "event_class"       raw EventClass value string (multi-class)
    "is_significant"    1 = SIGNIFICANT_OUTBREAK, 0 = WEAK_OUTBREAK
                        (cases outside those two classes are excluded)
    "tornado_count"     raw integer count
    "log_tornado_count" log1p(tornado_count) — reduces right-tail skew

    X has NaN for missing optional features; apply SimpleImputer before fitting.
    """
    rows: list[dict] = []
    targets: list = []

    for case in cases:
        feat = extract_features(case)
        if feat is None:
            continue

        if target == "event_class":
            t = case.event_class.value

        elif target == "is_significant":
            if case.event_class.value not in {"SIGNIFICANT_OUTBREAK", "WEAK_OUTBREAK"}:
                continue
            t = 1 if case.event_class.value == "SIGNIFICANT_OUTBREAK" else 0

        elif target == "tornado_count":
            t = case.tornado_count

        elif target == "log_tornado_count":
            t = math.log1p(case.tornado_count)

        else:
            raise ValueError(f"Unknown target: {target!r}")

        rows.append(feat)
        targets.append(t)

    X = pd.DataFrame(rows, columns=FEATURE_NAMES)
    y = pd.Series(targets, name=target)
    return X, y
