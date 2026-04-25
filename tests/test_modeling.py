"""
Tests for the ok_weather_model.modeling package.

Uses synthetic HistoricalCase objects so tests run without touching the
live database or requiring Wyoming / Mesonet network access.
"""
from __future__ import annotations

import math
from datetime import date, datetime, timezone
from typing import Optional

import pandas as pd
import pytest

from ok_weather_model.models.enums import (
    EventClass,
    OklahomaSoundingStation,
    HodographShape,
)
from ok_weather_model.models.sounding import ThermodynamicIndices
from ok_weather_model.models.kinematic import KinematicProfile, WindLevel
from ok_weather_model.models.case import HistoricalCase
from ok_weather_model.modeling.features import (
    FEATURE_NAMES,
    extract_features,
    extract_features_from_indices,
    build_feature_matrix,
)
from ok_weather_model.modeling.severity_classifier import SeverityClassifier
from ok_weather_model.modeling.tornado_regressor import TornadoRegressor


# ── Fixtures ──────────────────────────────────────────────────────────────────

_VALID_TIME = datetime(2000, 5, 3, 12, 0, tzinfo=timezone.utc)


def _make_indices(
    MLCAPE: float = 2000.0,
    MLCIN: float = 50.0,
    SBCAPE: float = 2500.0,
    SBCIN: float = 30.0,
    MUCAPE: float = 3000.0,
    LFC_height: Optional[float] = 1200.0,
    cap_strength: float = 2.0,
    EML_depth: Optional[float] = 200.0,
) -> ThermodynamicIndices:
    return ThermodynamicIndices(
        valid_time=_VALID_TIME,
        station=OklahomaSoundingStation.OUN,
        MLCAPE=MLCAPE,
        MLCIN=MLCIN,
        SBCAPE=SBCAPE,
        SBCIN=SBCIN,
        MUCAPE=MUCAPE,
        LCL_height=800.0,
        LFC_height=LFC_height,
        EL_height=12000.0,
        convective_temperature=90.0,
        cap_strength=cap_strength,
        EML_base=700.0 if EML_depth else None,
        EML_top=500.0 if EML_depth else None,
        EML_depth=EML_depth,
        lapse_rate_700_500=7.5,
        lapse_rate_850_500=7.0,
        precipitable_water=1.2,
        mixing_ratio_850=12.0,
        wet_bulb_zero=3000.0,
    )


def _make_kinematics(
    SRH_0_1km: float = 200.0,
    SRH_0_3km: float = 350.0,
    BWD_0_6km: float = 50.0,
    EHI: Optional[float] = 2.5,
    STP: Optional[float] = 3.0,
    SCP: Optional[float] = 5.0,
    LLJ_speed: Optional[float] = 40.0,
) -> KinematicProfile:
    levels = [WindLevel(pressure=850.0, height=1500.0, u_component=10.0, v_component=5.0)]
    return KinematicProfile(
        valid_time=_VALID_TIME,
        station=OklahomaSoundingStation.OUN,
        levels=levels,
        SRH_0_1km=SRH_0_1km,
        SRH_0_3km=SRH_0_3km,
        SRH_effective=None,
        BWD_0_1km=20.0,
        BWD_0_6km=BWD_0_6km,
        BWD_effective=None,
        LLJ_speed=LLJ_speed,
        LLJ_direction=180.0 if LLJ_speed else None,
        mean_wind_0_6km=30.0,
        hodograph_shape=HodographShape.CURVED,
        storm_motion_bunkers_right=(10.0, 5.0),
        storm_motion_bunkers_left=(-5.0, 15.0),
        EHI=EHI,
        STP=STP,
        SCP=SCP,
    )


def _make_case(
    event_class: EventClass = EventClass.SIGNIFICANT_OUTBREAK,
    tornado_count: int = 15,
    case_date: date = date(2000, 5, 3),
    MLCAPE: float = 2000.0,
    MLCIN: float = 50.0,
    SRH_0_3km: float = 350.0,
    convective_temp_gap: Optional[float] = 5.0,
) -> HistoricalCase:
    indices = _make_indices(MLCAPE=MLCAPE, MLCIN=MLCIN)
    kin = _make_kinematics(SRH_0_3km=SRH_0_3km)
    return HistoricalCase(
        case_id=HistoricalCase.make_case_id(case_date),
        date=case_date,
        event_class=event_class,
        tornado_count=tornado_count,
        sounding_12Z=indices,
        kinematics_12Z=kin,
        convective_temp_gap_12Z=convective_temp_gap,
        sounding_data_available=True,
    )


def _make_synthetic_cases(
    n_significant: int = 20,
    n_weak: int = 60,
) -> list[HistoricalCase]:
    """Build a synthetic case library with two distinct clusters."""
    cases = []
    base = date(2000, 4, 1)
    from datetime import timedelta

    for i in range(n_significant):
        d = base + timedelta(days=i)
        cases.append(_make_case(
            event_class=EventClass.SIGNIFICANT_OUTBREAK,
            tornado_count=15 + i % 10,
            case_date=date(2000 + i % 20, 4, 1 + i % 28),
            MLCAPE=3000.0 + i * 50,
            SRH_0_3km=400.0 + i * 5,
        ))

    for i in range(n_weak):
        d = base + timedelta(days=i + 100)
        cases.append(_make_case(
            event_class=EventClass.WEAK_OUTBREAK,
            tornado_count=2 + i % 3,
            case_date=date(2000 + i % 20, 5, 1 + i % 28),
            MLCAPE=1000.0 + i * 10,
            SRH_0_3km=150.0 + i * 2,
        ))

    return cases


# ── Feature extraction tests ─────────────────────────────────────────────────

class TestExtractFeatures:
    def test_returns_none_without_sounding(self):
        case = HistoricalCase(
            case_id="20000503_OK",
            date=date(2000, 5, 3),
            event_class=EventClass.WEAK_OUTBREAK,
        )
        assert extract_features(case) is None

    def test_returns_none_without_kinematics(self):
        case = HistoricalCase(
            case_id="20000503_OK",
            date=date(2000, 5, 3),
            event_class=EventClass.WEAK_OUTBREAK,
            sounding_12Z=_make_indices(),
        )
        assert extract_features(case) is None

    def test_returns_all_feature_names(self):
        case = _make_case()
        feat = extract_features(case)
        assert feat is not None
        assert set(feat.keys()) == set(FEATURE_NAMES)
        assert len(feat) == len(FEATURE_NAMES)

    def test_nan_for_optional_fields_when_absent(self):
        indices = _make_indices(LFC_height=None, EML_depth=None)
        kin = _make_kinematics(EHI=None, STP=None, SCP=None, LLJ_speed=None)
        feat = extract_features_from_indices(indices, kin, convective_temp_gap=None)

        assert math.isnan(feat["LFC_height"])
        assert math.isnan(feat["EML_depth"])
        assert math.isnan(feat["EHI"])
        assert math.isnan(feat["STP"])
        assert math.isnan(feat["SCP"])
        assert math.isnan(feat["LLJ_speed"])
        assert math.isnan(feat["convective_temp_gap_12Z"])

    def test_concrete_values_roundtrip(self):
        indices = _make_indices(MLCAPE=2500.0, MLCIN=75.0, cap_strength=3.5)
        kin = _make_kinematics(SRH_0_1km=250.0, BWD_0_6km=55.0)
        feat = extract_features_from_indices(indices, kin, convective_temp_gap=8.0)

        assert feat["MLCAPE"] == pytest.approx(2500.0)
        assert feat["MLCIN"] == pytest.approx(75.0)
        assert feat["cap_strength"] == pytest.approx(3.5)
        assert feat["SRH_0_1km"] == pytest.approx(250.0)
        assert feat["BWD_0_6km"] == pytest.approx(55.0)
        assert feat["convective_temp_gap_12Z"] == pytest.approx(8.0)


class TestBuildFeatureMatrix:
    def test_excludes_cases_without_sounding(self):
        cases = [
            _make_case(),
            HistoricalCase(
                case_id="20010101_OK",
                date=date(2001, 1, 1),
                event_class=EventClass.WEAK_OUTBREAK,
            ),
        ]
        X, y = build_feature_matrix(cases, target="tornado_count")
        assert len(X) == 1

    def test_is_significant_excludes_isolated(self):
        cases = [
            _make_case(event_class=EventClass.SIGNIFICANT_OUTBREAK),
            _make_case(event_class=EventClass.WEAK_OUTBREAK, tornado_count=2,
                       case_date=date(2001, 5, 3)),
            _make_case(event_class=EventClass.ISOLATED_SIGNIFICANT, tornado_count=1,
                       case_date=date(2002, 5, 3)),
        ]
        X, y = build_feature_matrix(cases, target="is_significant")
        assert len(X) == 2  # ISOLATED excluded
        assert set(y.tolist()) == {0, 1}

    def test_log_tornado_count_target(self):
        case = _make_case(tornado_count=4)
        X, y = build_feature_matrix([case], target="log_tornado_count")
        assert y.iloc[0] == pytest.approx(math.log1p(4))

    def test_shape_matches_feature_names(self):
        cases = _make_synthetic_cases(n_significant=5, n_weak=10)
        X, y = build_feature_matrix(cases, target="is_significant")
        assert X.shape[1] == len(FEATURE_NAMES)
        assert list(X.columns) == FEATURE_NAMES

    def test_unknown_target_raises(self):
        cases = [_make_case()]
        with pytest.raises(ValueError, match="Unknown target"):
            build_feature_matrix(cases, target="invalid_target")


# ── Severity classifier tests ─────────────────────────────────────────────────

class TestSeverityClassifier:
    def test_train_returns_expected_keys(self):
        cases = _make_synthetic_cases()
        clf = SeverityClassifier()
        metrics = clf.train(cases)
        assert "n_cases" in metrics
        assert "n_significant" in metrics
        assert "n_weak" in metrics
        assert "train_accuracy" in metrics
        assert 0.0 <= metrics["train_accuracy"] <= 1.0

    def test_predict_proba_sums_to_one(self):
        cases = _make_synthetic_cases()
        clf = SeverityClassifier()
        clf.train(cases)
        indices = _make_indices()
        kin = _make_kinematics()
        result = clf.predict_proba(indices, kin, convective_temp_gap=5.0)
        assert "significant" in result
        assert "weak" in result
        assert result["significant"] + result["weak"] == pytest.approx(1.0, abs=1e-3)

    def test_high_cape_shear_predicts_significant(self):
        """High CAPE + strong shear should lean toward SIGNIFICANT."""
        cases = _make_synthetic_cases(n_significant=30, n_weak=70)
        clf = SeverityClassifier()
        clf.train(cases)

        high_env = clf.predict_proba(
            _make_indices(MLCAPE=4000.0),
            _make_kinematics(SRH_0_3km=500.0, BWD_0_6km=60.0),
        )
        low_env = clf.predict_proba(
            _make_indices(MLCAPE=500.0),
            _make_kinematics(SRH_0_3km=100.0, BWD_0_6km=25.0),
        )
        assert high_env["significant"] > low_env["significant"]

    def test_predict_proba_without_training_raises(self):
        clf = SeverityClassifier()
        with pytest.raises(RuntimeError, match="not trained"):
            clf.predict_proba(_make_indices(), _make_kinematics())

    def test_feature_importances_after_training(self):
        cases = _make_synthetic_cases()
        clf = SeverityClassifier()
        clf.train(cases)
        assert clf.feature_importances_ is not None
        assert len(clf.feature_importances_) == len(FEATURE_NAMES)
        assert clf.feature_importances_.sum() == pytest.approx(1.0, abs=1e-3)

    def test_train_requires_minimum_cases(self):
        clf = SeverityClassifier()
        with pytest.raises(ValueError, match="at least 10"):
            clf.train(_make_synthetic_cases(n_significant=1, n_weak=2))


# ── Tornado regressor tests ───────────────────────────────────────────────────

class TestTornadoRegressor:
    def test_train_returns_expected_keys(self):
        cases = _make_synthetic_cases()
        reg = TornadoRegressor()
        metrics = reg.train(cases)
        assert "n_cases" in metrics
        assert "train_rmse" in metrics
        assert "train_mae" in metrics
        assert metrics["train_rmse"] >= 0
        assert metrics["train_mae"] >= 0

    def test_predict_returns_nonneg_count(self):
        cases = _make_synthetic_cases()
        reg = TornadoRegressor()
        reg.train(cases)
        result = reg.predict(_make_indices(), _make_kinematics())
        assert result["expected_count"] >= 0
        assert result["interval_low"] >= 0
        assert result["interval_high"] >= result["interval_low"]

    def test_high_cape_predicts_more_tornadoes(self):
        """Stronger environment should predict more tornadoes."""
        cases = _make_synthetic_cases(n_significant=30, n_weak=70)
        reg = TornadoRegressor()
        reg.train(cases)

        high = reg.predict(
            _make_indices(MLCAPE=4000.0),
            _make_kinematics(SRH_0_3km=500.0),
        )
        low = reg.predict(
            _make_indices(MLCAPE=300.0),
            _make_kinematics(SRH_0_3km=80.0),
        )
        assert high["expected_count"] > low["expected_count"]

    def test_predict_without_training_raises(self):
        reg = TornadoRegressor()
        with pytest.raises(RuntimeError, match="not trained"):
            reg.predict(_make_indices(), _make_kinematics())

    def test_feature_importances_after_training(self):
        cases = _make_synthetic_cases()
        reg = TornadoRegressor()
        reg.train(cases)
        assert reg.feature_importances_ is not None
        assert len(reg.feature_importances_) == len(FEATURE_NAMES)

    def test_interval_wider_than_point_estimate(self):
        """80% PI must bracket the point estimate."""
        cases = _make_synthetic_cases()
        reg = TornadoRegressor()
        reg.train(cases)
        result = reg.predict(_make_indices(), _make_kinematics())
        assert result["interval_low"] <= result["expected_count"]
        assert result["interval_high"] >= result["expected_count"]
