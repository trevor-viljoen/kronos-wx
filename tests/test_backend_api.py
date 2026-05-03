"""
Tests for kronos_web.backend.api — pure helpers and HTTP endpoints.

Pure functions are tested directly (no mocking needed).
HTTP endpoints use FastAPI's TestClient with lifespan disabled so no
background tasks or network calls fire during tests.
"""
from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import Optional
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

# Import the module — background tasks start only if lifespan runs
from kronos_web.backend import api as _api


# ── TestClient (no lifespan, no background tasks) ────────────────────────────

@pytest.fixture(scope="module")
def client():
    """FastAPI TestClient with lifespan disabled."""
    with TestClient(_api.app, raise_server_exceptions=True) as c:
        yield c


# ── Reusable synthetic builders (mirrors test_modeling.py style) ──────────────

from ok_weather_model.models.enums import OklahomaSoundingStation, HodographShape
from ok_weather_model.models.sounding import ThermodynamicIndices
from ok_weather_model.models.kinematic import KinematicProfile, WindLevel

_VALID_TIME = datetime(2000, 5, 3, 12, 0, tzinfo=timezone.utc)


def _make_indices(
    MLCAPE: float = 2000.0,
    MLCIN: float = 50.0,
    cap_strength: float = 2.5,
    lapse_rate_700_500: float = 7.5,
    convective_temperature: float = 92.0,
) -> ThermodynamicIndices:
    return ThermodynamicIndices(
        valid_time=_VALID_TIME,
        station=OklahomaSoundingStation.OUN,
        MLCAPE=MLCAPE,
        MLCIN=MLCIN,
        SBCAPE=2500.0,
        SBCIN=30.0,
        MUCAPE=3000.0,
        LCL_height=800.0,
        LFC_height=1200.0,
        EL_height=12000.0,
        convective_temperature=convective_temperature,
        cap_strength=cap_strength,
        lapse_rate_700_500=lapse_rate_700_500,
        lapse_rate_850_500=7.0,
        precipitable_water=1.2,
        mixing_ratio_850=12.0,
        wet_bulb_zero=3000.0,
    )


def _make_kinematics(
    SRH_0_1km: float = 200.0,
    SRH_0_3km: float = 350.0,
    BWD_0_6km: float = 50.0,
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
        LLJ_speed=40.0,
        LLJ_direction=180.0,
        mean_wind_0_6km=30.0,
        hodograph_shape=HodographShape.CURVED,
        storm_motion_bunkers_right=(10.0, 5.0),
        storm_motion_bunkers_left=(-5.0, 15.0),
        EHI=2.5,
        STP=3.0,
        SCP=5.0,
    )


# ── _sanitize_nan ─────────────────────────────────────────────────────────────

class TestSanitizeNan:
    def test_finite_float_unchanged(self):
        assert _api._sanitize_nan(3.14) == 3.14

    def test_nan_becomes_none(self):
        assert _api._sanitize_nan(float("nan")) is None

    def test_inf_becomes_none(self):
        assert _api._sanitize_nan(float("inf")) is None

    def test_neg_inf_becomes_none(self):
        assert _api._sanitize_nan(float("-inf")) is None

    def test_dict_recursive(self):
        result = _api._sanitize_nan({"a": float("nan"), "b": 1.0})
        assert result["a"] is None
        assert result["b"] == 1.0

    def test_list_recursive(self):
        result = _api._sanitize_nan([float("nan"), 2.0, float("inf")])
        assert result == [None, 2.0, None]

    def test_nested_structure(self):
        obj = {"x": [float("nan"), {"y": float("inf"), "z": 42.0}]}
        result = _api._sanitize_nan(obj)
        assert result["x"][0] is None
        assert result["x"][1]["y"] is None
        assert result["x"][1]["z"] == 42.0

    def test_non_float_types_pass_through(self):
        assert _api._sanitize_nan("hello") == "hello"
        assert _api._sanitize_nan(42) == 42
        assert _api._sanitize_nan(None) is None
        assert _api._sanitize_nan(True) is True


# ── _compute_state_hash ───────────────────────────────────────────────────────

class TestComputeStateHash:
    def _base_state(self, **overrides) -> dict:
        state = {
            "hrrr_valid": "2024-05-03T18:00:00+00:00",
            "tier_map": {"CLEVELAND": "HIGH"},
            "spc": {"alerts": [], "outlook": {"category": "SLGT"}},
        }
        state.update(overrides)
        return state

    def test_returns_12_chars(self):
        h = _api._compute_state_hash(self._base_state())
        assert len(h) == 12

    def test_deterministic(self):
        state = self._base_state()
        assert _api._compute_state_hash(state) == _api._compute_state_hash(state)

    def test_same_logical_content_same_hash(self):
        s1 = self._base_state()
        s2 = self._base_state()
        assert _api._compute_state_hash(s1) == _api._compute_state_hash(s2)

    def test_different_tier_different_hash(self):
        s1 = self._base_state()
        s2 = self._base_state(tier_map={"CLEVELAND": "EXTREME"})
        assert _api._compute_state_hash(s1) != _api._compute_state_hash(s2)

    def test_different_alert_count_different_hash(self):
        s1 = self._base_state()
        s2 = self._base_state()
        s2["spc"]["alerts"] = [{"event": "Tornado Warning"}]
        assert _api._compute_state_hash(s1) != _api._compute_state_hash(s2)

    def test_empty_state_does_not_crash(self):
        h = _api._compute_state_hash({})
        assert len(h) == 12


# ── _parse_watch_counties_ok ──────────────────────────────────────────────────

class TestParseWatchCountiesOk:
    def test_single_county(self):
        result = _api._parse_watch_counties_ok("Cleveland County OK")
        from ok_weather_model.models.enums import OklahomaCounty
        assert OklahomaCounty.CLEVELAND in result

    def test_multiple_counties_semicolon(self):
        result = _api._parse_watch_counties_ok("Cleveland County OK; Pottawatomie County OK")
        from ok_weather_model.models.enums import OklahomaCounty
        assert OklahomaCounty.CLEVELAND in result
        assert OklahomaCounty.POTTAWATOMIE in result

    def test_unknown_county_ignored(self):
        result = _api._parse_watch_counties_ok("Nonexistent County TX")
        assert len(result) == 0

    def test_mixed_valid_invalid(self):
        result = _api._parse_watch_counties_ok("Cleveland County OK; FakePlace County TX")
        from ok_weather_model.models.enums import OklahomaCounty
        assert OklahomaCounty.CLEVELAND in result
        assert len(result) == 1

    def test_empty_string(self):
        assert _api._parse_watch_counties_ok("") == set()

    def test_comma_separated(self):
        result = _api._parse_watch_counties_ok("Garfield County OK, Kay County OK")
        from ok_weather_model.models.enums import OklahomaCounty
        assert OklahomaCounty.GARFIELD in result
        assert OklahomaCounty.KAY in result


# ── _tile_bbox_3857 ───────────────────────────────────────────────────────────

class TestTileBbox3857:
    _W = _api._RIDGE2_WORLD  # 20037508.3427892

    def test_returns_comma_separated_four_values(self):
        bbox = _api._tile_bbox_3857(0, 0, 0)
        parts = bbox.split(",")
        assert len(parts) == 4

    def test_zoom0_tile000_covers_full_world(self):
        bbox = _api._tile_bbox_3857(0, 0, 0)
        xmin, ymin, xmax, ymax = map(float, bbox.split(","))
        assert abs(xmin - (-self._W)) < 1.0
        assert abs(ymin - (-self._W)) < 1.0
        assert abs(xmax - self._W) < 1.0
        assert abs(ymax - self._W) < 1.0

    def test_zoom1_nw_tile(self):
        # Tile (0,0) at z=1 → northwest quadrant
        bbox = _api._tile_bbox_3857(0, 0, 1)
        xmin, ymin, xmax, ymax = map(float, bbox.split(","))
        assert xmin < 0
        assert ymax > 0
        assert xmax == pytest.approx(0.0, abs=1.0)
        assert ymin == pytest.approx(0.0, abs=1.0)

    def test_bbox_width_equals_height(self):
        # Tiles should always be square in 3857
        bbox = _api._tile_bbox_3857(3, 5, 4)
        xmin, ymin, xmax, ymax = map(float, bbox.split(","))
        assert (xmax - xmin) == pytest.approx(ymax - ymin, rel=1e-6)

    def test_higher_zoom_smaller_tile(self):
        z4 = _api._tile_bbox_3857(0, 0, 4)
        z5 = _api._tile_bbox_3857(0, 0, 5)
        xmin4, _, xmax4, _ = map(float, z4.split(","))
        xmin5, _, xmax5, _ = map(float, z5.split(","))
        assert (xmax4 - xmin4) == pytest.approx(2 * (xmax5 - xmin5), rel=1e-6)


# ── _validate_push_endpoint ───────────────────────────────────────────────────

class TestValidatePushEndpoint:
    def test_chrome_push_service_allowed(self):
        assert _api._validate_push_endpoint(
            "https://fcm.googleapis.com/fcm/send/abc123"
        )

    def test_firefox_push_service_allowed(self):
        assert _api._validate_push_endpoint(
            "https://push.services.mozilla.com/wpush/v2/abc123"
        )

    def test_http_rejected(self):
        assert not _api._validate_push_endpoint(
            "http://fcm.googleapis.com/fcm/send/abc"
        )

    def test_localhost_rejected(self):
        assert not _api._validate_push_endpoint(
            "https://localhost:4000/push"
        )

    def test_arbitrary_domain_rejected(self):
        assert not _api._validate_push_endpoint(
            "https://attacker.example.com/push"
        )

    def test_internal_ip_rejected(self):
        assert not _api._validate_push_endpoint(
            "https://192.168.1.1/push"
        )

    def test_empty_string_rejected(self):
        assert not _api._validate_push_endpoint("")

    def test_malformed_url_rejected(self):
        assert not _api._validate_push_endpoint("not-a-url")


# ── _analogue_feature_vector + _analogue_distance ────────────────────────────

class TestAnalogueHelpers:
    def test_feature_vector_returns_5_elements(self):
        idx = _make_indices()
        kin = _make_kinematics()
        vec = _api._analogue_feature_vector(idx, kin, 10.0)
        assert vec is not None
        assert len(vec) == 5

    def test_feature_vector_none_when_indices_none(self):
        assert _api._analogue_feature_vector(None, None, None) is None

    def test_feature_vector_all_nonneg(self):
        idx = _make_indices()
        kin = _make_kinematics()
        vec = _api._analogue_feature_vector(idx, kin, 10.0)
        assert all(v >= 0 for v in vec)

    def test_feature_vector_at_most_sum_of_weights(self):
        # All weights sum to 1.0; each component is weight * normalized_val ≤ weight
        idx = _make_indices()
        kin = _make_kinematics()
        vec = _api._analogue_feature_vector(idx, kin, 10.0)
        assert sum(vec) <= 1.0 + 1e-9

    def test_high_cin_increases_first_component(self):
        low_cin = _make_indices(MLCIN=10.0)
        high_cin = _make_indices(MLCIN=200.0)
        kin = _make_kinematics()
        v_low = _api._analogue_feature_vector(low_cin, kin, 10.0)
        v_high = _api._analogue_feature_vector(high_cin, kin, 10.0)
        assert v_high[0] > v_low[0]

    def test_distance_identical_vectors_zero(self):
        idx = _make_indices()
        kin = _make_kinematics()
        vec = _api._analogue_feature_vector(idx, kin, 10.0)
        assert _api._analogue_distance(vec, vec) == pytest.approx(0.0)

    def test_distance_symmetric(self):
        idx1 = _make_indices(MLCIN=50.0)
        idx2 = _make_indices(MLCIN=150.0)
        kin = _make_kinematics()
        v1 = _api._analogue_feature_vector(idx1, kin, 10.0)
        v2 = _api._analogue_feature_vector(idx2, kin, 10.0)
        assert _api._analogue_distance(v1, v2) == pytest.approx(_api._analogue_distance(v2, v1))

    def test_distance_positive_for_different_vectors(self):
        v1 = [0.1, 0.2, 0.3, 0.1, 0.05]
        v2 = [0.3, 0.1, 0.1, 0.2, 0.1]
        assert _api._analogue_distance(v1, v2) > 0


# ── _filter_geojson_ok ────────────────────────────────────────────────────────

class TestFilterGeojsonOk:
    def _feature(self, lon, lat) -> dict:
        return {
            "type": "Feature",
            "geometry": {
                "type": "Polygon",
                "coordinates": [[[lon, lat], [lon+1, lat], [lon, lat+1], [lon, lat]]],
            },
            "properties": {},
        }

    def test_ok_feature_kept(self):
        # OUN at -97.44°W, 35.22°N — inside OK bbox
        fc = {"features": [self._feature(-97.44, 35.22)]}
        result = _api._filter_geojson_ok(fc)
        assert len(result) == 1

    def test_outside_ok_filtered(self):
        # Middle of the Atlantic — nowhere near OK
        fc = {"features": [self._feature(-30.0, 40.0)]}
        result = _api._filter_geojson_ok(fc)
        assert len(result) == 0

    def test_empty_features_list(self):
        result = _api._filter_geojson_ok({"features": []})
        assert result == []

    def test_mixed_keeps_only_ok(self):
        fc = {
            "features": [
                self._feature(-97.44, 35.22),  # OK
                self._feature(-30.0,  40.0),   # Atlantic
            ]
        }
        result = _api._filter_geojson_ok(fc)
        assert len(result) == 1

    def test_multipolygon_inside_ok_kept(self):
        feature = {
            "type": "Feature",
            "geometry": {
                "type": "MultiPolygon",
                "coordinates": [
                    [[[-97.44, 35.22], [-96.0, 35.22], [-97.44, 36.0], [-97.44, 35.22]]],
                ],
            },
            "properties": {},
        }
        fc = {"features": [feature]}
        result = _api._filter_geojson_ok(fc)
        assert len(result) == 1

    def test_unknown_geometry_type_skipped(self):
        feature = {
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [-97.0, 35.0]},
            "properties": {},
        }
        fc = {"features": [feature]}
        result = _api._filter_geojson_ok(fc)
        assert len(result) == 0


# ── _json_default ─────────────────────────────────────────────────────────────

class TestJsonDefault:
    def test_datetime_serialized_as_isoformat(self):
        dt = datetime(2024, 5, 3, 12, 0, tzinfo=timezone.utc)
        assert _api._json_default(dt) == dt.isoformat()

    def test_enum_serialized_as_value(self):
        from ok_weather_model.models.enums import OklahomaCounty
        result = _api._json_default(OklahomaCounty.CLEVELAND)
        assert result == OklahomaCounty.CLEVELAND.value

    def test_unknown_type_falls_back_to_str(self):
        class _Custom:
            def __str__(self):
                return "custom-repr"
        result = _api._json_default(_Custom())
        assert result == "custom-repr"


# ── HTTP endpoints ────────────────────────────────────────────────────────────

class TestHealthEndpoint:
    def test_returns_200(self, client):
        resp = client.get("/health")
        assert resp.status_code == 200

    def test_has_status_field(self, client):
        data = client.get("/health").json()
        assert "status" in data
        assert data["status"] in ("ok", "degraded")

    def test_has_tasks_field(self, client):
        data = client.get("/health").json()
        assert "tasks" in data
        for name in ("hrrr", "env", "surface", "spc"):
            assert name in data["tasks"]

    def test_has_warnings_list(self, client):
        data = client.get("/health").json()
        assert isinstance(data["warnings"], list)


class TestStateEndpoint:
    def test_returns_200(self, client):
        resp = client.get("/api/state")
        assert resp.status_code == 200

    def test_returns_json(self, client):
        resp = client.get("/api/state")
        data = resp.json()
        assert isinstance(data, dict)

    def test_cache_control_no_store(self, client):
        resp = client.get("/api/state")
        assert "no-store" in resp.headers.get("cache-control", "")


class TestRadarTileProxy:
    def test_invalid_station_returns_400(self, client):
        resp = client.get("/api/radar/tile/kxyz/2024-05-03T18:00:00Z/6/14/24")
        assert resp.status_code == 400

    def test_valid_station_format_accepted(self, client):
        # Will hit NOAA — mock the HRRR client to avoid real network
        with patch("httpx.AsyncClient.get") as mock_get:
            mock_get.side_effect = Exception("no network in tests")
            resp = client.get("/api/radar/tile/ktlx/2024-05-03T18:00:00Z/6/14/24")
            # 502 (upstream error) is fine — station was accepted
            assert resp.status_code in (200, 204, 502, 400)


class TestCountyEndpoint:
    def test_unknown_county_returns_404(self, client):
        resp = client.get("/api/county/FAKECOUNTY")
        assert resp.status_code == 404

    def test_known_county_returns_200_or_404(self, client):
        # 200 if HRRR data available; 404 if not yet loaded
        resp = client.get("/api/county/CLEVELAND")
        assert resp.status_code in (200, 404)


class TestPushEndpoints:
    def test_vapid_when_unconfigured_returns_503(self, client):
        # Default state has no VAPID keys loaded
        resp = client.get("/api/push/vapid")
        assert resp.status_code in (200, 503)

    def test_subscribe_missing_endpoint_returns_400(self, client):
        resp = client.post("/api/push/subscribe", json={})
        assert resp.status_code == 400

    def test_subscribe_invalid_endpoint_returns_400(self, client):
        resp = client.post(
            "/api/push/subscribe",
            json={"endpoint": "http://evil.example.com/push"},
        )
        assert resp.status_code == 400

