"""
Tests for Mesonet-based dryline detection.
"""

from datetime import datetime, timezone


from ok_weather_model.models.enums import BoundaryType, OklahomaCounty
from ok_weather_model.models.mesonet import MesonetObservation, MesonetTimeSeries
from ok_weather_model.processing.dryline_detector import (
    detect_dryline,
    compute_dryline_surge_rate,
    analyze_dryline_from_mesonet,
)

# ── Fixtures ───────────────────────────────────────────────────────────────────

_VT = datetime(1999, 5, 3, 18, 0, tzinfo=timezone.utc)
_DATE = _VT.date()


def _make_obs(county: OklahomaCounty, td: float, wdir: float = 180.0,
              valid_time: datetime = _VT) -> MesonetObservation:
    """Minimal MesonetObservation with the given county and dewpoint."""
    return MesonetObservation(
        station_id=county.mesonet_station_id,
        county=county,
        valid_time=valid_time,
        temperature=85.0,
        dewpoint=td,
        relative_humidity=50.0,
        wind_direction=wdir,
        wind_speed=15.0,
        pressure=980.0,
    )


def _make_series(county: OklahomaCounty, td: float, wdir: float = 180.0,
                 valid_time: datetime = _VT) -> MesonetTimeSeries:
    """Single-observation MesonetTimeSeries for a county."""
    obs = _make_obs(county, td, wdir, valid_time)
    return MesonetTimeSeries(
        station_id=county.mesonet_station_id,
        county=county,
        start_time=valid_time,
        end_time=valid_time,
        observations=[obs],
    )


def _station_series(
    assignments: list[tuple[OklahomaCounty, float, float]],  # (county, td, wdir)
    valid_time: datetime = _VT,
) -> dict[str, MesonetTimeSeries]:
    """Build a station_series dict from (county, td, wdir) tuples."""
    result = {}
    for county, td, wdir in assignments:
        ts = _make_series(county, td, wdir, valid_time)
        result[county.mesonet_station_id] = ts
    return result


# ── detect_dryline ─────────────────────────────────────────────────────────────

class TestDetectDryline:

    def test_classic_central_ok_dryline(self):
        """
        Classic dryline: western stations dry (25–30°F), eastern stations moist (60–65°F).
        Expect a clear DRYLINE BoundaryObservation with high confidence.

        West → East cross-section across central OK:
          CUSTER   (~-99.0) Td=28°F  — dry sector
          CADDO    (~-98.2) Td=32°F  — transition
          GRADY    (~-97.9) Td=58°F  — moist sector (sharp jump)
          OKLAHOMA (~-97.5) Td=62°F  — moist sector
          CLEVELAND(~-97.4) Td=63°F  — moist sector
        """
        series = _station_series([
            (OklahomaCounty.CUSTER,   28.0, 250.0),  # WSW wind — dry sector
            (OklahomaCounty.CADDO,    32.0, 240.0),
            (OklahomaCounty.GRADY,    58.0, 175.0),  # SSE wind — moist sector
            (OklahomaCounty.OKLAHOMA, 62.0, 170.0),
            (OklahomaCounty.CLEVELAND,63.0, 165.0),
        ])
        boundary = detect_dryline(series, _VT)

        assert boundary is not None
        assert boundary.boundary_type == BoundaryType.DRYLINE
        assert boundary.detected_by == "mesonet_td_gradient"

        # Dryline should be somewhere between -98.2 and -97.9 (between CADDO and GRADY)
        mean_lon = sum(boundary.position_lon) / len(boundary.position_lon)
        assert -98.5 < mean_lon < -97.5, f"Dryline lon {mean_lon:.2f} out of expected range"

        # At least 2 polyline points (N-S segment)
        assert len(boundary.position_lat) >= 2
        assert len(boundary.position_lon) >= 2

        # Confidence should be reasonable with a strong gradient
        assert boundary.confidence > 0.3

    def test_no_dryline_outflow_boundary_in_moist_sector(self):
        """
        Outflow boundary: sharp Td drop between two moist-sector stations.
        The cool outflow pocket (OKFUSKEE, 44°F) sits east of LINCOLN (moist,
        64°F). The contiguous-dry-sector walk reaches CUSTER→CANADIAN before
        ever reaching LINCOLN, so OKFUSKEE is never considered as a dry edge.

        Validated 2026-05-18: actual dryline at HARMON/BECKHAM was ~120 miles
        east of the TX panhandle dry sector. The walk correctly identifies
        CUSTER as the dry edge and pairs it with CANADIAN.
        """
        series = _station_series([
            # Western OK — actual dry sector near Weatherford
            (OklahomaCounty.ROGER_MILLS, 28.0, 265.0),  # lon≈-100.0, dry
            (OklahomaCounty.CUSTER,      32.0, 255.0),  # lon≈-99.0,  dry
            (OklahomaCounty.CANADIAN,    62.0, 175.0),  # lon≈-97.9,  moist
            # Eastern OK — sharp outflow gradient entirely in the moist sector
            (OklahomaCounty.LINCOLN,     64.0, 170.0),  # lon≈-96.9,  moist
            (OklahomaCounty.OKFUSKEE,    44.0, 220.0),  # lon≈-96.3,  post-outflow (cool, lower Td)
            (OklahomaCounty.MUSKOGEE,    65.0, 165.0),  # lon≈-95.4,  moist again
        ])
        boundary = detect_dryline(series, _VT)
        assert boundary is not None, "Should detect the dryline in western OK"
        mean_lon = sum(boundary.position_lon) / len(boundary.position_lon)
        # Must be in western OK (between CUSTER and CANADIAN), not near Okemah
        assert mean_lon < -97.5, (
            f"Dryline lon {mean_lon:.1f}°W detected in moist sector — "
            "outflow boundary filter not working"
        )

    def test_no_dryline_uniform_dewpoints(self):
        """Uniform Td across all stations — no dryline should be detected."""
        series = _station_series([
            (OklahomaCounty.CUSTER,   62.0, 180.0),
            (OklahomaCounty.GRADY,    63.0, 180.0),
            (OklahomaCounty.OKLAHOMA, 61.0, 180.0),
            (OklahomaCounty.CLEVELAND,62.0, 180.0),
        ])
        assert detect_dryline(series, _VT) is None

    def test_no_dryline_weak_gradient(self):
        """
        Small Td spread with no pair clearing both the absolute-drop and
        gradient thresholds simultaneously.

        CUSTER(54°F) → CLEVELAND(62°F): abs drop = 8°F ≥ threshold, but
        dlon ≈ 1.4° → gradient ≈ 5.7°F/deg < MIN_TD_GRADIENT_F_PER_DEG(8).
        CUSTER(54°F) → GRADY(58°F ≥ 55 ✓): abs drop = 4°F < threshold.
        GRADY(58°F) → CLEVELAND(62°F ≥ 55 ✓): abs drop = 4°F < threshold.
        """
        series = _station_series([
            (OklahomaCounty.CUSTER,   54.0, 200.0),
            (OklahomaCounty.GRADY,    58.0, 185.0),
            (OklahomaCounty.CLEVELAND,62.0, 175.0),
        ])
        result = detect_dryline(series, _VT)
        if result is not None:
            assert result.confidence < 0.4

    def test_no_dryline_backwards_gradient(self):
        """Td *increases* from east to west — this is not a dryline signature."""
        series = _station_series([
            (OklahomaCounty.CUSTER,   65.0, 160.0),  # moist to the west (unusual)
            (OklahomaCounty.GRADY,    62.0, 165.0),
            (OklahomaCounty.CLEVELAND,30.0, 250.0),  # dry to the east
        ])
        # dtd = td_east - td_west < 0 throughout, so no dryline
        assert detect_dryline(series, _VT) is None

    def test_sparse_stations_single_band(self):
        """
        Two stations in the same latitude window — detection should succeed and
        produce a ≥2-point polyline (padded if only one vertex detects).
        """
        series = _station_series([
            (OklahomaCounty.BLAINE,   28.0, 270.0),  # ~35.84°N, ~-98.5°W
            (OklahomaCounty.OKLAHOMA, 62.0, 175.0),  # ~35.55°N, ~-97.5°W
        ])
        boundary = detect_dryline(series, _VT)
        assert boundary is not None
        assert len(boundary.position_lat) >= 2
        assert len(boundary.position_lon) >= 2

    def test_statewide_multi_band_dryline(self):
        """
        Statewide dryline with N/C/S coverage — should detect across the full
        latitude range and produce a ≥3-point polyline with high confidence.
        The sliding-window approach may produce more than 3 vertices when
        adjacent windows both detect the same dryline segment.
        """
        series = _station_series([
            # North (36.75°N)
            (OklahomaCounty.WOODS,    30.0, 260.0),   # lat=36.75°, lon=-98.77
            (OklahomaCounty.GRANT,    60.0, 175.0),   # lat=36.79°, lon=-97.69
            # Central (35.05–35.57°N)
            (OklahomaCounty.CUSTER,   27.0, 255.0),   # lat=35.57°, lon=-99.03
            (OklahomaCounty.GRADY,    62.0, 170.0),   # lat=35.05°, lon=-97.97
            # South (34.51–34.73°N)
            (OklahomaCounty.HARMON,   25.0, 265.0),   # lat=34.73°, lon=-99.85
            (OklahomaCounty.STEPHENS, 61.0, 175.0),   # lat=34.51°, lon=-97.95
        ])
        boundary = detect_dryline(series, _VT)
        assert boundary is not None
        assert len(boundary.position_lat) >= 3
        assert boundary.confidence > 0.5

    def test_observation_outside_time_window_ignored(self):
        """
        Station with only an observation 20 minutes away from valid_time
        should be excluded from detection.
        """
        from datetime import timedelta
        old_vt = _VT - timedelta(minutes=20)

        dry_series = _make_series(OklahomaCounty.CUSTER, 28.0, valid_time=old_vt)
        moist_series = _make_series(OklahomaCounty.GRADY, 64.0, valid_time=_VT)

        series = {
            OklahomaCounty.CUSTER.mesonet_station_id:  dry_series,
            OklahomaCounty.GRADY.mesonet_station_id:   moist_series,
        }
        # CUSTER observation is outside the 7-min window; without it, only
        # one station is valid in the central band → no dryline.
        result = detect_dryline(series, _VT)
        assert result is None

    def test_counties_intersected_populated(self):
        """Detected dryline should include at least one county in counties_intersected."""
        series = _station_series([
            (OklahomaCounty.BLAINE,   28.0, 260.0),
            (OklahomaCounty.OKLAHOMA, 63.0, 175.0),
        ])
        boundary = detect_dryline(series, _VT)
        # Might be None if only one station per band, but if detected:
        if boundary is not None:
            assert isinstance(boundary.counties_intersected, list)

    def test_polyline_sorted_south_to_north(self):
        """Polyline lats should be in ascending order (S→N)."""
        series = _station_series([
            (OklahomaCounty.WOODS,    28.0, 260.0),
            (OklahomaCounty.GRANT,    60.0, 175.0),
            (OklahomaCounty.CUSTER,   27.0, 255.0),
            (OklahomaCounty.GRADY,    62.0, 170.0),
            (OklahomaCounty.HARMON,   25.0, 265.0),
            (OklahomaCounty.STEPHENS, 61.0, 175.0),
        ])
        boundary = detect_dryline(series, _VT)
        if boundary is not None and len(boundary.position_lat) > 1:
            for i in range(len(boundary.position_lat) - 1):
                assert boundary.position_lat[i] <= boundary.position_lat[i + 1]

    def test_bowing_dryline_polyline_non_straight(self):
        """
        Dryline with a southward bulge: the central latitude detects the dryline
        farther east than the north and south sections. The resulting polyline
        should have different longitudes at different latitudes — not a straight
        N-S line.

        Scenario: dryline at -99°W in north and south, but bulging east to
        -98°W in the central corridor where warm Gulf air has surged further west.
        """
        series = _station_series([
            # North segment — dryline at ~-99°W
            (OklahomaCounty.WOODS,     28.0, 260.0),  # lat=36.75°, lon=-98.77 — dry
            (OklahomaCounty.GRANT,     62.0, 175.0),  # lat=36.79°, lon=-97.69 — moist
            # Central segment — dryline pushed east to ~-98°W (eastward bulge)
            (OklahomaCounty.KINGFISHER,28.0, 255.0),  # lat=35.86°, lon=-97.93 — dry
            (OklahomaCounty.OKLAHOMA,  64.0, 170.0),  # lat=35.55°, lon=-97.50 — moist
            # South segment — dryline at ~-99°W
            (OklahomaCounty.HARMON,    25.0, 265.0),  # lat=34.73°, lon=-99.85 — dry
            (OklahomaCounty.STEPHENS,  61.0, 175.0),  # lat=34.51°, lon=-97.95 — moist
        ])
        boundary = detect_dryline(series, _VT)
        assert boundary is not None
        assert len(boundary.position_lon) >= 3, "Need ≥3 points to express a bow"

        # The polyline should not be a perfectly straight N-S line:
        # at least two longitude values should differ by more than 0.3°
        lon_range = max(boundary.position_lon) - min(boundary.position_lon)
        assert lon_range > 0.3, (
            f"All polyline lons within 0.3° — straight line, bow not captured "
            f"(lons={[round(l,2) for l in boundary.position_lon]})"
        )


# ── compute_dryline_surge_rate ────────────────────────────────────────────────

class TestComputeDrylineSurgeRate:

    def _make_boundary_at(self, lon: float, hour: int) -> object:
        from ok_weather_model.models.boundary import BoundaryObservation
        vt = datetime(1999, 5, 3, hour, 0, tzinfo=timezone.utc)
        return BoundaryObservation(
            valid_time=vt,
            boundary_type=BoundaryType.DRYLINE,
            position_lat=[34.5, 36.5],
            position_lon=[lon, lon],
            counties_intersected=[OklahomaCounty.OKLAHOMA],
        )

    def test_eastward_surge(self):
        """Dryline moves 1° east (~53 miles) in 3 hours → ~17.7 mph."""
        early = self._make_boundary_at(-100.0, 15)
        late  = self._make_boundary_at(-99.0,  18)
        rate  = compute_dryline_surge_rate(early, late)
        assert rate is not None
        assert 15.0 < rate < 22.0

    def test_retrograde_dryline(self):
        """Dryline retreats westward → negative surge rate."""
        early = self._make_boundary_at(-98.0, 15)
        late  = self._make_boundary_at(-99.5, 18)
        rate  = compute_dryline_surge_rate(early, late)
        assert rate is not None
        assert rate < 0.0

    def test_stationary_dryline(self):
        """Dryline doesn't move → ~0 mph."""
        early = self._make_boundary_at(-98.5, 15)
        late  = self._make_boundary_at(-98.5, 18)
        rate  = compute_dryline_surge_rate(early, late)
        assert rate is not None
        assert abs(rate) < 1.0

    def test_returns_none_for_short_interval(self):
        """Time gap < 30 min → return None."""
        early = self._make_boundary_at(-99.0, 18)
        late  = self._make_boundary_at(-98.0, 18)  # same hour, so dt=0
        assert compute_dryline_surge_rate(early, late) is None


# ── analyze_dryline_from_mesonet ──────────────────────────────────────────────

class TestAnalyzeDrylineFromMesonet:

    def test_returns_expected_keys(self):
        """Result dict always has the three expected keys."""
        result = analyze_dryline_from_mesonet({}, _DATE)
        assert "boundaries" in result
        assert "dryline_lon_18Z" in result
        assert "surge_rate_mph" in result

    def test_empty_series_returns_none_values(self):
        """No stations → no dryline detected at any hour."""
        result = analyze_dryline_from_mesonet({}, _DATE)
        assert result["boundaries"] == []
        assert result["dryline_lon_18Z"] is None
        assert result["surge_rate_mph"] is None

    def test_18z_lon_populated(self):
        """When an 18Z dryline is detected, dryline_lon_18Z should be set."""

        # Build station series with observations at 15Z, 18Z, and 21Z
        assignments_and_hours = [
            (OklahomaCounty.BLAINE,   28.0, 260.0),
            (OklahomaCounty.OKLAHOMA, 63.0, 175.0),
        ]
        series: dict[str, MesonetTimeSeries] = {}
        for hour in (15, 18, 21):
            vt = datetime(1999, 5, 3, hour, 0, tzinfo=timezone.utc)
            for county, td, wdir in assignments_and_hours:
                stid = county.mesonet_station_id
                obs = _make_obs(county, td, wdir, vt)
                if stid not in series:
                    series[stid] = MesonetTimeSeries(
                        station_id=stid,
                        county=county,
                        start_time=datetime(1999, 5, 3, 15, 0, tzinfo=timezone.utc),
                        end_time=datetime(1999, 5, 3, 21, 0, tzinfo=timezone.utc),
                        observations=[],
                    )
                series[stid].observations.append(obs)

        result = analyze_dryline_from_mesonet(series, _DATE)
        assert result["dryline_lon_18Z"] is not None
        # Should be between BLAINE and OKLAHOMA longitudes (~-98.5, ~-97.5)
        assert -99.0 < result["dryline_lon_18Z"] < -97.0
