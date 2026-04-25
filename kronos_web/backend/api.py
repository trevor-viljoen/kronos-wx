"""
KRONOS-WX Web API — FastAPI backend with SSE push.

Runs four background fetch loops (HRRR, environment, surface, SPC) and
streams a single consolidated DashboardState to all SSE subscribers whenever
any data refreshes.

Run from the project root:
    uvicorn kronos_web.backend.api:app --reload --port 8000
"""
from __future__ import annotations

import asyncio
import json
import logging
import sys
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

import httpx
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from sse_starlette.sse import EventSourceResponse

# Ensure project root is on sys.path when run directly
_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from ok_weather_model.ingestion import HRRRClient, MesonetClient, SoundingClient
from ok_weather_model.ingestion.spc_products import (
    fetch_active_mds,
    fetch_active_watches_warnings,
    fetch_spc_outlook,
    _D1_CAT_URL,
    _D1_TORN_URL,
    _NWS_ALERT_URL,
    _NWS_UA,
    _OK_LAT_MIN, _OK_LAT_MAX, _OK_LON_MIN, _OK_LON_MAX,
)
from ok_weather_model.models import OklahomaSoundingStation, OklahomaCounty
from ok_weather_model.models.mesonet import MesonetTimeSeries
from ok_weather_model.modeling import (
    extract_features_from_indices,
    FEATURE_NAMES,
    load_model,
)
from ok_weather_model.processing import (
    compute_thermodynamic_indices,
    compute_kinematic_profile,
    compute_moisture_return,
)
from ok_weather_model.processing.cap_calculator import compute_ces_from_sounding
from ok_weather_model.processing.dryline_detector import detect_dryline
from ok_weather_model.processing.risk_zone import compute_risk_zones_from_hrrr, _TIER_RANK

logger = logging.getLogger("kronos_web.api")
logging.basicConfig(level=logging.INFO)

# ── Shared mutable state ──────────────────────────────────────────────────────

_state: dict[str, Any] = {
    "updated_at":      None,
    "hrrr_valid":      None,
    "risk_zones":      [],      # list[dict] — serialized RiskZone
    "hrrr_counties":   [],      # list[dict] — HRRRCountyPoint per county
    "tier_map":        {},      # county_name → tier (for tendency diff)
    "environment":     None,    # dict with oun/lmn sub-keys
    "ces":             None,
    "moisture":        None,
    "dryline":         None,
    "dryline_surge":   None,
    "tendency":        [],
    "spc":             {"outlook": None, "alerts": [], "mds": []},
    "alert_geojson":   None,    # raw NWS FeatureCollection (polygon geometry)
    "outlook_geojson": None,    # SPC Day1 categorical FeatureCollection
    "model_forecast":  None,
    "alert_log":       [],
}

_hrrr_base: Any = None          # first HRRR snapshot (baseline for tendency)
_hrrr_now:  Any = None          # most recent HRRR snapshot
_dryline_obj: Any = None        # BoundaryObservation for risk zone boost

_prev_tier_map: dict = {}       # for tier change diffing
_prev_md_set:   set  = set()
_prev_alert_set:set  = set()

_lock = asyncio.Lock()
_subscribers: list[asyncio.Queue] = []

# ── Census TIGER Oklahoma county GeoJSON (fetched once at startup) ───────────
_ok_counties_geojson: Optional[dict] = None

_COUNTY_GEOJSON_URL = (
    "https://raw.githubusercontent.com/plotly/datasets/master/"
    "geojson-counties-fips.json"
)


async def _load_ok_counties() -> dict:
    """Fetch US counties GeoJSON and filter to Oklahoma FIPS prefix '40'."""
    global _ok_counties_geojson
    async with httpx.AsyncClient(timeout=20.0) as client:
        resp = await client.get(_COUNTY_GEOJSON_URL)
        resp.raise_for_status()
        all_counties = resp.json()

    ok_features = [
        f for f in all_counties.get("features", [])
        if str(f.get("id", "")).zfill(5).startswith("40")
    ]
    _ok_counties_geojson = {"type": "FeatureCollection", "features": ok_features}
    logger.info("Loaded %d Oklahoma county features", len(ok_features))
    return _ok_counties_geojson


# ── Helper: broadcast to all SSE subscribers ─────────────────────────────────

async def _broadcast() -> None:
    _state["updated_at"] = datetime.now(tz=timezone.utc).isoformat()
    payload = json.dumps(_state, default=_json_default)
    dead = []
    for q in _subscribers:
        try:
            q.put_nowait(payload)
        except asyncio.QueueFull:
            dead.append(q)
    for q in dead:
        _subscribers.remove(q)


def _json_default(obj: Any) -> Any:
    if hasattr(obj, "isoformat"):
        return obj.isoformat()
    if hasattr(obj, "value"):   # enum
        return obj.value
    if hasattr(obj, "__str__"):
        return str(obj)
    raise TypeError(f"Not serializable: {type(obj)}")


def _log_alert(msg: str) -> None:
    entry = {
        "ts":  datetime.now(tz=timezone.utc).strftime("%H:%MZ"),
        "msg": msg,
    }
    _state["alert_log"] = ([entry] + _state["alert_log"])[:100]


# ── Serializers ───────────────────────────────────────────────────────────────

def _ser_zone(zone) -> dict:
    return {
        "tier":           zone.tier,
        "tier_rank":      zone.tier_rank,
        "counties":       [c.name for c in zone.counties],
        "center_lat":     zone.center_lat,
        "center_lon":     zone.center_lon,
        "lat_min":        zone.lat_min,
        "lat_max":        zone.lat_max,
        "lon_min":        zone.lon_min,
        "lon_max":        zone.lon_max,
        "peak_MLCAPE":    zone.peak_MLCAPE,
        "peak_MLCIN":     zone.peak_MLCIN,
        "peak_SRH_0_1km": zone.peak_SRH_0_1km,
        "peak_EHI":       zone.peak_EHI,
    }


def _ser_county_pt(pt) -> dict:
    return {
        "county":         pt.county.name,
        "lat":            pt.county.lat,
        "lon":            pt.county.lon,
        "MLCAPE":         pt.MLCAPE,
        "MLCIN":          pt.MLCIN,
        "SBCAPE":         pt.SBCAPE,
        "SBCIN":          pt.SBCIN,
        "SRH_0_1km":      pt.SRH_0_1km,
        "SRH_0_3km":      pt.SRH_0_3km,
        "BWD_0_6km":      pt.BWD_0_6km,
        "lapse_rate":     pt.lapse_rate_700_500,
        "dewpoint_2m_F":  pt.dewpoint_2m_F,
        "LCL_height_m":   pt.LCL_height_m,
        "EHI":            pt.EHI,
        "STP":            pt.STP,
    }


def _ser_indices(idx, kin) -> dict:
    return {
        "station":         idx.station.value if hasattr(idx.station, "value") else str(idx.station),
        "valid_time":      idx.valid_time.isoformat() if idx.valid_time else None,
        "MLCAPE":          idx.MLCAPE,
        "MLCIN":           idx.MLCIN,
        "SBCAPE":          idx.SBCAPE,
        "SBCIN":           idx.SBCIN,
        "MUCAPE":          idx.MUCAPE,
        "LCL_height":      idx.LCL_height,
        "LFC_height":      idx.LFC_height,
        "cap_strength":    idx.cap_strength,
        "convective_temperature": idx.convective_temperature,
        "lapse_rate_700_500": idx.lapse_rate_700_500,
        "precipitable_water": idx.precipitable_water,
        "SRH_0_1km":       kin.SRH_0_1km,
        "SRH_0_3km":       kin.SRH_0_3km,
        "BWD_0_6km":       kin.BWD_0_6km,
        "EHI":             kin.EHI,
        "STP":             kin.STP,
        "SCP":             kin.SCP,
        "LLJ_speed":       kin.LLJ_speed,
    }


def _ser_dryline(dl, surge) -> Optional[dict]:
    if dl is None:
        return None
    return {
        "position_lat":       dl.position_lat,
        "position_lon":       dl.position_lon,
        "confidence":         dl.confidence,
        "surge_mph":          surge,
        "counties":           [c.name for c in (dl.counties_intersected or [])],
        "motion_speed":       dl.motion_speed,
    }


def _ser_moisture(m) -> Optional[dict]:
    if m is None:
        return None
    return {
        "state_mean_dewpoint_f":    m.state_mean_dewpoint_f,
        "moisture_return_gradient_f": m.moisture_return_gradient_f,
        "gulf_moisture_fraction":   m.gulf_moisture_fraction,
        "n_stations":               m.n_stations,
    }


def _ser_outlook(o) -> Optional[dict]:
    if o is None:
        return None
    return {
        "category":          o.category,
        "max_tornado_prob":  o.max_tornado_prob,
        "sig_tornado_hatched": o.sig_tornado_hatched,
        "issued_utc":        o.issued_utc.isoformat() if o.issued_utc else None,
    }


def _ser_alert(a) -> dict:
    return {
        "event":        a.event,
        "headline":     a.headline,
        "area_desc":    a.area_desc,
        "expires_utc":  a.expires_utc.isoformat() if a.expires_utc else None,
        "expires_label":a.expires_label,
        "watch_number": a.watch_number,
        "priority":     a.priority,
    }


def _ser_md(md) -> dict:
    num = getattr(md, "md_number", None) or getattr(md, "number", 0)
    return {
        "number":          num,
        "url":             getattr(md, "url", ""),
        "areas_affected":  getattr(md, "areas_affected", ""),
        "concerning":      getattr(md, "concerning", ""),
        "body_lines":      getattr(md, "body_lines", [])[:3],
        "prob_watch":      getattr(md, "prob_watch_next_2h", None),
    }


# ── Background fetch tasks ────────────────────────────────────────────────────

async def _task_hrrr() -> None:
    global _hrrr_base, _hrrr_now, _prev_tier_map
    while True:
        try:
            now = datetime.now(tz=timezone.utc)
            snap = None
            await asyncio.sleep(0)  # yield

            def _do_fetch():
                with HRRRClient() as hc:
                    for h_back in range(3):
                        try:
                            s = hc.get_county_snapshot(now - timedelta(hours=h_back))
                            if s is not None:
                                return s
                        except Exception:
                            continue
                return None

            snap = await asyncio.to_thread(_do_fetch)
            if snap is None:
                logger.warning("HRRR: no analysis available yet")
                await asyncio.sleep(5 * 60)
                continue

            async with _lock:
                if _hrrr_base is None:
                    _hrrr_base = snap
                _hrrr_now = snap

            # Compute risk zones (pass current dryline object if available)
            zones = await asyncio.to_thread(
                compute_risk_zones_from_hrrr, snap, _dryline_obj, "MARGINAL"
            )

            # Build county list
            county_pts = [_ser_county_pt(pt) for pt in snap.counties]

            # Build tier map and detect changes
            new_tier_map: dict = {}
            for zone in zones:
                for county in zone.counties:
                    new_tier_map[county.name] = zone.tier

            async with _lock:
                old_map = dict(_prev_tier_map)
                _prev_tier_map = new_tier_map

            for cname, tier in new_tier_map.items():
                old_tier = old_map.get(cname, "LOW")
                if old_tier != tier:
                    old_rank = _TIER_RANK.get(old_tier, 0)
                    new_rank = _TIER_RANK.get(tier, 0)
                    if max(old_rank, new_rank) >= 2:
                        arrow = "▲" if new_rank > old_rank else "▼"
                        _log_alert(f"{arrow} {cname}: {old_tier} → {tier}")

            # Tendency
            tend = _compute_tendency(snap, new_tier_map)

            vt = getattr(snap, "valid_time", None)
            ts = vt.strftime("%H:%MZ") if vt else None

            async with _lock:
                _state["hrrr_valid"]    = ts
                _state["risk_zones"]    = [_ser_zone(z) for z in zones]
                _state["hrrr_counties"] = county_pts
                _state["tier_map"]      = new_tier_map
                _state["tendency"]      = tend

            await _broadcast()
            logger.info("HRRR updated: %s, %d zones", ts, len(zones))

        except Exception as exc:
            logger.error("HRRR task error: %s", exc, exc_info=True)

        await asyncio.sleep(15 * 60)


def _compute_tendency(snap, tier_map: dict) -> list[dict]:
    """Compute delta fields for all MODERATE+ counties."""
    global _hrrr_base
    base = _hrrr_base
    if base is None or base is snap:
        return []

    rows = []
    seen: set = set()
    tier_to_rank = {t: r for t, r in _TIER_RANK.items()}

    sorted_counties = sorted(
        [(cname, tier) for cname, tier in tier_map.items()
         if tier_to_rank.get(tier, 0) >= 2],
        key=lambda x: -tier_to_rank.get(x[1], 0),
    )

    for cname, tier in sorted_counties[:10]:
        if cname in seen:
            continue
        seen.add(cname)
        try:
            county_enum = OklahomaCounty[cname]
        except KeyError:
            continue
        pt_n = snap.get(county_enum)
        pt_b = base.get(county_enum)
        if pt_n is None or pt_b is None:
            continue

        d_cin  = pt_n.MLCIN  - pt_b.MLCIN
        d_cape = pt_n.MLCAPE - pt_b.MLCAPE
        d_srh1 = pt_n.SRH_0_1km - pt_b.SRH_0_1km
        d_srh3 = pt_n.SRH_0_3km - pt_b.SRH_0_3km
        d_ehi  = (pt_n.EHI or 0.0) - (pt_b.EHI or 0.0)

        score = (
            (1 if d_cin <= -10 else 0) + (1 if d_cin <= -30 else 0) +
            (1 if d_srh1 >= 20 else 0) + (1 if d_cape >= 200 else 0) +
            (-1 if d_cin >= 10 else 0) + (-1 if d_srh1 <= -20 else 0)
        )
        trend = "▲▲" if score >= 3 else "▲" if score == 2 else "→" if score >= 0 else "▼"
        trend_level = "improving2" if score >= 3 else "improving" if score == 2 else "steady" if score >= 0 else "degrading"

        rows.append({
            "county": cname,
            "tier": tier,
            "d_cin":   round(d_cin,  1),
            "d_cape":  round(d_cape, 1),
            "d_srh1":  round(d_srh1, 1),
            "d_srh3":  round(d_srh3, 1),
            "d_ehi":   round(d_ehi,  2),
            "trend":       trend,
            "trend_level": trend_level,
        })

    return rows


async def _task_environment() -> None:
    while True:
        try:
            stn   = OklahomaSoundingStation.OUN
            today = datetime.now(tz=timezone.utc).date()

            def _do_sounding():
                profile = None
                fetched_hour = None
                with SoundingClient() as sc:
                    for h in (12, 0, 18, 21):
                        try:
                            p = sc.get_sounding(stn, today, h)
                            if p:
                                profile = p
                                fetched_hour = h
                                break
                        except Exception:
                            continue
                return profile, fetched_hour

            profile, fetched_hour = await asyncio.to_thread(_do_sounding)

            if profile is None:
                logger.warning("Sounding: no data for today yet")
                await asyncio.sleep(30 * 60)
                continue

            idx = await asyncio.to_thread(compute_thermodynamic_indices, profile)
            kin = await asyncio.to_thread(compute_kinematic_profile, profile, idx)

            # LMN sounding
            lmn_idx = lmn_kin = None
            if fetched_hour is not None:
                def _do_lmn():
                    with SoundingClient() as sc2:
                        lp = sc2.get_sounding(OklahomaSoundingStation.LMN, today, fetched_hour)
                    if lp:
                        li = compute_thermodynamic_indices(lp)
                        lk = compute_kinematic_profile(lp, li)
                        return li, lk
                    return None, None
                lmn_idx, lmn_kin = await asyncio.to_thread(_do_lmn)

            # CES projection (12Z only)
            ces_data = None
            if fetched_hour == 12:
                try:
                    async with _lock:
                        moist = _state.get("moisture")
                    surf_t_c = None
                    if moist:
                        td_f = moist.get("state_mean_dewpoint_f")
                        if td_f is not None:
                            td_c = (td_f - 32.0) / 1.8
                            surf_t_c = td_c + 2.0 + idx.LCL_height * 0.0065

                    if surf_t_c is not None:
                        traj = await asyncio.to_thread(
                            compute_ces_from_sounding, idx, surf_t_c, today
                        )
                        if traj is not None:
                            cb = traj.cap_behavior
                            cb_str = cb.value if hasattr(cb, "value") else str(cb)
                            ces_data = {
                                "cap_behavior":     cb_str,
                                "erosion_hour":     getattr(traj, "erosion_hour_utc", None),
                                "tc_gap_12Z":       getattr(traj, "convective_temp_gap_12Z", None),
                                "tc_gap_18Z":       getattr(traj, "convective_temp_gap_18Z", None),
                            }
                except Exception as exc:
                    logger.debug("CES error: %s", exc)

            # Model predictions
            model_pred = None
            try:
                clf = load_model("severity_classifier")
                reg = load_model("tornado_regressor")
                if clf and reg:
                    proba = clf.predict_proba(idx, kin)
                    count = reg.predict(idx, kin)
                    model_pred = {
                        "sig_pct":      round(proba.get("significant", 0) * 100, 1),
                        "count_exp":    round(count.get("expected_count", 0), 1),
                        "count_lo":     round(count.get("interval_low", 0), 1),
                        "count_hi":     round(count.get("interval_high", 0), 1),
                    }
            except Exception:
                pass

            env_data = {
                "oun": _ser_indices(idx, kin),
                "lmn": _ser_indices(lmn_idx, lmn_kin) if lmn_idx and lmn_kin else None,
                "fetched_hour": fetched_hour,
            }

            async with _lock:
                _state["environment"]   = env_data
                _state["ces"]           = ces_data
                _state["model_forecast"]= model_pred

            await _broadcast()
            logger.info("Environment updated: %s %02dZ", today, fetched_hour)

        except Exception as exc:
            logger.error("Environment task error: %s", exc, exc_info=True)

        await asyncio.sleep(60 * 60)


async def _task_surface() -> None:
    global _dryline_obj
    while True:
        try:
            now = datetime.now(tz=timezone.utc)

            def _do_surface():
                station_series: dict = {}
                current_obs = []
                current_snap_time = None

                with MesonetClient() as mc:
                    for h_back in (0, 1):
                        target = now - timedelta(hours=h_back)
                        for back_min in range(0, 25, 5):
                            snap_time = target - timedelta(minutes=back_min)
                            try:
                                obs = mc.get_snapshot_observations(snap_time)
                            except Exception as exc:
                                if any(s in str(exc) for s in ("404", "400")):
                                    continue
                                break
                            if not obs:
                                continue
                            for o in obs:
                                stid = o.station_id
                                if stid not in station_series:
                                    station_series[stid] = MesonetTimeSeries(
                                        station_id=stid, county=o.county,
                                        start_time=snap_time, end_time=snap_time,
                                        observations=[],
                                    )
                                station_series[stid].observations.append(o)
                                station_series[stid].end_time = max(
                                    station_series[stid].end_time, snap_time
                                )
                            if h_back == 0 and current_snap_time is None:
                                current_obs = obs
                                current_snap_time = snap_time
                            break

                return station_series, current_obs, current_snap_time

            station_series, current_obs, snap_time = await asyncio.to_thread(_do_surface)
            if not station_series or snap_time is None:
                await asyncio.sleep(5 * 60)
                continue

            moisture = await asyncio.to_thread(compute_moisture_return, current_obs)
            dl = await asyncio.to_thread(detect_dryline, station_series, snap_time)

            # Compute surge vs previous
            prev_dl = _dryline_obj
            surge = None
            if prev_dl is not None and dl is not None:
                try:
                    from ok_weather_model.processing.dryline_detector import compute_dryline_surge_rate
                    surge = compute_dryline_surge_rate(prev_dl, dl)
                except Exception:
                    pass

            # Dryline change alerts
            if dl is not None and prev_dl is None:
                _log_alert("Dryline appeared")
            elif dl is None and prev_dl is not None:
                _log_alert("Dryline no longer detected")
            elif dl is not None and surge is not None and abs(surge) >= 10:
                direction = "eastward" if surge > 0 else "retrograding"
                _log_alert(f"Dryline {direction}: {surge:+.0f} mph")

            _dryline_obj = dl

            async with _lock:
                _state["moisture"]      = _ser_moisture(moisture)
                _state["dryline"]       = _ser_dryline(dl, surge)

            await _broadcast()

        except Exception as exc:
            logger.error("Surface task error: %s", exc, exc_info=True)

        await asyncio.sleep(5 * 60)


async def _task_spc() -> None:
    global _prev_md_set, _prev_alert_set
    while True:
        try:
            def _do_spc():
                outlook  = fetch_spc_outlook()
                alerts   = fetch_active_watches_warnings()
                mds      = fetch_active_mds()
                return outlook, alerts or [], mds or []

            outlook, alerts, mds = await asyncio.to_thread(_do_spc)

            # Fetch raw NWS FeatureCollection (includes polygon geometry)
            async with httpx.AsyncClient(
                timeout=12.0,
                headers={"User-Agent": _NWS_UA},
                follow_redirects=True,
            ) as client:
                try:
                    r = await client.get(_NWS_ALERT_URL)
                    r.raise_for_status()
                    alert_geojson = r.json()
                except Exception:
                    alert_geojson = None

                # SPC Day1 categorical GeoJSON (filter to OK bbox)
                try:
                    r2 = await client.get(_D1_CAT_URL)
                    r2.raise_for_status()
                    cat_data = r2.json()
                    ok_features = _filter_geojson_ok(cat_data)
                    outlook_geojson = {"type": "FeatureCollection", "features": ok_features}
                except Exception:
                    outlook_geojson = None

            # Alert diffing
            new_alert_set: set = set()
            for a in alerts:
                key = f"{a.event}:{getattr(a, 'watch_number', '')}:{a.area_desc[:30]}"
                new_alert_set.add(key)
                if key not in _prev_alert_set:
                    if "Tornado Warning" in a.event:
                        _log_alert(f"🌪 TORNADO WARNING: {a.area_desc[:60]}")
                    elif "Tornado Watch" in a.event:
                        num_s = f" #{a.watch_number}" if a.watch_number else ""
                        _log_alert(f"📋 TORNADO WATCH{num_s} issued")

            new_md_set: set = set()
            for md in mds:
                num = getattr(md, "md_number", None) or getattr(md, "number", None)
                if num:
                    new_md_set.add(num)
                    if num not in _prev_md_set:
                        concerning = getattr(md, "concerning", "") or getattr(md, "areas_affected", "") or "active"
                        _log_alert(f"📋 SPC MD #{num}: {concerning[:60]}")

            _prev_alert_set = new_alert_set
            _prev_md_set    = new_md_set

            async with _lock:
                _state["spc"] = {
                    "outlook": _ser_outlook(outlook),
                    "alerts":  [_ser_alert(a) for a in alerts],
                    "mds":     [_ser_md(md) for md in mds],
                }
                _state["alert_geojson"]   = alert_geojson
                _state["outlook_geojson"] = outlook_geojson

            await _broadcast()
            logger.info("SPC updated: %s, %d alerts, %d MDs",
                        outlook.category if outlook else "none", len(alerts), len(mds))

        except Exception as exc:
            logger.error("SPC task error: %s", exc, exc_info=True)

        await asyncio.sleep(15 * 60)


def _filter_geojson_ok(geojson: dict) -> list:
    """Keep only features whose geometry intersects the Oklahoma bounding box."""
    def _bbox_intersects(coords_list) -> bool:
        for ring in coords_list:
            for lon, lat in ring:
                if (_OK_LON_MIN <= lon <= _OK_LON_MAX and
                        _OK_LAT_MIN <= lat <= _OK_LAT_MAX):
                    return True
        return False

    filtered = []
    for feature in geojson.get("features", []):
        geom = feature.get("geometry", {})
        gtype = geom.get("type", "")
        coords = geom.get("coordinates", [])
        if gtype == "Polygon" and _bbox_intersects(coords):
            filtered.append(feature)
        elif gtype == "MultiPolygon":
            for poly in coords:
                if _bbox_intersects(poly):
                    filtered.append(feature)
                    break
    return filtered


# ── App lifespan ──────────────────────────────────────────────────────────────

@asynccontextmanager
async def _lifespan(app: FastAPI):
    # Fetch OK county GeoJSON
    try:
        await _load_ok_counties()
    except Exception as exc:
        logger.warning("Could not load county GeoJSON: %s", exc)

    # Start all background fetch tasks
    tasks = [
        asyncio.create_task(_task_hrrr(),        name="hrrr"),
        asyncio.create_task(_task_environment(), name="env"),
        asyncio.create_task(_task_surface(),     name="surface"),
        asyncio.create_task(_task_spc(),         name="spc"),
    ]
    yield
    for t in tasks:
        t.cancel()


# ── FastAPI app ───────────────────────────────────────────────────────────────

app = FastAPI(title="KRONOS-WX API", lifespan=_lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)


@app.get("/api/state")
async def get_state():
    """Return the full current dashboard state as JSON."""
    return JSONResponse(
        content=json.loads(json.dumps(_state, default=_json_default))
    )


@app.get("/api/stream")
async def stream(request: Request):
    """Server-Sent Events stream — pushes DashboardState on every data update."""
    q: asyncio.Queue = asyncio.Queue(maxsize=10)
    _subscribers.append(q)

    async def generator():
        try:
            # Send current state immediately on connect
            yield {"data": json.dumps(_state, default=_json_default)}
            while True:
                if await request.is_disconnected():
                    break
                try:
                    payload = await asyncio.wait_for(q.get(), timeout=30.0)
                    yield {"data": payload}
                except asyncio.TimeoutError:
                    yield {"data": '{"ping":true}'}  # keepalive
        finally:
            if q in _subscribers:
                _subscribers.remove(q)

    return EventSourceResponse(generator())


@app.get("/api/county/{county_name}")
async def get_county(county_name: str):
    """Per-county detail: current HRRR data + tier from latest snapshot."""
    try:
        county_enum = OklahomaCounty[county_name.upper()]
    except KeyError:
        return JSONResponse({"error": f"Unknown county: {county_name}"}, status_code=404)

    # Find in hrrr_counties list
    for pt in _state.get("hrrr_counties", []):
        if pt.get("county") == county_enum.name:
            tier = _state.get("tier_map", {}).get(county_enum.name, "LOW")
            return {"county": county_enum.name, "tier": tier, "data": pt}

    return JSONResponse({"error": "No HRRR data yet"}, status_code=404)


@app.get("/api/counties.geojson")
async def get_counties_geojson():
    """Oklahoma county boundaries GeoJSON (Census TIGER, filtered to FIPS 40)."""
    if _ok_counties_geojson is None:
        return JSONResponse({"error": "County GeoJSON not loaded yet"}, status_code=503)
    return JSONResponse(_ok_counties_geojson)


@app.get("/health")
async def health():
    return {"status": "ok", "updated_at": _state.get("updated_at")}
