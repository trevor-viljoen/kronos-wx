"""
KRONOS-WX TUI Dashboard — real-time severe weather situational awareness.

Layout (130+ column terminal):
  ┌─ Risk Zones ──────────────────┬─ Environment ──────┬─ Surface / Dryline ──┐
  │ DataTable: county risk tiers  │ OUN │ LMN columns   │ Moisture return      │
  │ (refresh 15 min)              │ (refresh 60 min)   │ (refresh 5 min)      │
  ├─── Tendency ──────────────────────── baseline → current ───────────────────┤
  │ County │ Tier │ ΔCIN │ ΔCAPE │ ΔSRH-1 │ ΔSRH-3 │ ΔEHI │ Trend             │
  ├─── SPC Products ───────────────────────────────────────────────────────────┤
  │ D1 Outlook │ Tornado Warnings │ Watches │ MDs                              │
  ├─── Alert Log ───────────────────────────────────────────────────────────────┤
  │ Scrolling tier changes and alert events                                    │
  └─────────────────────────────────────────────────────────────────────────────┘

Key bindings: R = refresh all, Q = quit
"""
from __future__ import annotations

import logging
import math
import re
import threading
from datetime import datetime, timedelta, timezone
from typing import Optional

from rich.text import Text
from textual import work
from textual.app import App, ComposeResult
from textual.containers import Horizontal, Vertical
from textual.reactive import reactive

# ── Pipeline imports at module level (avoids import-lock deadlocks in threads) ─
from ok_weather_model.ingestion import HRRRClient, MesonetClient, SoundingClient
from ok_weather_model.models.mesonet import MesonetTimeSeries
from ok_weather_model.ingestion.spc_products import (
    fetch_spc_outlook,
    fetch_active_watches_warnings,
    fetch_active_mds,
)
from ok_weather_model.models import OklahomaSoundingStation
from ok_weather_model.modeling import (
    extract_features_from_indices,
    FEATURE_NAMES,
    load_model,
)
from ok_weather_model.processing import (
    compute_thermodynamic_indices,
    compute_kinematic_profile,
    compute_moisture_return,
    compute_modified_indices,
)
from ok_weather_model.processing.cap_calculator import compute_ces_from_sounding
from ok_weather_model.processing.dryline_detector import (
    detect_dryline,
    compute_dryline_surge_rate,
)
from ok_weather_model.processing.risk_zone import (
    compute_risk_zones_from_hrrr,
    _TIER_RANK,
)
from ok_weather_model.storage import Database
from textual.widgets import DataTable, Footer, Header, RichLog, Static

logger = logging.getLogger(__name__)

# ── Tier styling ──────────────────────────────────────────────────────────────
_TIER_COLOR = {
    "EXTREME":           "bold bright_red",
    "HIGH":              "bold red",
    "DANGEROUS_CAPPED":  "bold magenta",
    "MODERATE":          "yellow",
    "MARGINAL":          "green",
    "LOW":               "dim white",
}

# ── Design tokens (DESIGN.md) ─────────────────────────────────────────────────
CSS = """
Screen {
    background: #050510;
    color: #d0d8e8;
}

#top-row {
    height: 1fr;
}

#risk-panel {
    width: 2fr;
    border: solid #1e3a5f;
    overflow-y: scroll;
}

#risk-panel > DataTable {
    background: #080820;
    height: 1fr;
}

#risk-panel > DataTable > .datatable--header {
    background: #0e2040;
    color: #7ec8e3;
}

#risk-panel > DataTable > .datatable--cursor {
    background: #080820;
}

#env-panel {
    width: 1fr;
    border: solid #1e3a5f;
    padding: 0 1;
    overflow-y: scroll;
    background: #080820;
}

#dryline-panel {
    width: 1fr;
    border: solid #1e3a5f;
    padding: 0 1;
    overflow-y: scroll;
    background: #080820;
}

#tendency-row {
    height: 13;
    border: solid #1e3a5f;
    padding: 0 1;
    overflow-x: scroll;
    background: #080820;
}

#spc-row {
    height: 7;
    border: solid #5f3a1e;
    padding: 0 1;
    overflow-y: scroll;
    background: #080820;
}

#alert-log {
    height: 10;
    border: solid #1e3a5f;
    background: #080820;
}

Footer {
    height: 1;
    background: #0e2040;
}
"""

# ── Helpers ───────────────────────────────────────────────────────────────────

def _fmt(val, fmt: str, fallback: str = "—") -> str:
    if val is None or (isinstance(val, float) and math.isnan(val)):
        return fallback
    return fmt.format(val)


def _abbrev_counties(area_desc: str, max_counties: int = 8) -> str:
    """Shorten a comma-separated county list for one-line display."""
    parts = [p.strip() for p in area_desc.replace(";", ",").split(",") if p.strip()]
    clean = [re.sub(r",?\s*[A-Z]{2}$", "", p).strip() for p in parts]
    clean = [c for c in clean if c]
    if len(clean) <= max_counties:
        return ", ".join(clean)
    return ", ".join(clean[:max_counties]) + f" +{len(clean) - max_counties}"


class KronosDashboard(App):
    """KRONOS-WX real-time TUI dashboard."""

    TITLE = "KRONOS-WX"
    BINDINGS = [
        ("r", "refresh_all", "Refresh All"),
        ("q", "quit", "Quit"),
    ]

    CSS = CSS

    # ── Mutable state (written from worker threads via call_from_thread) ───────
    _hrrr_base: Optional[object]   = None
    _hrrr_now:  Optional[object]   = None
    _risk_zones: list              = []
    _profile_oun: Optional[object] = None
    _indices_oun: Optional[object] = None
    _kinematics_oun: Optional[object] = None
    _indices_lmn: Optional[object] = None
    _kinematics_lmn: Optional[object] = None
    _ces: Optional[dict]           = None
    _moisture: Optional[object]    = None
    _dryline: Optional[object]     = None
    _dryline_prev: Optional[object] = None
    _dryline_surge: Optional[float] = None
    _spc_outlook: Optional[object] = None
    _nws_alerts: list              = []
    _spc_mds: list                 = []
    _prev_tier_map: dict           = {}
    _prev_md_numbers: set          = set()
    _prev_alert_keys: set          = set()
    _risk_table_ready: bool        = False

    def __init__(self, station: str = "OUN", **kwargs):
        super().__init__(**kwargs)
        self._station = station
        self._lock = threading.Lock()

    # ── Layout ────────────────────────────────────────────────────────────────

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        with Horizontal(id="top-row"):
            with Vertical(id="risk-panel"):
                yield DataTable(id="risk-table", cursor_type="none", show_header=True)
            yield Static("Loading environment…", id="env-panel")
            yield Static("Loading surface…", id="dryline-panel")
        yield Static("Loading tendency…", id="tendency-row")
        yield Static("Loading SPC…", id="spc-row")
        yield RichLog(id="alert-log", highlight=True, markup=True, wrap=True)
        yield Footer()

    def on_mount(self) -> None:
        self._alert_log = self.query_one("#alert-log", RichLog)

        # Set border titles per DESIGN.md
        self.query_one("#risk-panel").border_title = "Risk Zones"
        self.query_one("#env-panel", Static).border_title = "Environment"
        self.query_one("#dryline-panel", Static).border_title = "Surface / Dryline"
        self.query_one("#tendency-row", Static).border_title = "Tendency"
        self.query_one("#spc-row", Static).border_title = "SPC Products"
        self.query_one("#alert-log", RichLog).border_title = "Alert Log"

        # Set up DataTable columns for risk zones
        tbl = self.query_one("#risk-table", DataTable)
        tbl.add_columns("County", "Tier", "CAPE", "CIN", "SRH-1", "SRH-3", "EHI", "STP")
        self._risk_table_ready = True

        self._log(f"[bold cyan]KRONOS-WX started — station {self._station}[/bold cyan]")
        self.action_refresh_all()

        self.set_interval(5  * 60, self._fetch_surface)
        self.set_interval(15 * 60, self._fetch_hrrr)
        self.set_interval(15 * 60, self._fetch_spc)
        self.set_interval(60 * 60, self._fetch_environment)

    # ── Key actions ───────────────────────────────────────────────────────────

    def action_refresh_all(self) -> None:
        self._log("[dim]Manual refresh triggered[/dim]")
        self._fetch_environment()
        self._fetch_hrrr()
        self._fetch_surface()
        self._fetch_spc()

    # ── Logging helper ────────────────────────────────────────────────────────

    def _log(self, msg: str) -> None:
        now = datetime.now(tz=timezone.utc).strftime("%H:%MZ")
        self._alert_log.write(f"[dim]{now}[/dim]  {msg}")

    # ── Worker: HRRR snapshots + risk zones + tendency ────────────────────────

    @work(thread=True)
    def _fetch_hrrr(self) -> None:
        try:
            now = datetime.now(tz=timezone.utc)
            snap = None
            with HRRRClient() as hc:
                for h_back in range(3):
                    try:
                        snap = hc.get_county_snapshot(now - timedelta(hours=h_back))
                        if snap is not None:
                            break
                    except Exception:
                        continue

            if snap is None:
                self.call_from_thread(self._set_risk_loading,
                                      "[yellow]HRRR: no analysis posted yet[/yellow]")
                return

            with self._lock:
                if self._hrrr_base is None:
                    self._hrrr_base = snap
                self._hrrr_now = snap

            with self._lock:
                dryline = self._dryline

            zones = compute_risk_zones_from_hrrr(snap, dryline=dryline, min_tier="MARGINAL")
            with self._lock:
                self._risk_zones = zones

            self.call_from_thread(self._render_risk_zones, snap, zones)
            self.call_from_thread(self._render_tendency)
            self.call_from_thread(self._check_tier_changes, zones)

        except Exception as exc:
            logger.debug("HRRR fetch error: %s", exc, exc_info=True)
            self.call_from_thread(self._set_risk_loading, f"[red]HRRR error: {exc}[/red]")

    def _set_risk_loading(self, msg: str) -> None:
        """Show a status message in the risk panel when DataTable has no data."""
        # Clear the table and add a single informational row
        tbl = self.query_one("#risk-table", DataTable)
        tbl.clear()
        tbl.add_row(Text(msg, style="yellow"), "", "", "", "", "", "", "")

    # ── Worker: soundings + thermodynamic indices ─────────────────────────────

    @work(thread=True)
    def _fetch_environment(self) -> None:
        try:
            stn   = OklahomaSoundingStation[self._station]
            today = datetime.now(tz=timezone.utc).date()
            profile = None
            fetched_hour = None
            with SoundingClient() as sc:
                for h in (12, 0, 18, 21):
                    try:
                        p = sc.get_sounding(stn, today, h)
                        if p is not None:
                            profile = p
                            fetched_hour = h
                            break
                    except Exception:
                        continue

            if profile is None:
                self.call_from_thread(
                    self.query_one("#env-panel", Static).update,
                    "[yellow]Sounding: no data for today yet[/yellow]"
                )
                return

            idx = compute_thermodynamic_indices(profile)
            kin = compute_kinematic_profile(profile, idx)

            # Fetch LMN sounding for dual-column display (best-effort)
            lmn_idx = lmn_kin = None
            if stn == OklahomaSoundingStation.OUN and fetched_hour is not None:
                try:
                    with SoundingClient() as sc2:
                        lmn_profile = sc2.get_sounding(
                            OklahomaSoundingStation.LMN, today, fetched_hour
                        )
                    if lmn_profile is not None:
                        lmn_idx = compute_thermodynamic_indices(lmn_profile)
                        lmn_kin = compute_kinematic_profile(lmn_profile, lmn_idx)
                except Exception:
                    pass

            # CES projection for 12Z sounding
            ces = None
            with self._lock:
                moisture = self._moisture
            if fetched_hour == 12:
                try:
                    # Estimate surface T from moisture if available
                    surf_t_c = None
                    if moisture is not None:
                        td_f = moisture.state_mean_dewpoint_f
                        td_c = (td_f - 32.0) / 1.8
                        surf_t_c = td_c + 2.0 + idx.LCL_height * 0.0065
                    if surf_t_c is not None:
                        ces_traj = compute_ces_from_sounding(idx, surf_t_c, today)
                        if ces_traj is not None:
                            ces = {
                                "cap_behavior":           ces_traj.cap_behavior,
                                "convective_temp_gap_12Z": getattr(ces_traj, "convective_temp_gap_12Z", None),
                                "convective_temp_gap_18Z": getattr(ces_traj, "convective_temp_gap_18Z", None),
                                "erosion_hour":            getattr(ces_traj, "erosion_hour_utc", None),
                            }
                except Exception:
                    pass

            # Model predictions (best-effort; may not be trained yet)
            model_pred = self._load_model_predictions(idx, kin)

            with self._lock:
                self._profile_oun     = profile
                self._indices_oun     = idx
                self._kinematics_oun  = kin
                self._indices_lmn     = lmn_idx
                self._kinematics_lmn  = lmn_kin
                self._ces             = ces

            analogues = self._load_analogues(idx, kin)
            self.call_from_thread(
                self._render_environment, profile, idx, kin,
                lmn_idx, lmn_kin, ces, model_pred, analogues
            )

        except Exception as exc:
            logger.debug("Environment fetch error: %s", exc, exc_info=True)
            self.call_from_thread(
                self.query_one("#env-panel", Static).update,
                f"[red]Environment error: {exc}[/red]"
            )

    def _load_model_predictions(self, idx, kin) -> Optional[dict]:
        """Load severity + count predictions from trained models (best-effort)."""
        try:
            clf = load_model("severity_classifier")
            reg = load_model("tornado_regressor")
            if clf is None or reg is None:
                return None
            proba = clf.predict_proba(idx, kin)
            count = reg.predict(idx, kin)
            return {"proba": proba, "count": count}
        except Exception:
            return None

    def _load_analogues(self, idx, kin) -> list:
        """Load top-4 analogues from the case database (best-effort)."""
        try:
            import numpy as np

            feat = extract_features_from_indices(idx, kin)
            feat_vec = np.array([feat.get(f, float("nan")) for f in FEATURE_NAMES])

            with Database() as db:
                cases = db.list_cases()

            scored = []
            for case in cases:
                if case.sounding_12Z is None or case.kinematics_12Z is None:
                    continue
                try:
                    cf = extract_features_from_indices(case.sounding_12Z, case.kinematics_12Z)
                    cv = np.array([cf.get(f, float("nan")) for f in FEATURE_NAMES])
                    key_idx = [FEATURE_NAMES.index(k) for k in
                               ["MLCAPE", "MLCIN", "cap_strength", "SRH_0_3km", "BWD_0_6km", "EHI"]
                               if k in FEATURE_NAMES]
                    diff = feat_vec[key_idx] - cv[key_idx]
                    norms = np.array([3000, 200, 5, 500, 60, 10], dtype=float)
                    dist = float(np.sqrt(np.nansum((diff / norms) ** 2)))
                    scored.append((dist, case))
                except Exception:
                    continue

            scored.sort(key=lambda x: x[0])
            return [(d, c) for d, c in scored[:4]]
        except Exception:
            return []

    # ── Worker: Mesonet surface + dryline ─────────────────────────────────────

    @work(thread=True)
    def _fetch_surface(self) -> None:
        try:
            now = datetime.now(tz=timezone.utc)
            station_series: dict[str, MesonetTimeSeries] = {}
            current_obs: list = []
            current_snap_time: Optional[datetime] = None
            prev_snap_time: Optional[datetime] = None

            with MesonetClient() as mc:
                for h_back in (0, 1):
                    target = now - timedelta(hours=h_back)
                    for back_min in (0, 5, 10, 15, 20):
                        snap_time = target - timedelta(minutes=back_min)
                        try:
                            snap_obs = mc.get_snapshot_observations(snap_time)
                        except Exception as exc:
                            if any(s in str(exc) for s in ("404", "Not Found", "400")):
                                continue
                            break
                        if not snap_obs:
                            continue
                        for o in snap_obs:
                            stid = o.station_id
                            if stid not in station_series:
                                station_series[stid] = MesonetTimeSeries(
                                    station_id=stid,
                                    county=o.county,
                                    start_time=snap_time,
                                    end_time=snap_time,
                                    observations=[],
                                )
                            station_series[stid].observations.append(o)
                            station_series[stid].end_time = max(
                                station_series[stid].end_time, snap_time
                            )
                        if h_back == 0 and current_snap_time is None:
                            current_obs = snap_obs
                            current_snap_time = snap_time
                        elif h_back == 1 and prev_snap_time is None:
                            prev_snap_time = snap_time
                        break

            if not station_series or current_snap_time is None:
                self.call_from_thread(
                    self.query_one("#dryline-panel", Static).update,
                    "[yellow]Mesonet: no recent observations available[/yellow]"
                )
                return

            moisture = compute_moisture_return(current_obs)

            with self._lock:
                prev_dl = self._dryline

            dl = detect_dryline(station_series, current_snap_time)

            # Compute surge rate from previous dryline observation
            surge: Optional[float] = None
            if prev_dl is not None and dl is not None:
                try:
                    surge = compute_dryline_surge_rate(prev_dl, dl)
                except Exception:
                    pass

            with self._lock:
                self._dryline_prev = prev_dl
                self._dryline      = dl
                self._dryline_surge = surge
                self._moisture     = moisture

            self.call_from_thread(self._render_dryline, moisture, dl, surge)

        except Exception as exc:
            logger.debug("Surface fetch error: %s", exc, exc_info=True)
            self.call_from_thread(
                self.query_one("#dryline-panel", Static).update,
                f"[red]Surface error: {exc}[/red]"
            )

    # ── Worker: SPC products ──────────────────────────────────────────────────

    @work(thread=True)
    def _fetch_spc(self) -> None:
        try:
            outlook  = fetch_spc_outlook()
            alerts   = fetch_active_watches_warnings()
            mds      = fetch_active_mds()

            with self._lock:
                self._spc_outlook  = outlook
                self._nws_alerts   = alerts or []
                self._spc_mds      = mds    or []

            self.call_from_thread(self._render_spc, outlook, alerts or [], mds or [])
            self.call_from_thread(self._check_spc_changes, alerts or [], mds or [])

        except Exception as exc:
            logger.debug("SPC fetch error: %s", exc, exc_info=True)
            self.call_from_thread(
                self.query_one("#spc-row", Static).update,
                f"[red]SPC error: {exc}[/red]"
            )

    # ── Render: risk zones (DataTable) ────────────────────────────────────────

    def _render_risk_zones(self, snap, zones) -> None:
        if not self._risk_table_ready:
            return

        vt = getattr(snap, "valid_time", None)
        ts = vt.strftime("%H:%MZ") if vt else "?Z"
        self.query_one("#risk-panel").border_title = f"Risk Zones  [dim]{ts} HRRR[/dim]"

        tbl = self.query_one("#risk-table", DataTable)
        tbl.clear()

        if not zones:
            tbl.add_row(Text("No elevated risk", style="dim white"),
                        "", "", "", "", "", "", "")
            return

        for zone in zones:
            style = _TIER_COLOR.get(zone.tier, "white")
            # Build per-county rows (one per county, collapsed to zone for brevity)
            for county in zone.counties:
                pt = snap.get(county) if snap else None
                if pt is None:
                    continue
                ehi_s = f"{pt.EHI:.2f}" if pt.EHI is not None else "—"
                stp_s = f"{pt.STP:.2f}" if hasattr(pt, "STP") and pt.STP is not None else "—"
                cape_v = pt.MLCAPE
                cin_v  = pt.MLCIN

                cape_c = ("bright_red" if cape_v >= 3000 else "red" if cape_v >= 2000
                          else "yellow" if cape_v >= 1000 else "white")
                cin_c  = ("bright_red" if cin_v >= 200 else "red" if cin_v >= 100
                          else "yellow" if cin_v >= 50 else "green")

                tbl.add_row(
                    Text(county.name, style=style),
                    Text(zone.tier, style=style),
                    Text(f"{cape_v:.0f}", style=cape_c),
                    Text(f"{cin_v:.0f}", style=cin_c),
                    Text(f"{pt.SRH_0_1km:.0f}"),
                    Text(f"{pt.SRH_0_3km:.0f}"),
                    Text(ehi_s),
                    Text(stp_s),
                )

    # ── Render: environment (OUN + LMN dual-column) ───────────────────────────

    def _render_environment(self, profile, idx, kin,
                             lmn_idx, lmn_kin, ces, model_pred, analogues) -> None:
        vt = getattr(profile, "valid_time", None)
        ts = vt.strftime("%H:%MZ") if vt else "?Z"
        stn = getattr(profile, "station", self._station)
        stn_name = stn.value if hasattr(stn, "value") else str(stn)

        self.query_one("#env-panel", Static).border_title = f"Environment  [dim]{stn_name} {ts}[/dim]"

        has_lmn = lmn_idx is not None and lmn_kin is not None
        w = 8  # column width

        def _r(v, fmt=".0f"):
            if v is None or (isinstance(v, float) and math.isnan(v)):
                return "—"
            return format(float(v), fmt)

        def cin_c(v):
            return "bright_red" if v >= 200 else "red" if v >= 100 else "yellow" if v >= 50 else "green"
        def cape_c(v):
            return "bright_red" if v >= 3000 else "red" if v >= 2000 else "yellow" if v >= 1000 else "white"
        def srh_c(v):
            return "bright_red" if v >= 400 else "red" if v >= 250 else "yellow" if v >= 150 else "white"
        def ehi_c(v):
            return "bright_red" if v >= 4.0 else "red" if v >= 2.5 else "yellow" if v >= 1.5 else "white"

        def _cv(val, color_fn, fmt=".0f"):
            """Format value with color."""
            s = _r(val, fmt)
            if val is not None and not (isinstance(val, float) and math.isnan(val)):
                c = color_fn(float(val))
                return f"[{c}]{s}[/{c}]"
            return s

        def row(label, oun_val, lmn_val, fmt=".0f", color_fn=None):
            ov = _cv(oun_val, color_fn, fmt) if color_fn else _r(oun_val, fmt)
            if has_lmn:
                lv = _r(lmn_val, fmt)
                return f"{label:<16} {ov:>{w}}  {lv:>{w}}"
            return f"{label:<16} {ov:>{w}}"

        lines = []
        if has_lmn:
            lines.append(f"{'':16} {'OUN':>{w}}  {'LMN':>{w}}")
            lines.append("─" * (16 + w * 2 + 4))
        else:
            lines.append(f"{'':16} {'OUN':>{w}}")
            lines.append("─" * (16 + w + 2))

        lines.append(row("MLCAPE (J/kg)",  idx.MLCAPE,         getattr(lmn_idx, "MLCAPE", None),         ".0f", cape_c))
        lines.append(row("MLCIN  (J/kg)",  idx.MLCIN,          getattr(lmn_idx, "MLCIN", None),          ".0f", cin_c))
        lines.append(row("SBCAPE (J/kg)",  getattr(idx, "SBCAPE", None),  getattr(lmn_idx, "SBCAPE", None)))
        lines.append(row("Cap    (°C)",    idx.cap_strength,   getattr(lmn_idx, "cap_strength", None),   ".1f"))
        lines.append(row("LCL    (m)",     idx.LCL_height,     getattr(lmn_idx, "LCL_height", None)))
        lines.append(row("LFC    (m)",     idx.LFC_height,     getattr(lmn_idx, "LFC_height", None)))
        lines.append("")
        lines.append(row("SRH 0-1 (m²/s²)", kin.SRH_0_1km,   getattr(lmn_kin, "SRH_0_1km", None),     ".0f", srh_c))
        lines.append(row("SRH 0-3 (m²/s²)", kin.SRH_0_3km,   getattr(lmn_kin, "SRH_0_3km", None),     ".0f", srh_c))
        lines.append(row("Shear 0-6 (kt)",  kin.BWD_0_6km,    getattr(lmn_kin, "BWD_0_6km", None)))
        lines.append(row("EHI",             kin.EHI,          getattr(lmn_kin, "EHI", None),           ".2f", ehi_c))
        lines.append(row("STP",             kin.STP,          getattr(lmn_kin, "STP", None),           ".2f"))

        # ── CES projection ────────────────────────────────────────────────────
        if ces:
            lines.append("\n[bold]── CES Projection ──[/bold]")
            cb = ces.get("cap_behavior")
            cb_str = cb.value if cb is not None and hasattr(cb, "value") else str(cb) if cb else "—"
            cb_color = {
                "CLEAN_EROSION":    "bright_green",
                "EARLY_EROSION":    "green",
                "LATE_EROSION":     "yellow",
                "NO_EROSION":       "red",
                "BOUNDARY_FORCED":  "cyan",
            }.get(cb_str, "white")
            lines.append(f"[{cb_color}]{cb_str}[/{cb_color}]")
            erosion_h = ces.get("erosion_hour")
            if erosion_h is not None:
                lines.append(f"Erosion hour: [cyan]{erosion_h:02d}Z[/cyan]")
            for key, label_s in [
                ("convective_temp_gap_12Z", "12Z Tc gap"),
                ("convective_temp_gap_18Z", "18Z Tc gap"),
            ]:
                val = ces.get(key)
                if val is not None:
                    gc = "green" if val < 0 else ("yellow" if val < 10 else "red")
                    lines.append(f"{label_s}: [{gc}]{val:+.1f}°F[/{gc}]")

        # ── Model predictions ─────────────────────────────────────────────────
        if model_pred is not None:
            lines.append("\n[bold]── Model Forecast ──[/bold]")
            proba = model_pred.get("proba", {})
            count = model_pred.get("count", {})
            sig_pct = proba.get("significant", 0) * 100
            sig_c = "bright_red" if sig_pct >= 60 else "red" if sig_pct >= 40 else "yellow" if sig_pct >= 20 else "green"
            lines.append(f"Severity: [{sig_c}]{sig_pct:.0f}% SIGNIFICANT[/{sig_c}]")
            if count:
                exp = count.get("expected_count", 0)
                lo  = count.get("interval_low", 0)
                hi  = count.get("interval_high", 0)
                lines.append(f"Count:    {exp:.0f} expected  (80% PI: {lo:.0f}–{hi:.0f})")
        else:
            lines.append("\n[dim]Models not trained — run train-models[/dim]")

        # ── Modified indices with live Mesonet moisture ───────────────────────
        with self._lock:
            moisture = self._moisture
        if moisture is not None:
            try:
                td_f = moisture.state_mean_dewpoint_f
                td_c_val = (td_f - 32.0) / 1.8
                surf_t_c = idx.LCL_height * 0.0065 + td_c_val + 2.0
                mod = compute_modified_indices(profile, surf_t_c, td_c_val)
                if mod is not None:
                    mcape_c = "bright_red" if mod.MLCAPE > 2500 else "red" if mod.MLCAPE > 1500 else "yellow"
                    lines.append(
                        f"\n[bold]Modified (Mesonet Td={td_f:.0f}°F)[/bold]\n"
                        f"  Mod MLCAPE [{mcape_c}]{mod.MLCAPE:.0f}[/{mcape_c}] J/kg\n"
                        f"  Mod MLCIN  {mod.MLCIN:.0f} J/kg"
                    )
            except Exception:
                pass

        # ── Top analogues ─────────────────────────────────────────────────────
        if analogues:
            lines.append("\n[bold]Top Analogues[/bold]")
            for dist, case in analogues:
                cls_s = case.event_class.value if case.event_class else "?"
                cls_c = "bright_red" if "SIGNIFICANT" in cls_s else "yellow"
                lines.append(
                    f"  {case.case_id}  "
                    f"[{cls_c}]{cls_s.replace('_OUTBREAK','').replace('_',' ')}[/{cls_c}]  "
                    f"dist={dist:.3f}  n={case.tornado_count}"
                )

        self.query_one("#env-panel", Static).update("\n".join(lines))

    # ── Render: dryline / moisture ────────────────────────────────────────────

    def _render_dryline(self, moisture, dryline, surge: Optional[float]) -> None:
        now = datetime.now(tz=timezone.utc).strftime("%H:%MZ")
        self.query_one("#dryline-panel", Static).border_title = f"Surface / Dryline  [dim]{now}[/dim]"

        td_f  = moisture.state_mean_dewpoint_f
        grad  = moisture.moisture_return_gradient_f
        gf    = moisture.gulf_moisture_fraction
        ns    = moisture.n_stations
        td_c  = "bright_red" if td_f >= 65 else "red" if td_f >= 60 else "yellow" if td_f >= 55 else "white"
        gf_c  = "bright_red" if gf >= 0.5 else "red" if gf >= 0.3 else "yellow" if gf >= 0.1 else "dim white"

        lines = [
            "[bold]MOISTURE RETURN[/bold]",
            f"  Td mean  [{td_c}]{td_f:.1f}[/{td_c}] °F",
            f"  S/N grad {grad:+.1f} °F",
            f"  Gulf cov [{gf_c}]{gf*100:.0f}%[/{gf_c}]",
            f"  Stations {ns}",
        ]

        if dryline is not None:
            conf_c = "green" if dryline.confidence > 0.7 else "yellow" if dryline.confidence > 0.4 else "dim white"
            lats = dryline.position_lat
            lons = dryline.position_lon
            if lats and lons:
                pos_s = (
                    f"{lats[0]:.1f}°N/{abs(lons[0]):.1f}°W → "
                    f"{lats[-1]:.1f}°N/{abs(lons[-1]):.1f}°W"
                )
            else:
                pos_s = "detected"
            lines += ["", "[bold]DRYLINE[/bold]", f"  Position {pos_s}",
                      f"  Conf     [{conf_c}]{dryline.confidence:.2f}[/{conf_c}]"]
            if surge is not None:
                direction = "eastward" if surge > 0 else "retrograding"
                sc = "red" if abs(surge) > 15 else "yellow" if abs(surge) > 5 else "green"
                lines.append(f"  Surge    [{sc}]{surge:+.0f} mph ({direction})[/{sc}]")
            counties = ", ".join(c.name for c in (dryline.counties_intersected or [])[:4])
            if counties:
                lines.append(f"  Counties {counties}")
        else:
            lines += ["", "[dim]No dryline detected[/dim]"]

        self.query_one("#dryline-panel", Static).update("\n".join(lines))

    # ── Render: tendency (composite scoring → ▲▲/▲/→/▼) ─────────────────────

    def _render_tendency(self) -> None:
        with self._lock:
            base  = self._hrrr_base
            now   = self._hrrr_now
            zones = self._risk_zones

        if base is None or now is None or not zones:
            self.query_one("#tendency-row", Static).update(
                "[dim]Waiting for two HRRR snapshots…[/dim]"
            )
            return

        vt_b = getattr(base, "valid_time", None)
        vt_n = getattr(now,  "valid_time", None)
        ts_b = vt_b.strftime("%H:%MZ") if vt_b else "?"
        ts_n = vt_n.strftime("%H:%MZ") if vt_n else "?"
        self.query_one("#tendency-row", Static).border_title = f"Tendency  [dim]{ts_b} → {ts_n}[/dim]"

        def _sign(v: float) -> str:
            return f"+{v:.0f}" if v >= 0 else f"{v:.0f}"

        lines = [
            f"{'County':<13}{'Tier':<20}{'ΔCIN':>6}{'ΔCAPE':>7}{'ΔSRH-1':>7}{'ΔSRH-3':>7}{'ΔEHI':>6}  Trend",
            "─" * 72,
        ]

        threat_counties: list = []
        seen: set = set()
        for zone in zones:
            if _TIER_RANK.get(zone.tier, 0) < 2:
                continue
            for county in zone.counties:
                if county in seen:
                    continue
                seen.add(county)
                pt_b = base.get(county) if base else None
                pt_n = now.get(county)  if now  else None
                if pt_b and pt_n:
                    threat_counties.append((zone.tier, county, pt_b, pt_n))

        if not threat_counties:
            lines.append("[dim]No threat counties above MODERATE[/dim]")
        else:
            for tier, county, pt_b, pt_n in threat_counties[:10]:
                d_cin  = pt_n.MLCIN  - pt_b.MLCIN
                d_cape = pt_n.MLCAPE - pt_b.MLCAPE
                d_srh1 = pt_n.SRH_0_1km - pt_b.SRH_0_1km
                d_srh3 = pt_n.SRH_0_3km - pt_b.SRH_0_3km
                d_ehi  = (pt_n.EHI or 0.0) - (pt_b.EHI or 0.0)

                cin_c  = "green" if d_cin  <= -10 else "red"    if d_cin  >= 10  else "yellow"
                srh_c  = "green" if d_srh1 >=  20 else "red"    if d_srh1 <= -20 else "yellow"
                cape_c = "green" if d_cape >= 200  else "red"   if d_cape <= -200 else "yellow"

                # Composite score → trend arrow (DESIGN.md formula)
                score = (
                    (1 if d_cin  <= -10 else 0) + (1 if d_cin  <= -30 else 0) +
                    (1 if d_srh1 >=  20 else 0) + (1 if d_cape >= 200  else 0) +
                    (-1 if d_cin  >= 10  else 0) + (-1 if d_srh1 <= -20 else 0)
                )
                arrow = (
                    "[bright_green]▲▲[/bright_green]" if score >= 3 else
                    "[green]▲[/green]"                if score == 2 else
                    "[yellow]→[/yellow]"              if score >= 0 else
                    "[red]▼[/red]"
                )

                style = _TIER_COLOR.get(tier, "white")
                lines.append(
                    f"[{style}]{county.name:<13}[/{style}]"
                    f"[{style}]{tier:<20}[/{style}]"
                    f"[{cin_c}]{d_cin:>+5.0f}[/{cin_c}]"
                    f"[{cape_c}]{d_cape:>+6.0f}[/{cape_c}]"
                    f"[{srh_c}]{d_srh1:>+6.0f}[/{srh_c}]"
                    f"[{srh_c}]{d_srh3:>+6.0f}[/{srh_c}]"
                    f"{d_ehi:>+5.2f}  {arrow}"
                )

        self.query_one("#tendency-row", Static).update("\n".join(lines))

    # ── Render: SPC products (categorized) ───────────────────────────────────

    _CAT_COLOR = {
        "HIGH":  "bright_red",
        "PDS":   "bright_red",
        "MDT":   "red",
        "ENH":   "yellow",
        "SLGT":  "yellow",
        "MRGL":  "green",
        "TSTM":  "dim white",
        "NONE":  "dim white",
    }

    def _render_spc(self, outlook, alerts, mds) -> None:
        now_s = datetime.now(tz=timezone.utc).strftime("%H:%MZ")
        self.query_one("#spc-row", Static).border_title = f"SPC Products  [dim]{now_s}[/dim]"

        lines: list[str] = []

        # ── D1 Outlook ────────────────────────────────────────────────────────
        if outlook:
            cat = outlook.category or "?"
            cat_c = self._CAT_COLOR.get(cat, "white")
            torn_pct = int((outlook.max_tornado_prob or 0) * 100)
            hatch_s = "  [bold bright_red]⚠ SIG TOR[/bold bright_red]" if outlook.sig_tornado_hatched else ""
            lines.append(f"D1: [{cat_c}]{cat}[/{cat_c}]  torn {torn_pct}%{hatch_s}")
        else:
            lines.append("[dim]D1 Outlook: unavailable[/dim]")

        # ── Tornado Warnings ──────────────────────────────────────────────────
        tor_warnings  = [a for a in alerts if "Tornado Warning" in a.event]
        tor_watches   = [a for a in alerts if "Tornado Watch"   in a.event]
        svr_warnings  = [a for a in alerts if "Severe Thunderstorm Warning" in a.event]

        if tor_warnings:
            for a in tor_warnings:
                counties = _abbrev_counties(a.area_desc, max_counties=4)
                exp_s = getattr(a, "expires_label", "")
                lines.append(
                    f"[bold bright_red]TORNADO WARNING[/bold bright_red]"
                    f"  {counties}  exp {exp_s}"
                )
        if tor_watches:
            for a in tor_watches:
                num_str  = f" #{a.watch_number}" if getattr(a, "watch_number", None) else ""
                counties = _abbrev_counties(a.area_desc, max_counties=8)
                exp_s    = getattr(a, "expires_label", "")
                lines.append(
                    f"[bold red]TORNADO WATCH{num_str}[/bold red]"
                    f"  {counties}  exp {exp_s}"
                )
        if svr_warnings:
            counties = _abbrev_counties(
                "; ".join(a.area_desc for a in svr_warnings), max_counties=6
            )
            lines.append(
                f"[yellow]{len(svr_warnings)} SVR TSTM WARNING{'S' if len(svr_warnings)>1 else ''}[/yellow]"
                f"  {counties}"
            )
        if not tor_warnings and not tor_watches and not svr_warnings:
            lines.append("[dim]No active warnings or watches[/dim]")

        # ── Mesoscale Discussions ─────────────────────────────────────────────
        if mds:
            for md in mds:
                md_num = getattr(md, "md_number", None) or getattr(md, "number", "?")
                pct    = getattr(md, "prob_watch_next_2h", None) or 0
                pct_c  = "bright_red" if pct >= 60 else "red" if pct >= 40 else "yellow"
                hl     = (getattr(md, "headline", None) or "")[:60]
                lines.append(
                    f"[cyan]MD #{md_num}[/cyan]  [{pct_c}]{pct}% watch[/{pct_c}]  {hl}"
                )
        else:
            lines.append("[dim]No active MDs[/dim]")

        self.query_one("#spc-row", Static).update("\n".join(lines))

    # ── Alert diffing ─────────────────────────────────────────────────────────

    def _check_tier_changes(self, zones) -> None:
        new_map: dict = {}
        for zone in zones:
            for county in zone.counties:
                new_map[county] = zone.tier

        with self._lock:
            prev = dict(self._prev_tier_map)
            self._prev_tier_map = new_map

        for county, tier in new_map.items():
            old_tier = prev.get(county, "LOW")
            if old_tier == tier:
                continue
            old_rank = _TIER_RANK.get(old_tier, 0)
            new_rank = _TIER_RANK.get(tier, 0)
            if max(old_rank, new_rank) < 2:
                continue
            style     = _TIER_COLOR.get(tier, "white")
            old_style = _TIER_COLOR.get(old_tier, "dim white")
            arrow = "▲" if new_rank > old_rank else "▼"
            self._log(
                f"[{style}]{arrow} {county.name}[/{style}]  "
                f"[{old_style}]{old_tier}[/{old_style}] → "
                f"[{style}]{tier}[/{style}]"
            )

    def _check_spc_changes(self, alerts, mds) -> None:
        with self._lock:
            prev_alerts = set(self._prev_alert_keys)
            prev_mds    = set(self._prev_md_numbers)

        new_alert_keys: set = set()
        for a in alerts:
            key = f"{a.event}:{getattr(a, 'watch_number', '')}:{a.area_desc}"
            new_alert_keys.add(key)
            if key not in prev_alerts:
                if "Tornado Warning" in a.event:
                    counties = _abbrev_counties(a.area_desc, max_counties=4)
                    self._log(
                        f"[bold bright_red]🌪 TORNADO WARNING[/bold bright_red]  "
                        f"{counties}  exp {getattr(a, 'expires_label', '')}"
                    )
                elif "Tornado Watch" in a.event:
                    num_str = f" #{a.watch_number}" if getattr(a, "watch_number", None) else ""
                    self._log(
                        f"[bold red]📋 TORNADO WATCH{num_str} issued[/bold red]  "
                        f"exp {getattr(a, 'expires_label', '')}"
                    )

        new_md_numbers: set = set()
        for md in mds:
            md_num = getattr(md, "md_number", None) or getattr(md, "number", None)
            if md_num is not None:
                new_md_numbers.add(md_num)
                if md_num not in prev_mds:
                    concerning = getattr(md, "concerning", None) or getattr(md, "headline", None) or "active"
                    self._log(f"[bold cyan]📋 NEW MD #{md_num}: {concerning[:80]}[/bold cyan]")

        with self._lock:
            self._prev_alert_keys  = new_alert_keys
            self._prev_md_numbers  = new_md_numbers
