"""
KRONOS-WX TUI Dashboard — real-time severe weather situational awareness.

Layout (120+ column terminal):
  ┌─ Risk Zones ──────────┬─ Environment ──┬─ Dryline/Moisture ──┐
  │ HRRR county tiers     │ OUN/LMN indices│ Mesonet surface     │
  │ (refresh 15 min)      │ (refresh 60min)│ (refresh 5 min)     │
  ├─── Tendency ──────────────────────────────────────────────────┤
  │ ΔMLCIN / ΔCAPE / ΔSRH per threat county (15 min)              │
  ├─── SPC Products ──────────────────────────────────────────────┤
  │ Outlook / Watches / MDs (15 min)                              │
  ├─── Alert Log ─────────────────────────────────────────────────┤
  │ Scrolling tier changes and alert events                       │
  └───────────────────────────────────────────────────────────────┘

Key bindings: R = refresh all, Q = quit
"""
from __future__ import annotations

import logging
import threading
from datetime import datetime, timedelta, timezone
from typing import Optional

from rich.table import Table
from rich.text import Text
from textual import work
from textual.app import App, ComposeResult
from textual.containers import Horizontal, Vertical
from textual.reactive import reactive

# ── Pipeline imports at module level (avoids import-lock deadlocks in threads) ─
from ok_weather_model.ingestion import HRRRClient, MesonetClient, SoundingClient
from ok_weather_model.ingestion.spc_products import (
    fetch_spc_outlook,
    fetch_active_watches_warnings,
    fetch_active_mds,
)
from ok_weather_model.models import OklahomaSoundingStation
from ok_weather_model.modeling import (
    extract_features_from_indices,
    FEATURE_NAMES,
)
from ok_weather_model.processing import (
    compute_thermodynamic_indices,
    compute_kinematic_profile,
    compute_moisture_return,
    compute_modified_indices,
)
from ok_weather_model.processing.dryline_detector import detect_dryline
from ok_weather_model.processing.risk_zone import compute_risk_zones_from_hrrr
from ok_weather_model.storage import Database
from textual.widgets import Footer, Header, RichLog, Static

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
_TIER_RANK = {
    "EXTREME": 5, "HIGH": 4, "DANGEROUS_CAPPED": 3,
    "MODERATE": 2, "MARGINAL": 1, "LOW": 0,
}

CSS = """
Screen {
    background: #050510;
    color: #d0d8e8;
}

#header-bar {
    height: 1;
    background: #0e2040;
    color: #7ec8e3;
    text-style: bold;
    content-align: center middle;
    dock: top;
}

#top-row {
    height: 1fr;
}

#risk-panel {
    width: 2fr;
    border: solid #1e3a5f;
    padding: 0 1;
    overflow-y: scroll;
}

#env-panel {
    width: 1fr;
    border: solid #1e3a5f;
    padding: 0 1;
    overflow-y: scroll;
}

#dryline-panel {
    width: 1fr;
    border: solid #1e3a5f;
    padding: 0 1;
    overflow-y: scroll;
}

#tendency-row {
    height: 8;
    border: solid #1e3a5f;
    padding: 0 1;
    overflow-x: scroll;
}

#spc-row {
    height: 5;
    border: solid #2d1b5f;
    padding: 0 1;
}

#alert-log {
    height: 10;
    border: solid #1e3a5f;
}

Footer {
    height: 1;
    background: #0e2040;
}
"""


class KronosDashboard(App):
    """KRONOS-WX real-time TUI dashboard."""

    TITLE = "KRONOS-WX"
    BINDINGS = [
        ("r", "refresh_all", "Refresh All"),
        ("q", "quit", "Quit"),
    ]

    CSS = CSS

    # ── Mutable state (written from worker threads via call_from_thread) ───────
    _hrrr_base: Optional[object]  = None   # first snapshot (baseline)
    _hrrr_now:  Optional[object]  = None   # most recent snapshot
    _risk_zones: list             = []
    _profile_oun: Optional[object] = None  # raw SoundingProfile
    _indices_oun: Optional[object] = None
    _kinematics_oun: Optional[object] = None
    _moisture: Optional[object]   = None
    _dryline: Optional[object]    = None
    _dryline_prev: Optional[object] = None
    _spc_outlook: Optional[object] = None
    _nws_alerts: list             = []
    _spc_mds: list                = []
    _prev_tier_map: dict          = {}      # county → tier (for diff)
    _prev_md_numbers: set         = set()
    _prev_alert_keys: set         = set()

    def __init__(self, station: str = "OUN", **kwargs):
        super().__init__(**kwargs)
        self._station = station
        self._lock = threading.Lock()

    # ── Layout ────────────────────────────────────────────────────────────────

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        with Horizontal(id="top-row"):
            yield Static("Loading risk zones…", id="risk-panel")
            yield Static("Loading environment…", id="env-panel")
            yield Static("Loading surface…", id="dryline-panel")
        yield Static("Loading tendency…", id="tendency-row")
        yield Static("Loading SPC…", id="spc-row")
        yield RichLog(id="alert-log", highlight=True, markup=True, wrap=True)
        yield Footer()

    def on_mount(self) -> None:
        self._alert_log = self.query_one("#alert-log", RichLog)
        self._log(f"[bold cyan]KRONOS-WX dashboard started — station {self._station}[/bold cyan]")
        # Kick off all four data workers immediately
        self.action_refresh_all()
        # Set independent timers
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
            with HRRRClient() as hc:
                snap = hc.get_county_snapshot(now)

            if snap is None:
                self.call_from_thread(
                    self.query_one("#risk-panel", Static).update,
                    "[red]HRRR unavailable[/red]"
                )
                return

            with self._lock:
                if self._hrrr_base is None:
                    self._hrrr_base = snap
                self._hrrr_now = snap

            zones = compute_risk_zones_from_hrrr(snap, min_tier="MARGINAL")
            with self._lock:
                self._risk_zones = zones

            self.call_from_thread(self._render_risk_zones, snap, zones)
            self.call_from_thread(self._render_tendency)
            self.call_from_thread(self._check_tier_changes, zones)

        except Exception as exc:
            logger.debug("HRRR fetch error: %s", exc, exc_info=True)
            self.call_from_thread(
                self.query_one("#risk-panel", Static).update,
                f"[red]HRRR error: {exc}[/red]"
            )

    # ── Worker: soundings + thermodynamic indices ─────────────────────────────

    @work(thread=True)
    def _fetch_environment(self) -> None:
        try:
            stn = OklahomaSoundingStation[self._station]
            with SoundingClient() as sc:
                profile = sc.get_latest_sounding(stn)

            if profile is None:
                self.call_from_thread(
                    self.query_one("#env-panel", Static).update,
                    "[red]Sounding unavailable[/red]"
                )
                return

            idx = compute_thermodynamic_indices(profile)
            kin = compute_kinematic_profile(profile, idx)

            with self._lock:
                self._profile_oun    = profile
                self._indices_oun    = idx
                self._kinematics_oun = kin

            # Try to get analogues
            analogues = self._load_analogues(idx, kin)

            self.call_from_thread(self._render_environment, profile, idx, kin, analogues)

        except Exception as exc:
            logger.debug("Environment fetch error: %s", exc, exc_info=True)
            self.call_from_thread(
                self.query_one("#env-panel", Static).update,
                f"[red]Environment error: {exc}[/red]"
            )

    def _load_analogues(self, idx, kin) -> list:
        """Load top-5 analogues from the case database (best-effort)."""
        try:
            import numpy as np
            import pandas as pd

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
                    # Euclidean distance on normalised key features
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
            return [(d, c) for d, c in scored[:5]]
        except Exception:
            return []

    # ── Worker: Mesonet surface + dryline ─────────────────────────────────────

    @work(thread=True)
    def _fetch_surface(self) -> None:
        try:
            now = datetime.now(tz=timezone.utc)
            obs = None
            # Walk back in 5-min steps — latest file may not be posted yet
            for back_min in (0, 5, 10, 15, 20):
                try:
                    with MesonetClient() as mc:
                        obs = mc.get_snapshot_observations(now - timedelta(minutes=back_min))
                    if obs:
                        break
                except Exception as exc:
                    if "404" in str(exc) or "Not Found" in str(exc) or "400" in str(exc):
                        continue
                    raise

            if not obs:
                self.call_from_thread(
                    self.query_one("#dryline-panel", Static).update,
                    "[dim]Mesonet: no recent file available[/dim]"
                )
                return

            moisture = compute_moisture_return(obs)

            prev_dl = None
            with self._lock:
                prev_dl = self._dryline

            dl = detect_dryline(obs)

            with self._lock:
                self._dryline_prev = prev_dl
                self._dryline      = dl
                self._moisture     = moisture

            self.call_from_thread(self._render_dryline, moisture, dl)

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

    # ── Render: risk zones ────────────────────────────────────────────────────

    def _render_risk_zones(self, snap, zones) -> None:
        vt = getattr(snap, "valid_time", None)
        ts = vt.strftime("%H:%MZ") if vt else "?Z"

        lines: list[str] = [f"[bold cyan]RISK ZONES[/bold cyan]  [dim]{ts} HRRR[/dim]\n"]

        if not zones:
            lines.append("[dim]No elevated risk[/dim]")
        else:
            for zone in zones:
                style = _TIER_COLOR.get(zone.tier, "white")
                cnames = ", ".join(c.name for c in zone.counties[:5])
                if len(zone.counties) > 5:
                    cnames += f" +{len(zone.counties)-5}"
                ehi_s = f"  EHI {zone.peak_EHI:.1f}" if zone.peak_EHI else ""
                lines.append(
                    f"[{style}]{zone.tier}[/{style}]\n"
                    f"  {cnames}\n"
                    f"  CAPE {zone.peak_MLCAPE:.0f}  CIN {zone.peak_MLCIN:.0f}"
                    f"  SRH1 {zone.peak_SRH_0_1km:.0f}{ehi_s}\n"
                )

        self.query_one("#risk-panel", Static).update("\n".join(lines))

    # ── Render: environment ───────────────────────────────────────────────────

    def _render_environment(self, profile, idx, kin, analogues) -> None:
        vt = getattr(profile, "valid_time", None)
        ts = vt.strftime("%H:%MZ") if vt else "?Z"
        stn = getattr(profile, "station", self._station)
        stn_name = stn.value if hasattr(stn, "value") else str(stn)

        def _r(v, fmt=".0f"):
            if v is None or v != v:  # nan check
                return "—"
            return format(float(v), fmt)

        lfc_s  = _r(idx.LFC_height)
        eml_s  = _r(idx.EML_depth) if idx.EML_depth else "—"
        ehi_s  = _r(kin.EHI,  ".2f") if kin.EHI  else "—"
        stp_s  = _r(kin.STP,  ".2f") if kin.STP  else "—"
        scp_s  = _r(kin.SCP,  ".1f") if kin.SCP  else "—"
        llj_s  = _r(kin.LLJ_speed) if kin.LLJ_speed else "—"

        # Colour-code by severity
        cape_c = "bright_red" if idx.MLCAPE > 2500 else "red" if idx.MLCAPE > 1500 else "yellow" if idx.MLCAPE > 500 else "white"
        cin_c  = "bright_red" if idx.MLCIN  > 150  else "red" if idx.MLCIN  > 75   else "yellow" if idx.MLCIN  > 30  else "green"
        srh_c  = "bright_red" if kin.SRH_0_3km > 400 else "red" if kin.SRH_0_3km > 250 else "yellow" if kin.SRH_0_3km > 150 else "white"
        sh_c   = "bright_red" if kin.BWD_0_6km > 55 else "red" if kin.BWD_0_6km > 40 else "yellow" if kin.BWD_0_6km > 25 else "white"

        txt = (
            f"[bold cyan]ENVIRONMENT[/bold cyan]  [dim]{stn_name} {ts}[/dim]\n\n"
            f"[bold]CAPE/CIN[/bold]\n"
            f"  MLCAPE  [{cape_c}]{idx.MLCAPE:.0f}[/{cape_c}] J/kg\n"
            f"  MLCIN   [{cin_c}]{idx.MLCIN:.0f}[/{cin_c}] J/kg\n"
            f"  SBCAPE  {idx.SBCAPE:.0f} J/kg\n"
            f"  MUCAPE  {idx.MUCAPE:.0f} J/kg\n\n"
            f"[bold]CAP / PARCEL[/bold]\n"
            f"  Cap     {idx.cap_strength:.1f} °C\n"
            f"  LCL     {idx.LCL_height:.0f} m\n"
            f"  LFC     {lfc_s} m\n"
            f"  EML     {eml_s} m\n\n"
            f"[bold]KINEMATICS[/bold]\n"
            f"  SRH 0-1 {kin.SRH_0_1km:.0f} m²/s²\n"
            f"  SRH 0-3 [{srh_c}]{kin.SRH_0_3km:.0f}[/{srh_c}] m²/s²\n"
            f"  Sh 0-6  [{sh_c}]{kin.BWD_0_6km:.0f}[/{sh_c}] kt\n\n"
            f"[bold]COMPOSITES[/bold]\n"
            f"  EHI     {ehi_s}\n"
            f"  STP     {stp_s}\n"
            f"  SCP     {scp_s}\n"
            f"  LLJ     {llj_s} kt\n"
        )

        # Modified indices using current Mesonet surface moisture
        with self._lock:
            moisture = self._moisture
        if moisture is not None:
            try:
                td_f  = moisture.state_mean_dewpoint_f
                td_c_val = (td_f - 32.0) / 1.8
                surf_t_c = idx.LCL_height * 0.0065 + td_c_val + 2.0  # rough surface T
                mod = compute_modified_indices(profile, surf_t_c, td_c_val)
                if mod is not None:
                    mcape_c = "bright_red" if mod.MLCAPE > 2500 else "red" if mod.MLCAPE > 1500 else "yellow"
                    txt += (
                        f"\n[bold]MODIFIED (Mesonet Td={td_f:.0f}°F)[/bold]\n"
                        f"  Mod MLCAPE [{mcape_c}]{mod.MLCAPE:.0f}[/{mcape_c}] J/kg\n"
                        f"  Mod MLCIN  {mod.MLCIN:.0f} J/kg\n"
                    )
            except Exception:
                pass

        if analogues:
            txt += "\n[bold]TOP ANALOGUES[/bold]\n"
            for dist, case in analogues[:4]:
                cls_s = case.event_class.value if case.event_class else "?"
                cls_c = "bright_red" if "SIGNIFICANT" in cls_s else "yellow"
                txt += (
                    f"  {case.case_id}  "
                    f"[{cls_c}]{cls_s.replace('_OUTBREAK','').replace('_',' ')}[/{cls_c}]  "
                    f"dist={dist:.3f}  n={case.tornado_count}\n"
                )

        self.query_one("#env-panel", Static).update(txt)

    # ── Render: dryline / moisture ────────────────────────────────────────────

    def _render_dryline(self, moisture, dryline) -> None:
        now = datetime.now(tz=timezone.utc).strftime("%H:%MZ")

        td_f  = moisture.state_mean_dewpoint_f
        grad  = moisture.moisture_return_gradient_f
        gf    = moisture.gulf_moisture_fraction
        ns    = moisture.n_stations
        td_c  = "bright_red" if td_f >= 65 else "red" if td_f >= 60 else "yellow" if td_f >= 55 else "white"
        gf_c  = "bright_red" if gf >= 0.5 else "red" if gf >= 0.3 else "yellow" if gf >= 0.1 else "dim white"

        txt = (
            f"[bold cyan]SURFACE / DRYLINE[/bold cyan]  [dim]{now}[/dim]\n\n"
            f"[bold]MOISTURE RETURN[/bold]\n"
            f"  Td mean  [{td_c}]{td_f:.1f}[/{td_c}] °F\n"
            f"  S/N grad {grad:+.1f} °F\n"
            f"  Gulf cov [{gf_c}]{gf*100:.0f}%[/{gf_c}]\n"
            f"  Stations {ns}\n"
        )

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
            counties = ", ".join(c.name for c in (dryline.counties_intersected or [])[:4])
            txt += (
                f"\n[bold]DRYLINE[/bold]\n"
                f"  Position {pos_s}\n"
                f"  Conf     [{conf_c}]{dryline.confidence:.2f}[/{conf_c}]\n"
            )
            if counties:
                txt += f"  Counties {counties}\n"
        else:
            txt += "\n[dim]No dryline detected[/dim]\n"

        self.query_one("#dryline-panel", Static).update(txt)

    # ── Render: tendency ──────────────────────────────────────────────────────

    def _render_tendency(self) -> None:
        with self._lock:
            base = self._hrrr_base
            now  = self._hrrr_now
            zones = self._risk_zones

        if base is None or now is None or not zones:
            self.query_one("#tendency-row", Static).update(
                "[bold cyan]TENDENCY[/bold cyan]  [dim]waiting for two snapshots…[/dim]"
            )
            return

        vt_b = getattr(base, "valid_time", None)
        vt_n = getattr(now,  "valid_time", None)
        ts_b = vt_b.strftime("%H:%MZ") if vt_b else "?"
        ts_n = vt_n.strftime("%H:%MZ") if vt_n else "?"

        def _arrow(delta: float, threshold: float = 0) -> str:
            if delta > threshold:  return "↑"
            if delta < -threshold: return "↓"
            return "→"

        def _sign(v: float) -> str:
            return f"+{v:.0f}" if v >= 0 else f"{v:.0f}"

        lines = [f"[bold cyan]TENDENCY[/bold cyan]  [dim]{ts_b} → {ts_n}[/dim]\n"]

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
            for tier, county, pt_b, pt_n in threat_counties[:8]:
                d_cin  = pt_n.MLCIN  - pt_b.MLCIN
                d_cape = pt_n.MLCAPE - pt_b.MLCAPE
                d_srh1 = pt_n.SRH_0_1km - pt_b.SRH_0_1km
                d_srh3 = pt_n.SRH_0_3km - pt_b.SRH_0_3km
                ehi_n  = pt_n.EHI or 0
                ehi_b  = pt_b.EHI or 0
                d_ehi  = ehi_n - ehi_b

                style = _TIER_COLOR.get(tier, "white")
                cin_c  = "red" if d_cin > 30 else "green" if d_cin < -30 else "white"
                cape_c = "green" if d_cape > 200 else "red" if d_cape < -200 else "white"
                srh_c  = "green" if d_srh3 > 50 else "red" if d_srh3 < -50 else "white"

                lines.append(
                    f"[{style}]{county.name:<14}[/{style}]  "
                    f"ΔCIN [{cin_c}]{_sign(d_cin):>6}[/{cin_c}] {_arrow(-d_cin, 20)}  "
                    f"ΔCAPE [{cape_c}]{_sign(d_cape):>6}[/{cape_c}] {_arrow(d_cape, 100)}  "
                    f"ΔSRH-1 [{srh_c}]{_sign(d_srh1):>5}[/{srh_c}]  "
                    f"ΔSRH-3 [{srh_c}]{_sign(d_srh3):>5}[/{srh_c}]  "
                    f"ΔEHI {_sign(d_ehi)}"
                )

        self.query_one("#tendency-row", Static).update("\n".join(lines))

    # ── Render: SPC products ──────────────────────────────────────────────────

    def _render_spc(self, outlook, alerts, mds) -> None:
        parts: list[str] = ["[bold magenta]SPC[/bold magenta]  "]

        if outlook:
            cat = outlook.category or "?"
            cat_c = {
                "PDS": "bold bright_red", "MDT": "bold red",
                "ENH": "red", "SLGT": "yellow",
                "MRGL": "green", "TSTM": "dim white",
            }.get(cat, "white")
            torn_pct = int((outlook.max_tornado_prob or 0) * 100)
            hatch_s  = "  [bold bright_red]⚠ SIG TOR[/bold bright_red]" if outlook.sig_tornado_hatched else ""
            parts.append(f"[{cat_c}]{cat}[/{cat_c}]  Torn {torn_pct}%{hatch_s}")
        else:
            parts.append("[dim]No outlook[/dim]")

        lines = ["".join(parts)]

        # Active watches / warnings
        tornado_alerts = [a for a in alerts if "tornado" in a.product_type.lower()]
        if tornado_alerts:
            for a in tornado_alerts:
                lines.append(f"  [bold bright_red]🌪  {a.headline}[/bold bright_red]")
        else:
            lines.append("  [dim]No active tornado watches/warnings[/dim]")

        # Active MDs
        if mds:
            for md in mds:
                pct = md.prob_watch_next_2h or 0
                pct_c = "bright_red" if pct >= 60 else "red" if pct >= 40 else "yellow"
                lines.append(
                    f"  [cyan]MD #{md.md_number}[/cyan]  [{pct_c}]{pct}% watch[/{pct_c}]  "
                    f"{(md.headline or '')[:60]}"
                )
        else:
            lines.append("  [dim]No active MDs[/dim]")

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
            if old_tier != tier:
                style = _TIER_COLOR.get(tier, "white")
                old_style = _TIER_COLOR.get(old_tier, "dim white")
                self._log(
                    f"[{style}]{county.name}[/{style}]  "
                    f"[{old_style}]{old_tier}[/{old_style}] → "
                    f"[{style}]{tier}[/{style}]"
                )

    def _check_spc_changes(self, alerts, mds) -> None:
        with self._lock:
            prev_alerts = set(self._prev_alert_keys)
            prev_mds    = set(self._prev_md_numbers)

        new_alert_keys = set()
        for a in alerts:
            key = f"{a.product_type}:{a.headline}"
            new_alert_keys.add(key)
            if key not in prev_alerts:
                self._log(f"[bold bright_red]🚨 NEW ALERT: {a.product_type} — {a.headline}[/bold bright_red]")

        new_md_numbers = set()
        for md in mds:
            new_md_numbers.add(md.md_number)
            if md.md_number not in prev_mds:
                self._log(
                    f"[bold cyan]📋 NEW MD #{md.md_number}: {(md.headline or '')[:80]}[/bold cyan]"
                )

        with self._lock:
            self._prev_alert_keys  = new_alert_keys
            self._prev_md_numbers  = new_md_numbers
