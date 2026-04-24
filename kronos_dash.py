"""
KRONOS-WX real-time TUI dashboard.

Four panes updating on independent timers:
  - Risk Zones     HRRR 3km county tiers, refreshes every 15 min
  - Environment    OUN + LMN sounding comparison + CES, refreshes every 60 min
  - Dryline        Mesonet surface network, refreshes every 5 min
  - Tendency       HRRR baseline vs current Δ, refreshes with HRRR
  - Alert log      Scrolling feed of all tier changes and dryline events

Run via:  python main.py dashboard
"""

from __future__ import annotations

import math
from datetime import datetime, timedelta, timezone
from typing import Optional

from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical
from textual.reactive import reactive
from textual.widgets import Footer, Header, RichLog, Static
from textual import work

# Import all ok_weather_model modules at the top level so Python's import lock
# is acquired and released once, before any @work(thread=True) workers start.
# Lazy imports inside threaded workers cause _DeadlockError when multiple
# workers fire simultaneously and race on the same _ModuleLock.
from ok_weather_model.ingestion import SoundingClient, MesonetClient, HRRRClient
from ok_weather_model.ingestion.spc_products import (
    fetch_active_mds,
    fetch_spc_outlook,
    MesoscaleDiscussion,
    SPCOutlook,
)
from ok_weather_model.models import OklahomaSoundingStation, OklahomaCounty
from ok_weather_model.models.mesonet import MesonetTimeSeries as _MTS
from ok_weather_model.processing import (
    compute_thermodynamic_indices,
    compute_kinematic_profile,
    detect_dryline,
    compute_dryline_surge_rate,
)
from ok_weather_model.processing.cap_calculator import compute_ces_from_sounding
from ok_weather_model.processing.risk_zone import (
    compute_risk_zones_from_hrrr,
    _TIER_RANK,
    _TIER_COLOR,
)


# ── Tier display helpers ──────────────────────────────────────────────────────
# _TIER_COLOR and _TIER_RANK are imported from ok_weather_model.processing.risk_zone

_TIER_DESC = {
    "EXTREME":
        "Near-zero cap · CAPE≥1500 · SRH1≥250 · EHI≥3.5\n"
        "Any storm that fires produces significant tornadoes.",
    "HIGH":
        "Manageable cap (CIN<100) · CAPE≥1000 · SRH1≥150 · EHI≥2.0\n"
        "Likely significant tornadoes with initiation.",
    "DANGEROUS_CAPPED":
        "Strong cap (CIN≥80) but violent kinematics (SRH1≥150 · EHI≥2.0)\n"
        "Boundary-forced initiation can produce violent tornadoes with minimal warning.",
    "MODERATE":
        "Some potential if cap erodes · CIN<200 · CAPE≥500 · SRH1≥100\n"
        "Organized convection possible; significant tornado threat limited.",
    "MARGINAL":
        "Isolated storm potential · CIN<250 · CAPE≥200 · SRH1≥50\n"
        "Weakly forced; storms possible but not likely significant.",
}


def _fmt(val, fmt: str, fallback: str = "—") -> str:
    if val is None or (isinstance(val, float) and math.isnan(val)):
        return fallback
    return fmt.format(val)


# ── Panels ────────────────────────────────────────────────────────────────────

class RiskZonePanel(Static):
    DEFAULT_CSS = """
    RiskZonePanel {
        border: solid $primary;
        height: 1fr;
        padding: 0 1;
        overflow-y: auto;
    }
    """

    def render_zones(self, zones: list, hrrr_label: str) -> None:
        lines = [f"[bold cyan]RISK ZONES[/bold cyan]  HRRR {hrrr_label}\n"]
        if not zones:
            lines.append("[dim]No elevated risk areas identified.[/dim]")
        else:
            for zone in zones:
                color = _TIER_COLOR.get(zone.tier, "white")
                names = ", ".join(c.name for c in zone.counties[:5])
                if len(zone.counties) > 5:
                    names += f" +{len(zone.counties) - 5}"
                center = f"{zone.center_lat:.1f}°N {abs(zone.center_lon):.1f}°W"
                lines.append(
                    f"[{color}]{zone.tier:<18}[/{color}]  {names}\n"
                    f"  [dim]{center}  "
                    f"CAPE {zone.peak_MLCAPE:.0f}  CIN {zone.peak_MLCIN:.0f}  "
                    f"SRH1 {zone.peak_SRH_0_1km:.0f}  EHI {zone.peak_EHI:.2f}[/dim]"
                )
        self.update("\n".join(lines))


class EnvironmentPanel(Static):
    DEFAULT_CSS = """
    EnvironmentPanel {
        border: solid $primary;
        height: 1fr;
        padding: 0 1;
        overflow-y: auto;
    }
    """

    def render_environment(
        self, indices, kinematics, lmn_indices, lmn_kinematics,
        ces: Optional[dict], snd_label: str
    ) -> None:
        lines = [f"[bold cyan]ENVIRONMENT[/bold cyan]  {snd_label}\n"]

        w = 9  # column width
        lines.append(f"{'':16} {'OUN':>{w}}  {'LMN':>{w}}")
        lines.append("─" * (16 + w * 2 + 4))

        def row(label, oun_val, lmn_val, fmt, color_fn=None):
            ov = _fmt(oun_val, fmt)
            lv = _fmt(lmn_val, fmt)
            if color_fn and oun_val is not None and not (isinstance(oun_val, float) and math.isnan(oun_val)):
                c = color_fn(oun_val)
                ov = f"[{c}]{ov}[/{c}]"
            lines.append(f"{label:<16} {ov:>{w}}  {lv:>{w}}")

        def cin_c(v):
            return "bright_red" if v >= 200 else "red" if v >= 100 else "yellow" if v >= 50 else "green"
        def cape_c(v):
            return "bright_red" if v >= 3000 else "red" if v >= 2000 else "yellow" if v >= 1000 else "white"
        def srh_c(v):
            return "bright_red" if v >= 400 else "red" if v >= 250 else "yellow" if v >= 150 else "white"
        def ehi_c(v):
            return "bright_red" if v >= 4.0 else "red" if v >= 2.5 else "yellow" if v >= 1.5 else "white"

        row("MLCAPE (J/kg)",  getattr(indices, "MLCAPE", None), getattr(lmn_indices, "MLCAPE", None), "{:.0f}", cape_c)
        row("MLCIN  (J/kg)",  getattr(indices, "MLCIN", None),  getattr(lmn_indices, "MLCIN", None),  "{:.0f}", cin_c)
        row("Cap    (°C)",    getattr(indices, "cap_strength", None), getattr(lmn_indices, "cap_strength", None), "{:.1f}")
        row("LCL    (m)",     getattr(indices, "LCL_height", None), getattr(lmn_indices, "LCL_height", None), "{:.0f}")
        row("Lapse  (°C/km)", getattr(indices, "lapse_rate_700_500", None), getattr(lmn_indices, "lapse_rate_700_500", None), "{:.1f}")
        lines.append("")
        row("SRH 0-1 (m²/s²)", getattr(kinematics, "SRH_0_1km", None), getattr(lmn_kinematics, "SRH_0_1km", None), "{:.0f}", srh_c)
        row("SRH 0-3 (m²/s²)", getattr(kinematics, "SRH_0_3km", None), getattr(lmn_kinematics, "SRH_0_3km", None), "{:.0f}", srh_c)
        row("Shear 0-6 (kt)",  getattr(kinematics, "BWD_0_6km", None),  getattr(lmn_kinematics, "BWD_0_6km", None),  "{:.0f}")
        row("EHI",             getattr(kinematics, "EHI", None),         getattr(lmn_kinematics, "EHI", None),         "{:.2f}", ehi_c)
        row("STP",             getattr(kinematics, "STP", None),         getattr(lmn_kinematics, "STP", None),         "{:.2f}")

        if ces:
            lines.append("\n[bold]── CES Projection ──[/bold]")
            cb = ces.get("cap_behavior")
            cb_str = cb.value if cb else "—"
            cb_color = {
                "CLEAN_EROSION": "bright_green", "EARLY_EROSION": "green",
                "LATE_EROSION": "yellow", "NO_EROSION": "red", "BOUNDARY_FORCED": "cyan",
            }.get(cb_str, "white")
            lines.append(f"[{cb_color}]{cb_str}[/{cb_color}]")
            for key, label in [
                ("convective_temp_gap_12Z", "12Z Tc gap"),
                ("convective_temp_gap_15Z", "15Z Tc gap"),
                ("convective_temp_gap_18Z", "18Z Tc gap"),
            ]:
                val = ces.get(key)
                if val is not None:
                    gc = "green" if val < 0 else ("yellow" if val < 10 else "red")
                    lines.append(f"{label}: [{gc}]{val:+.1f}°F[/{gc}]")

        self.update("\n".join(lines))


class DrylinePanel(Static):
    DEFAULT_CSS = """
    DrylinePanel {
        border: solid $secondary;
        height: auto;
        min-height: 8;
        padding: 0 1;
    }
    """

    def render_dryline(self, dryline, surge: Optional[float], mesonet_label: str) -> None:
        lines = [f"[bold cyan]DRYLINE[/bold cyan]  Mesn {mesonet_label}\n"]
        if dryline is None:
            lines.append("[dim]Not detected in Mesonet network[/dim]")
        else:
            if dryline.position_lon:
                lons = dryline.position_lon
                lats = dryline.position_lat
                center_lon = sum(lons) / len(lons)
                lines.append(f"Center:     [cyan]{abs(center_lon):.1f}°W[/cyan]")
                if len(lons) >= 2:
                    lines.append(f"Extent:     {lats[0]:.1f}°N → {lats[-1]:.1f}°N")
            conf_c = "green" if dryline.confidence >= 0.6 else "yellow" if dryline.confidence >= 0.35 else "red"
            lines.append(f"Confidence: [{conf_c}]{dryline.confidence:.2f}[/{conf_c}]")
            if surge is not None:
                direction = "eastward" if surge > 0 else "retrograding"
                sc = "red" if abs(surge) > 15 else "yellow" if abs(surge) > 5 else "green"
                lines.append(f"Surge:      [{sc}]{surge:+.0f} mph ({direction})[/{sc}]")
            if dryline.counties_intersected:
                names = ", ".join(c.name for c in dryline.counties_intersected[:5])
                if len(dryline.counties_intersected) > 5:
                    names += f" +{len(dryline.counties_intersected) - 5}"
                lines.append(f"Near:       {names}")
        self.update("\n".join(lines))


class TendencyPanel(Static):
    DEFAULT_CSS = """
    TendencyPanel {
        border: solid $secondary;
        height: 1fr;
        padding: 0 1;
        overflow-y: auto;
    }
    """

    def render_tendency(
        self, tend_counties: list, baseline_label: str, current_label: str
    ) -> None:
        if not tend_counties:
            self.update(
                f"[bold cyan]TENDENCY[/bold cyan]  [dim]{baseline_label} → {current_label}\n"
                "No elevated counties to track.[/dim]"
            )
            return

        lines = [
            f"[bold cyan]TENDENCY[/bold cyan]  {baseline_label} → {current_label}\n",
            f"{'County':<13}{'Tier':<19}{'ΔCIN':>6}{'ΔCAPE':>7}{'ΔSRH1':>7}{'ΔEHI':>6}  Trend",
            "─" * 62,
        ]

        for tier, pt_now, pt_base in tend_counties[:10]:
            color  = _TIER_COLOR.get(tier, "white")
            d_cin  = pt_now.MLCIN  - pt_base.MLCIN
            d_cape = pt_now.MLCAPE - pt_base.MLCAPE
            d_srh1 = pt_now.SRH_0_1km - pt_base.SRH_0_1km
            d_ehi  = (pt_now.EHI or 0.0) - (pt_base.EHI or 0.0)

            cin_c  = "green" if d_cin  <= -10 else ("red"    if d_cin  >= 10  else "yellow")
            srh_c  = "green" if d_srh1 >=  20 else ("red"    if d_srh1 <= -20 else "yellow")
            cape_c = "green" if d_cape >= 200  else ("red"    if d_cape <= -200 else "yellow")

            score = (
                (1 if d_cin <= -10 else 0) + (1 if d_cin <= -30 else 0) +
                (1 if d_srh1 >= 20 else 0) + (1 if d_cape >= 200 else 0) +
                (-1 if d_cin >= 10 else 0) + (-1 if d_srh1 <= -20 else 0)
            )
            arrow = (
                "[bright_green]▲▲[/bright_green]" if score >= 3 else
                "[green]▲[/green]"                if score == 2 else
                "[bright_yellow]→[/bright_yellow]" if score >= 0 else
                "[red]▼[/red]"
            )

            lines.append(
                f"{pt_now.county.name:<13}"
                f"[{color}]{tier:<19}[/{color}]"
                f"[{cin_c}]{d_cin:>+5.0f}[/{cin_c}]"
                f"[{cape_c}]{d_cape:>+6.0f}[/{cape_c}]"
                f"[{srh_c}]{d_srh1:>+6.0f}[/{srh_c}]"
                f"{d_ehi:>+5.2f}  {arrow}"
            )

        self.update("\n".join(lines))


class SPCProductsPanel(Static):
    DEFAULT_CSS = """
    SPCProductsPanel {
        border: solid $warning;
        height: auto;
        min-height: 5;
        padding: 0 1;
        overflow-y: auto;
    }
    """

    # Outlook category color mapping
    _CAT_COLOR = {
        "HIGH":  "bright_red",
        "MDT":   "red",
        "ENH":   "dark_orange",
        "SLGT":  "yellow",
        "MRGL":  "green",
        "TSTM":  "dim",
        "NONE":  "dim",
    }

    def render_spc(
        self,
        mds: list[MesoscaleDiscussion],
        outlook: Optional[SPCOutlook],
        fetch_label: str,
    ) -> None:
        lines = [f"[bold yellow]SPC PRODUCTS[/bold yellow]  {fetch_label}"]

        # ── Day 1 outlook line ────────────────────────────────────────────────
        if outlook is not None:
            cat   = outlook.category
            color = self._CAT_COLOR.get(cat, "white")
            prob  = outlook.max_tornado_prob
            prob_str = f"  torn {prob:.0%}" if prob is not None else ""
            lines.append(
                f"D1 Outlook: [{color}]{cat}[/{color}]{prob_str}"
            )
        else:
            lines.append("[dim]D1 Outlook: unavailable[/dim]")

        lines.append("")

        # ── Active Mesoscale Discussions ──────────────────────────────────────
        if not mds:
            lines.append("[dim]No active SPC Mesoscale Discussions for Oklahoma[/dim]")
        else:
            for md in mds:
                ok_tag = " [cyan](OK)[/cyan]" if md.mentions_oklahoma else ""
                lines.append(
                    f"[bold]MD #{md.number:04d}[/bold]{ok_tag}"
                )
                if md.areas_affected:
                    lines.append(f"  [dim]Areas:[/dim] {md.areas_affected}")
                if md.concerning:
                    lines.append(f"  [dim]Re:[/dim]    {md.concerning}")
                for body_line in md.body_lines[:3]:
                    lines.append(f"  {body_line}")
                lines.append(f"  [dim]{md.url}[/dim]")
                lines.append("")

        self.update("\n".join(lines))


class AlertLog(RichLog):
    DEFAULT_CSS = """
    AlertLog {
        border: solid $accent;
        height: 10;
        padding: 0 1;
    }
    """

    def add_alert(self, message: str) -> None:
        ts = datetime.now(tz=timezone.utc).strftime("%H:%MZ")
        self.write(f"[dim]{ts}[/dim]  {message}")


# ── Main App ──────────────────────────────────────────────────────────────────

class KronosDashboard(App):
    """KRONOS-WX real-time severe weather dashboard."""

    CSS = """
    Screen {
        layout: grid;
        grid-size: 2;
        grid-rows: 1fr auto 10;
    }

    #left-col {
        layout: vertical;
        height: 1fr;
    }

    #right-col {
        layout: vertical;
        height: 1fr;
    }

    #spc-row {
        column-span: 2;
        height: auto;
        min-height: 5;
        max-height: 16;
    }

    #alert-row {
        column-span: 2;
        height: 10;
    }
    """

    BINDINGS = [
        Binding("q", "quit", "Quit"),
        Binding("r", "force_refresh", "Refresh now"),
    ]

    TITLE = "KRONOS-WX"

    # ── Reactive state ────────────────────────────────────────────────────────
    # Workers post results here; watchers push them into the widgets.

    _hrrr_label:     reactive[str] = reactive("—")
    _mesonet_label:  reactive[str] = reactive("—")
    _sounding_label: reactive[str] = reactive("—")

    def __init__(self, station: str = "OUN", **kwargs):
        super().__init__(**kwargs)
        self._station = station

        # Shared data — written by workers, read by watchers/widgets
        self._risk_zones:      list = []
        self._base_risk_zones: list = []
        self._tend_counties:   list = []
        self._hrrr_snap        = None
        self._hrrr_base        = None
        self._hrrr_valid:      Optional[datetime] = None
        self._hrrr_base_valid: Optional[datetime] = None
        self._dryline          = None
        self._prev_dryline     = None
        self._dryline_surge:   Optional[float] = None
        self._indices          = None
        self._kinematics       = None
        self._lmn_indices      = None
        self._lmn_kinematics   = None
        self._ces:             Optional[dict] = None
        self._snd_hour:        Optional[int] = None
        self._surface_temp_c:  Optional[float] = None

        # Tier change tracking
        self._prev_county_tiers: dict = {}
        self._prev_dryline_lon:  Optional[float] = None

        # SPC products
        self._spc_mds:     list = []
        self._spc_outlook: Optional[SPCOutlook] = None
        self._spc_label:   str = "—"

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        with Horizontal():
            with Vertical(id="left-col"):
                yield RiskZonePanel(id="risk-panel")
                yield DrylinePanel(id="dryline-panel")
            with Vertical(id="right-col"):
                yield EnvironmentPanel(id="env-panel")
                yield TendencyPanel(id="tend-panel")
        yield SPCProductsPanel(id="spc-panel")
        yield AlertLog(id="alert-log", highlight=True, markup=True)
        yield Footer()

    def on_mount(self) -> None:
        # Kick off all workers immediately, then set intervals
        self._do_sounding()
        self._do_mesonet()
        self._do_hrrr()
        self._do_spc()

        self.set_interval(60 * 60, self._do_sounding)   # 60 min
        self.set_interval(5  * 60, self._do_mesonet)    # 5 min
        self.set_interval(15 * 60, self._do_hrrr)       # 15 min
        self.set_interval(10 * 60, self._do_spc)        # 10 min

    def action_force_refresh(self) -> None:
        self._do_mesonet()
        self._do_hrrr()
        self._do_spc()
        self.query_one(AlertLog).add_alert("[dim]Manual refresh triggered[/dim]")

    # ── Workers ───────────────────────────────────────────────────────────────

    @work(thread=True, exclusive=True, group="sounding")
    def _do_sounding(self) -> None:
        now_utc = datetime.now(tz=timezone.utc)
        today   = now_utc.date()
        try:
            stn = OklahomaSoundingStation[self._station.upper()]
        except KeyError:
            return

        standard_hours = (21, 18, 15, 12, 9, 6, 3, 0)
        hours_to_try   = [h for h in standard_hours if h <= now_utc.hour] or [0]

        profile = fetched_hour = None
        with SoundingClient() as sc:
            for h in hours_to_try:
                p = sc.get_sounding(stn, today, h)
                if p is not None:
                    profile = p
                    fetched_hour = h
                    break
        if profile is None:
            return

        try:
            indices    = compute_thermodynamic_indices(profile)
            kinematics = compute_kinematic_profile(profile, indices)
        except Exception:
            return

        lmn_indices = lmn_kinematics = None
        if stn == OklahomaSoundingStation.OUN:
            with SoundingClient() as sc2:
                lp = sc2.get_sounding(
                    OklahomaSoundingStation.LMN, today, fetched_hour
                )
            if lp is not None:
                try:
                    lmn_indices    = compute_thermodynamic_indices(lp)
                    lmn_kinematics = compute_kinematic_profile(lp, lmn_indices)
                except Exception:
                    pass

        ces = None
        if fetched_hour == 12 and self._surface_temp_c is not None:
            try:
                ces = compute_ces_from_sounding(indices, self._surface_temp_c, today)
            except Exception:
                pass

        label = f"{self._station.upper()} {fetched_hour:02d}Z {today}"
        self.call_from_thread(
            self._update_sounding, indices, kinematics,
            lmn_indices, lmn_kinematics, ces, label, fetched_hour
        )

    def _update_sounding(self, indices, kinematics, lmn_indices, lmn_kinematics,
                          ces, label: str, fetched_hour: int) -> None:
        self._indices       = indices
        self._kinematics    = kinematics
        self._lmn_indices   = lmn_indices
        self._lmn_kinematics = lmn_kinematics
        self._ces           = ces
        self._snd_hour      = fetched_hour
        self._sounding_label = label
        self.query_one(EnvironmentPanel).render_environment(
            indices, kinematics, lmn_indices, lmn_kinematics, ces, label
        )

    @work(thread=True, exclusive=True, group="mesonet")
    def _do_mesonet(self) -> None:
        now_utc = datetime.now(tz=timezone.utc)
        station_series: dict[str, _MTS] = {}
        snap_times: list[datetime] = []

        with MesonetClient() as mc:
            for hrs_back in (0, 1):
                target = now_utc - timedelta(hours=hrs_back)
                for lb in range(4):
                    snap_time = target - timedelta(minutes=5 * lb)
                    try:
                        obs = mc.get_snapshot_observations(snap_time)
                    except Exception:
                        continue
                    for o in obs:
                        stid = o.station_id
                        if stid not in station_series:
                            station_series[stid] = _MTS(
                                station_id=stid, county=o.county,
                                start_time=snap_time, end_time=snap_time,
                                observations=[],
                            )
                        station_series[stid].observations.append(o)
                        station_series[stid].end_time = max(
                            station_series[stid].end_time, snap_time
                        )
                    snap_times.append(snap_time)
                    break

        current_dryline = prev_dryline = None
        if len(snap_times) >= 1:
            current_dryline = detect_dryline(station_series, snap_times[0])
        if len(snap_times) >= 2:
            prev_dryline = detect_dryline(station_series, snap_times[-1])

        surge: Optional[float] = None
        if prev_dryline and current_dryline:
            surge = compute_dryline_surge_rate(prev_dryline, current_dryline)

        surface_temp_c = None
        if snap_times:
            for ts in station_series.values():
                if ts.county == OklahomaCounty.CLEVELAND:
                    for o in ts.observations:
                        if abs((o.valid_time - snap_times[0]).total_seconds()) <= 420:
                            surface_temp_c = (o.temperature - 32) * 5 / 9
                            break
                    break

        label = snap_times[0].strftime("%H:%MZ") if snap_times else "—"
        self.call_from_thread(
            self._update_mesonet, current_dryline, prev_dryline, surge,
            surface_temp_c, label
        )

    def _update_mesonet(self, current_dryline, prev_dryline, surge,
                         surface_temp_c, label: str) -> None:
        self._surface_temp_c = surface_temp_c
        self._mesonet_label  = label
        alert_log = self.query_one(AlertLog)

        # Dryline change alerts
        curr_lon = None
        if current_dryline and current_dryline.position_lon:
            curr_lon = sum(current_dryline.position_lon) / len(current_dryline.position_lon)

        if curr_lon is not None and self._prev_dryline_lon is None:
            alert_log.add_alert(
                f"[cyan]Dryline appeared[/cyan]  "
                f"{abs(curr_lon):.1f}°W  conf {current_dryline.confidence:.2f}"
            )
        elif curr_lon is None and self._prev_dryline_lon is not None:
            alert_log.add_alert("[yellow]Dryline no longer detected[/yellow]")
        elif curr_lon is not None and surge is not None and abs(surge) >= 10:
            direction = "eastward" if surge > 0 else "retrograding"
            sc = "red" if abs(surge) > 15 else "yellow"
            alert_log.add_alert(
                f"[{sc}]Dryline surge {direction}: {surge:+.0f} mph  "
                f"now {abs(curr_lon):.1f}°W[/{sc}]"
            )

        self._prev_dryline_lon = curr_lon
        self._dryline          = current_dryline
        self._prev_dryline     = prev_dryline
        self._dryline_surge    = surge

        self.query_one(DrylinePanel).render_dryline(current_dryline, surge, label)

    @work(thread=True, exclusive=True, group="hrrr")
    def _do_hrrr(self) -> None:
        now_utc = datetime.now(tz=timezone.utc)
        today   = now_utc.date()
        snd_hour = self._snd_hour or 12

        base_vt = datetime(today.year, today.month, today.day,
                           snd_hour, 0, tzinfo=timezone.utc)
        hrrr_base = curr_snap = curr_valid = None
        base_valid = None

        with HRRRClient() as hc:
            hrrr_base = hc.get_county_snapshot(base_vt)
            base_valid = base_vt if hrrr_base else None

            for hb in range(4):
                try_h    = (now_utc.hour - hb) % 24
                try_date = today if now_utc.hour >= hb else today - timedelta(days=1)
                vt = datetime(try_date.year, try_date.month, try_date.day,
                              try_h, 0, tzinfo=timezone.utc)
                if vt == base_vt:
                    curr_snap  = hrrr_base
                    curr_valid = vt
                    break
                snap = hc.get_county_snapshot(vt)
                if snap is not None:
                    curr_snap  = snap
                    curr_valid = vt
                    break

        if curr_snap is None:
            return

        dryline = self._dryline  # thread-safe read of simple reference
        risk_zones = compute_risk_zones_from_hrrr(
            curr_snap, dryline=dryline, min_tier="MARGINAL"
        )
        base_zones = compute_risk_zones_from_hrrr(
            hrrr_base, dryline=dryline, min_tier="MARGINAL"
        ) if hrrr_base and hrrr_base is not curr_snap else []

        # Tendency counties
        elevated_now  = any(z.tier_rank >= 3 for z in risk_zones)
        elevated_base = any(z.tier_rank >= 3 for z in base_zones)
        ref_zones = risk_zones if elevated_now else base_zones
        tend_counties = []
        seen: set = set()
        for zone in ref_zones:
            if zone.tier_rank < 3:
                continue
            for county in zone.counties:
                if county in seen:
                    continue
                seen.add(county)
                pt_now  = curr_snap.get(county)
                pt_base = hrrr_base.get(county) if hrrr_base else None
                if pt_now and pt_base and curr_valid != base_valid:
                    tend_counties.append((zone.tier, pt_now, pt_base))

        hrrr_label    = curr_valid.strftime("%H:%MZ") if curr_valid else "—"
        base_label    = base_valid.strftime("%H:%MZ") if base_valid else "—"

        self.call_from_thread(
            self._update_hrrr, curr_snap, hrrr_base, curr_valid, base_valid,
            risk_zones, base_zones, tend_counties, hrrr_label, base_label
        )

    def _update_hrrr(
        self, curr_snap, hrrr_base, curr_valid, base_valid,
        risk_zones, base_zones, tend_counties,
        hrrr_label: str, base_label: str
    ) -> None:
        alert_log = self.query_one(AlertLog)

        # Build current tier map, detect changes
        curr_tiers: dict = {}
        for zone in risk_zones:
            for county in zone.counties:
                curr_tiers[county] = zone.tier

        upgrades = downgrades = []
        if self._prev_county_tiers:
            for county, new_tier in curr_tiers.items():
                old_tier = self._prev_county_tiers.get(county, "LOW")
                if old_tier == new_tier:
                    continue
                old_rank = _TIER_RANK.get(old_tier, 0)
                new_rank = _TIER_RANK.get(new_tier, 0)
                if max(old_rank, new_rank) < 2:
                    continue
                if new_rank > old_rank:
                    color = _TIER_COLOR.get(new_tier, "white")
                    alert_log.add_alert(
                        f"[{color}]▲ {county.name}: {old_tier} → {new_tier}[/{color}]"
                    )
                elif old_rank > new_rank:
                    alert_log.add_alert(
                        f"[yellow]▼ {county.name}: {old_tier} → {new_tier}[/yellow]"
                    )
            # Counties that fell out entirely
            for county, old_tier in self._prev_county_tiers.items():
                if county not in curr_tiers and _TIER_RANK.get(old_tier, 0) >= 2:
                    alert_log.add_alert(
                        f"[dim]▼ {county.name}: {old_tier} → (below MARGINAL)[/dim]"
                    )

        if hrrr_label != self._hrrr_label:
            alert_log.add_alert(f"[dim]New HRRR run: {hrrr_label}[/dim]")

        self._hrrr_snap        = curr_snap
        self._hrrr_base        = hrrr_base
        self._hrrr_valid       = curr_valid
        self._hrrr_base_valid  = base_valid
        self._risk_zones       = risk_zones
        self._base_risk_zones  = base_zones
        self._tend_counties    = tend_counties
        self._prev_county_tiers = curr_tiers
        self._hrrr_label       = hrrr_label

        self.query_one(RiskZonePanel).render_zones(risk_zones, hrrr_label)
        self.query_one(TendencyPanel).render_tendency(
            tend_counties, base_label, hrrr_label
        )

    # ── SPC products worker ───────────────────────────────────────────────────

    @work(thread=True, exclusive=True, group="spc")
    def _do_spc(self) -> None:
        mds     = fetch_active_mds()
        outlook = fetch_spc_outlook()
        label   = datetime.now(tz=timezone.utc).strftime("%H:%MZ")
        self.call_from_thread(self._update_spc, mds, outlook, label)

    def _update_spc(
        self,
        mds: list,
        outlook: Optional[SPCOutlook],
        label: str,
    ) -> None:
        alert_log = self.query_one(AlertLog)

        # Alert on new Oklahoma-relevant MD
        new_ok_nums = {md.number for md in mds if md.mentions_oklahoma}
        old_ok_nums = {md.number for md in self._spc_mds if md.mentions_oklahoma}
        for md in mds:
            if md.mentions_oklahoma and md.number not in old_ok_nums:
                alert_log.add_alert(
                    f"[bold yellow]SPC MD #{md.number:04d}[/bold yellow]  "
                    f"{md.concerning or md.areas_affected or 'active'}"
                )

        # Alert on outlook upgrade
        if outlook and self._spc_outlook:
            _rank = {"NONE": 0, "TSTM": 1, "MRGL": 2, "SLGT": 3, "ENH": 4, "MDT": 5, "HIGH": 6}
            old_r = _rank.get(self._spc_outlook.category, 0)
            new_r = _rank.get(outlook.category, 0)
            if new_r > old_r and new_r >= 3:
                color = SPCProductsPanel._CAT_COLOR.get(outlook.category, "yellow")
                alert_log.add_alert(
                    f"[{color}]SPC D1 upgraded to {outlook.category}[/{color}]"
                    + (f"  torn {outlook.max_tornado_prob:.0%}" if outlook.max_tornado_prob else "")
                )

        self._spc_mds     = mds
        self._spc_outlook = outlook
        self._spc_label   = label
        self.query_one(SPCProductsPanel).render_spc(mds, outlook, label)
