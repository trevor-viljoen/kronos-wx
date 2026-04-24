"""
KRONOS-WX CLI — Oklahoma severe weather case library orchestration.

Commands:
    build-case-skeleton    Build initial case library from SPC tornado data
    enrich-case            Add sounding + Mesonet data to a single case
    enrich-all             Bulk enrichment over a year range
    analyze-cap-behavior   Compute full cap erosion trajectory for a case
    build-bust-database    Identify bust and alarm-bell cases
"""

import logging
import sys
from datetime import date, datetime, time, timezone, timedelta
from pathlib import Path

import click
from rich.console import Console
from rich.table import Table
from rich.progress import track
from rich import print as rprint

from ok_weather_model.config import (
    CASE_LIBRARY_START_YEAR,
    CASE_LIBRARY_END_YEAR,
    LOG_LEVEL,
    LOG_DIR,
    VALIDATION_CASE_ID,
)
from ok_weather_model.models import (
    EventClass,
    CapBehavior,
    OklahomaSoundingStation,
    HistoricalCase,
)

console = Console()

# ── Logging setup ─────────────────────────────────────────────────────────────

def _setup_logging() -> None:
    log_file = LOG_DIR / "kronos_wx.log"
    logging.basicConfig(
        level=getattr(logging, LOG_LEVEL, logging.INFO),
        format="%(asctime)s  %(name)s  %(levelname)s  %(message)s",
        handlers=[
            logging.StreamHandler(sys.stderr),
            logging.FileHandler(log_file),
        ],
    )

_setup_logging()
logger = logging.getLogger(__name__)


# ── CLI group ─────────────────────────────────────────────────────────────────

@click.group()
@click.version_option("0.1.0", prog_name="kronos-wx")
def cli():
    """KRONOS-WX: Oklahoma severe weather outbreak/bust analysis system."""
    pass


# ── build-case-skeleton ───────────────────────────────────────────────────────

@cli.command("build-case-skeleton")
@click.option("--start-year", default=CASE_LIBRARY_START_YEAR, show_default=True,
              help="First year to include")
@click.option("--end-year", default=CASE_LIBRARY_END_YEAR, show_default=True,
              help="Last year to include")
@click.option("--overwrite", is_flag=True, default=False,
              help="Overwrite existing cases (default: skip existing)")
def build_case_skeleton(start_year: int, end_year: int, overwrite: bool):
    """
    Pull SPC tornado database, filter to Oklahoma, group by date,
    classify EventClass, and save skeleton HistoricalCase objects.
    """
    from ok_weather_model.ingestion import SPCClient
    from ok_weather_model.storage import Database

    console.rule("[bold red]Building case skeleton from SPC data[/bold red]")

    db = Database()
    start_date = date(start_year, 1, 1)
    end_date = date(end_year, 12, 31)

    with SPCClient() as spc:
        with console.status(f"Downloading SPC tornado data {start_year}–{end_year}..."):
            skeletons = spc.build_case_skeletons(start_date, end_date)

    console.print(f"[green]Built {len(skeletons)} case skeletons from SPC data[/green]")

    saved = skipped = 0
    for case in track(skeletons, description="Saving to database..."):
        if not overwrite and db.case_exists(case.case_id):
            skipped += 1
            continue
        db.save_case(case)
        saved += 1

    # ── Summary table ─────────────────────────────────────────────────────────
    stats = db.get_case_statistics()

    table = Table(title=f"Case Library Summary ({start_year}–{end_year})", show_lines=True)
    table.add_column("Event Class", style="cyan")
    table.add_column("Cases", justify="right", style="green")
    table.add_column("Avg Tornadoes", justify="right")
    table.add_column("Avg Completeness", justify="right")

    for event_class, info in stats.items():
        table.add_row(
            event_class,
            str(info["count"]),
            f"{info['avg_tornadoes']:.1f}",
            f"{info['avg_completeness']:.0%}",
        )

    console.print(table)
    console.print(f"\nSaved: [green]{saved}[/green]  Skipped (already exist): [yellow]{skipped}[/yellow]")


# ── enrich-case ──────────────────────────────────────────────────────────────

@cli.command("enrich-case")
@click.argument("case_ref")  # case_id (YYYYMMDD_OK) or date (YYYY-MM-DD / YYYYMMDD)
@click.option("--force", is_flag=True, default=False,
              help="Re-enrich even if sounding data already loaded")
def enrich_case(case_ref: str, force: bool):
    """
    Enrich a single case with sounding and Mesonet data.

    CASE_REF: case_id (e.g. 19990503_OK) or date (e.g. 1999-05-03)
    """
    from ok_weather_model.storage import Database
    from ok_weather_model.ingestion import SoundingClient, MesonetClient
    from ok_weather_model.processing import (
        compute_thermodynamic_indices,
        compute_kinematic_profile,
        compute_convective_temp_gap,
    )

    db = Database()
    case_id, case_date = _resolve_case_ref(case_ref)

    case = db.load_case(case_id)
    if case is None:
        # Create a minimal skeleton
        console.print(f"[yellow]Case {case_id} not in database — creating skeleton[/yellow]")
        case = HistoricalCase(
            case_id=case_id,
            date=case_date,
            event_class=EventClass.NULL_BUST,
        )

    if case.sounding_data_available and not force:
        console.print(f"[yellow]Case {case_id} already has sounding data. Use --force to re-enrich.[/yellow]")
        return

    console.rule(f"[bold]Enriching {case_id}[/bold]")

    # ── 12Z OUN sounding ──────────────────────────────────────────────────────
    with console.status("Fetching 12Z OUN sounding..."):
        with SoundingClient() as sc:
            profile = sc.get_sounding(OklahomaSoundingStation.OUN, case_date, 12)

    if profile is None:
        console.print(f"[red]No 12Z OUN sounding found for {case_date}[/red]")
    else:
        db.save_sounding(profile)
        console.print(f"[green]Sounding: {len(profile.levels)} levels[/green]")

        with console.status("Computing thermodynamic indices..."):
            try:
                indices = compute_thermodynamic_indices(profile)
                kinematics = compute_kinematic_profile(profile, indices)
                case.sounding_12Z = indices
                case.kinematics_12Z = kinematics
                case.sounding_data_available = True

                console.print(
                    f"  MLCAPE={indices.MLCAPE:.0f} J/kg  "
                    f"MLCIN={indices.MLCIN:.0f} J/kg  "
                    f"cap={indices.cap_strength:.1f}°C  "
                    f"SRH0-3={kinematics.SRH_0_3km:.0f} m²/s²"
                )
            except Exception as exc:
                console.print(f"[red]MetPy computation failed: {exc}[/red]")
                logger.exception("Thermodynamic index computation failed for %s", case_id)

    # ── Mesonet full-day pull ─────────────────────────────────────────────────
    with console.status("Pulling Mesonet data..."):
        try:
            with MesonetClient() as mc:
                station_data = mc.get_historical_case_data(case_date)

            for station_id, ts in station_data.items():
                db.save_mesonet_timeseries(ts)

            case.mesonet_data_available = len(station_data) > 0
            console.print(f"[green]Mesonet: {len(station_data)} stations[/green]")

            # ── Convective temp gap at 12Z, 15Z, 18Z ──────────────────────────
            if case.sounding_12Z is not None:
                tc = case.sounding_12Z.convective_temperature

                for label, hour in [("12Z", 12), ("15Z", 15), ("18Z", 18)]:
                    valid_time = datetime(
                        case_date.year, case_date.month, case_date.day, hour, 0,
                        tzinfo=timezone.utc
                    )
                    # Average OUN-county surface temp at that time
                    from ok_weather_model.models import OklahomaCounty
                    try:
                        oun_county = OklahomaCounty.CLEVELAND  # OUN is in Cleveland Co.
                        county_ts_list = [
                            ts for sid, ts in station_data.items()
                            if ts.county == oun_county
                        ]
                        surface_state = mc.compute_county_surface_state(
                            oun_county, valid_time, county_ts_list
                        )
                        if surface_state:
                            gap = compute_convective_temp_gap(surface_state.mean_temperature, tc)
                            setattr(case, f"convective_temp_gap_{label}", gap)
                            console.print(f"  {label} Tc-gap: {gap:+.1f}°F")
                    except Exception as exc:
                        logger.warning("Could not compute %s Tc-gap: %s", label, exc)

            # ── Dryline detection ──────────────────────────────────────────────
            with console.status("Detecting dryline..."):
                try:
                    from ok_weather_model.processing import analyze_dryline_from_mesonet
                    dryline_result = analyze_dryline_from_mesonet(station_data, case_date)
                    case.boundaries = [
                        *case.boundaries,
                        *dryline_result["boundaries"],
                    ]
                    if dryline_result["dryline_lon_18Z"] is not None:
                        case.dryline_longitude_18Z = dryline_result["dryline_lon_18Z"]
                    if dryline_result["surge_rate_mph"] is not None:
                        case.dryline_surge_rate_mph = dryline_result["surge_rate_mph"]

                    n = len(dryline_result["boundaries"])
                    if n:
                        lon = dryline_result["dryline_lon_18Z"]
                        surge = dryline_result["surge_rate_mph"]
                        lon_str = f"{lon:.1f}°W" if lon is not None else "—"
                        surge_str = f"{surge:+.1f} mph" if surge is not None else "—"
                        console.print(
                            f"[green]Dryline: {n} snapshot(s) detected  "
                            f"18Z lon={lon_str}  surge={surge_str}[/green]"
                        )
                    else:
                        console.print("[dim]Dryline: not detected[/dim]")
                except Exception as exc:
                    console.print(f"[yellow]Dryline detection failed: {exc}[/yellow]")
                    logger.warning("Dryline detection failed for %s: %s", case_id, exc)

        except Exception as exc:
            console.print(f"[red]Mesonet pull failed: {exc}[/red]")
            logger.exception("Mesonet pull failed for %s", case_id)

    # ── Save enriched case ───────────────────────────────────────────────────
    case.recompute_completeness()
    db.save_case(case)
    console.print(
        f"\n[bold green]Case {case_id} saved. "
        f"Completeness: {case.data_completeness_score:.0%}[/bold green]"
    )


# ── enrich-all ────────────────────────────────────────────────────────────────

@cli.command("enrich-all")
@click.argument("start_year", type=int)
@click.argument("end_year", type=int)
@click.option("--force", is_flag=True, default=False,
              help="Re-enrich already-enriched cases (refetches all hours)")
@click.option("--upgrade", is_flag=True, default=False,
              help="Add missing 00Z/18Z/21Z to cases that already have 12Z, "
                   "without re-fetching hours already present")
def enrich_all(start_year: int, end_year: int, force: bool, upgrade: bool):
    """
    Enrich all cases in a year range. Supports resume (skips already-enriched).

    START_YEAR END_YEAR: e.g.  enrich-all 1994 2023

    Default: skips cases that already have sounding data.
    --upgrade: backfills 00Z/18Z/21Z for cases that have 12Z but are missing
               the surrounding hours (only fetches the missing ones).
    --force: re-enriches everything from scratch.
    """
    from ok_weather_model.storage import Database

    db = Database()
    start_date = date(start_year, 1, 1)
    end_date = date(end_year, 12, 31)

    cases = db.query_parameter_space(
        {"start_date": str(start_date), "end_date": str(end_date)}
    )

    if not cases:
        console.print(f"[yellow]No cases found for {start_year}–{end_year}. "
                      f"Run build-case-skeleton first.[/yellow]")
        return

    if force:
        to_enrich = cases
        mode_label = "force re-enrich"
    elif upgrade:
        to_enrich = [
            c for c in cases
            if c.sounding_data_available and (
                c.sounding_00Z is None or
                c.sounding_18Z is None or
                c.sounding_21Z is None
            )
        ]
        mode_label = "upgrade (backfill 00Z/18Z/21Z)"
    else:
        to_enrich = [c for c in cases if not c.sounding_data_available]
        mode_label = "enrich new"

    console.print(
        f"Found {len(cases)} total cases. "
        f"{mode_label}: {len(to_enrich)} "
        f"(skipping {len(cases) - len(to_enrich)})."
    )

    from ok_weather_model.ingestion import SoundingClient
    from ok_weather_model.processing import compute_thermodynamic_indices, compute_kinematic_profile

    errors = []
    enriched = 0

    # Named hours stored as dedicated case fields.
    # 12Z is the primary pre-convective sounding.
    # 00Z captures the overnight cap state.
    # 18Z and 21Z bracket the initiation window and LLJ onset.
    _HOUR_FIELDS = {
        0:  ("sounding_00Z", "kinematics_00Z"),
        12: ("sounding_12Z", "kinematics_12Z"),
        18: ("sounding_18Z", "kinematics_18Z"),
        21: ("sounding_21Z", "kinematics_21Z"),
    }

    # Routine twice-daily launches are reliably available at all four stations;
    # fall back to adjacent stations when OUN misses these.
    # Special-hour soundings (03Z, 06Z, 09Z, 15Z) are OUN-only event launches —
    # adjacent stations won't have them, so fallback would just waste requests.
    _FALLBACK_HOURS = {0, 12}

    with SoundingClient() as sc:
        for case in track(to_enrich, description="Enriching cases..."):
            try:
                if upgrade and not force:
                    # Only fetch hours not yet populated on this case
                    hours_needed = {
                        h for h, (idx_f, _) in _HOUR_FIELDS.items()
                        if getattr(case, idx_f) is None
                    }
                    # Always include special hours that might have new data
                    hours_needed |= {h for h in SoundingClient.STANDARD_HOURS
                                     if h not in _HOUR_FIELDS}
                else:
                    hours_needed = set(SoundingClient.STANDARD_HOURS)

                # Fetch the needed hours from OUN
                all_profiles: dict[int, object] = {}
                for hour in sorted(hours_needed):
                    profile = sc.get_sounding(
                        OklahomaSoundingStation.OUN, case.date, hour
                    )
                    if profile is not None:
                        all_profiles[hour] = profile

                # For routine 00Z/12Z only, try adjacent stations if OUN misses
                for hour in _FALLBACK_HOURS & hours_needed:
                    if hour not in all_profiles:
                        fallback = sc.get_sounding_with_fallback(
                            OklahomaSoundingStation.OUN, case.date, hour
                        )
                        if fallback is not None:
                            all_profiles[hour] = fallback

                if not all_profiles:
                    logger.debug("No sounding at any station or hour for %s", case.case_id)
                    continue

                # Persist every profile to the sounding store
                for profile in all_profiles.values():
                    db.save_sounding(profile)

                # Compute indices for named hours and populate case fields
                for hour, (idx_field, kin_field) in _HOUR_FIELDS.items():
                    if hour in all_profiles:
                        profile = all_profiles[hour]
                        indices    = compute_thermodynamic_indices(profile)
                        kinematics = compute_kinematic_profile(profile, indices)
                        setattr(case, idx_field, indices)
                        setattr(case, kin_field, kinematics)

                if case.sounding_12Z is not None:
                    case.sounding_data_available = True

                case.recompute_completeness()
                db.save_case(case)
                enriched += 1

            except Exception as exc:
                errors.append((case.case_id, str(exc)))
                logger.exception("Enrichment failed for %s", case.case_id)

    console.print(f"\n[bold green]Enriched: {enriched}[/bold green]  "
                  f"No sounding: {len(to_enrich) - enriched - len(errors)}  "
                  f"Errors: {len(errors)}")
    if errors:
        console.print(f"\n[red]Failures:[/red]")
        for case_id, err in errors[:10]:
            console.print(f"  {case_id}: {err}")
        if len(errors) > 10:
            console.print(f"  ... and {len(errors) - 10} more (see log)")


# ── analyze-cap-behavior ──────────────────────────────────────────────────────

@cli.command("analyze-cap-behavior")
@click.argument("case_ref")
@click.option("--forcing-window-hours", default=12, type=int, show_default=True,
              help="Length of the synoptic forcing window in hours from 12Z")
def analyze_cap_behavior(case_ref: str, forcing_window_hours: int):
    """
    Compute full CapErosionTrajectory and classify cap_behavior for a case.

    CASE_REF: case_id (e.g. 19990503_OK) or date (e.g. 1999-05-03)
    """
    from ok_weather_model.storage import Database
    from ok_weather_model.processing import compute_cap_erosion_budget, estimate_erosion_trajectory
    from ok_weather_model.models import OklahomaCounty, CapErosionBudget

    db = Database()
    case_id, case_date = _resolve_case_ref(case_ref)
    case = db.load_case(case_id)

    if case is None:
        console.print(f"[red]Case {case_id} not found. Run enrich-case first.[/red]")
        return

    if case.sounding_12Z is None:
        console.print(f"[red]Case {case_id} has no sounding data. Run enrich-case first.[/red]")
        return

    console.rule(f"[bold]Cap Analysis: {case_id}[/bold]")

    forcing_window_close = datetime(
        case_date.year, case_date.month, case_date.day, 12, 0, tzinfo=timezone.utc
    ) + timedelta(hours=forcing_window_hours)

    # Build budgets for OUN county (Cleveland Co.) using available data.
    # First try pre-stored Mesonet data; if absent, fetch live from the API
    # and cache it so subsequent runs are fast.
    budgets: list[CapErosionBudget] = []
    county = OklahomaCounty.CLEVELAND

    from ok_weather_model.ingestion import MesonetClient

    # Check whether any Mesonet data is already stored for this case
    _probe_time = datetime(case_date.year, case_date.month, case_date.day, 18, 0, tzinfo=timezone.utc)
    _probe = db.load_mesonet_timeseries(county, _probe_time - timedelta(minutes=15), _probe_time + timedelta(minutes=15))
    mesonet_in_db = _probe is not None and bool(_probe.observations)

    live_station_data: dict = {}
    if not mesonet_in_db:
        console.print("[yellow]No Mesonet data in database — fetching live from API...[/yellow]")
        try:
            with MesonetClient() as mc:
                live_station_data = mc.get_historical_case_data(case_date)
            if live_station_data:
                for ts in live_station_data.values():
                    db.save_mesonet_timeseries(ts)
                case.mesonet_data_available = True
                console.print(f"[green]Fetched and cached Mesonet data ({len(live_station_data)} stations)[/green]")
            else:
                console.print("[yellow]Mesonet API returned no data for this date[/yellow]")
        except Exception as exc:
            console.print(f"[yellow]Live Mesonet fetch failed: {exc}[/yellow]")
            logger.warning("Live Mesonet fetch failed for %s: %s", case_id, exc)

    # Load the full convective day (12Z–22Z) for the county so tendency
    # computation can compare across snapshots (e.g. 12Z→18Z heating rate).
    day_start = datetime(case_date.year, case_date.month, case_date.day, 11, 45, tzinfo=timezone.utc)
    day_end = datetime(case_date.year, case_date.month, case_date.day, 22, 15, tzinfo=timezone.utc)
    county_ts_day = db.load_mesonet_timeseries(county, day_start, day_end)

    with MesonetClient() as mc:
        for label, hour in [("12Z", 12), ("15Z", 15), ("18Z", 18), ("21Z", 21)]:
            valid_time = datetime(
                case_date.year, case_date.month, case_date.day, hour, 0,
                tzinfo=timezone.utc
            )

            # Prefer full-day DB load (provides prior snapshots for tendency);
            # fall back to the in-memory live data if DB had nothing.
            if county_ts_day is not None and county_ts_day.observations:
                surface_state = mc.compute_county_surface_state(county, valid_time, [county_ts_day])
            elif live_station_data:
                county_series = [ts for ts in live_station_data.values() if ts.county == county]
                surface_state = mc.compute_county_surface_state(county, valid_time, county_series)
            else:
                surface_state = None

            if surface_state is None:
                logger.debug("No surface state at %s for %s", label, case_id)
                continue

            try:
                budget = compute_cap_erosion_budget(case.sounding_12Z, surface_state)
                budgets.append(budget)
            except Exception as exc:
                logger.warning("Budget computation failed at %s: %s", label, exc)

    if not budgets:
        console.print(
            "[yellow]No Mesonet surface data available — running CES sounding-only analysis[/yellow]"
        )
        from ok_weather_model.processing.cap_calculator import compute_ces_from_sounding
        from ok_weather_model.models.enums import ErosionMechanism

        valid_time_12z = datetime(
            case_date.year, case_date.month, case_date.day, 12, 0, tzinfo=timezone.utc
        )
        sounding_raw = db.load_sounding(OklahomaSoundingStation.OUN, valid_time_12z)
        surface_temp_c = (
            sounding_raw.levels[0].temperature
            if sounding_raw and sounding_raw.levels
            else None
        )

        if case.sounding_12Z is None or surface_temp_c is None:
            console.print(
                "[red]No 12Z sounding data available — cannot compute CES. "
                "Run enrich-case first.[/red]"
            )
            return

        ces = compute_ces_from_sounding(case.sounding_12Z, surface_temp_c, case_date)

        ces_table = Table(title=f"CES Sounding-Only Analysis — {case_id}", show_lines=True)
        ces_table.add_column("Metric", style="cyan")
        ces_table.add_column("Value", style="green")
        ces_table.add_row("Event Class", case.event_class.value)
        ces_table.add_row("Cap Behavior", ces["cap_behavior"].value if ces["cap_behavior"] else "—")
        ces_table.add_row("12Z MLCAPE", f"{case.sounding_12Z.MLCAPE:.0f} J/kg")
        ces_table.add_row("12Z MLCIN", f"{case.sounding_12Z.MLCIN:.0f} J/kg")
        ces_table.add_row("12Z Cap Strength", f"{case.sounding_12Z.cap_strength:.1f}°C")
        ces_table.add_row(
            "Projected Erosion",
            ces["cap_erosion_time"].strftime("%H:%M UTC") if ces["cap_erosion_time"] else "No erosion"
        )
        if ces["convective_temp_gap_12Z"] is not None:
            ces_table.add_row("12Z Tc Gap", f"{ces['convective_temp_gap_12Z']:+.1f}°F")
        if ces["convective_temp_gap_15Z"] is not None:
            ces_table.add_row("15Z Tc Gap", f"{ces['convective_temp_gap_15Z']:+.1f}°F")
        if ces["convective_temp_gap_18Z"] is not None:
            ces_table.add_row("18Z Tc Gap", f"{ces['convective_temp_gap_18Z']:+.1f}°F")
        console.print(ces_table)

        case.cap_behavior = ces["cap_behavior"]
        case.cap_erosion_mechanism = ErosionMechanism.HEATING
        if ces["cap_erosion_time"] is not None:
            case.cap_erosion_time = ces["cap_erosion_time"]
        case.convective_temp_gap_12Z = ces["convective_temp_gap_12Z"]
        case.convective_temp_gap_15Z = ces["convective_temp_gap_15Z"]
        case.convective_temp_gap_18Z = ces["convective_temp_gap_18Z"]
        case.recompute_completeness()
        db.save_case(case)
        return

    trajectory = estimate_erosion_trajectory(budgets, forcing_window_close)

    # ── Classify cap_behavior ─────────────────────────────────────────────────
    cap_behavior = _classify_cap_behavior(trajectory, case_date)
    case.cap_behavior = cap_behavior
    case.cap_erosion_mechanism = trajectory.primary_mechanism
    if trajectory.erosion_time:
        case.cap_erosion_time = trajectory.erosion_time.time()
        case.cap_erosion_county = county

    db.save_case(case)

    # ── Color helpers ────────────────────────────────────────────────────────
    def _cape_color(v: float) -> str:
        if v >= 3000: return "bright_red"
        if v >= 2000: return "red"
        if v >= 1000: return "yellow"
        if v >= 500:  return "green"
        return "white"

    def _cin_color(v: float) -> str:
        """Higher CIN = more red (stronger cap)."""
        if v >= 200:  return "bright_red"
        if v >= 100:  return "red"
        if v >= 50:   return "yellow"
        if v >= 20:   return "bright_yellow"
        return "green"

    def _cap_strength_color(v: float) -> str:
        if v >= 4.0:  return "bright_red"
        if v >= 2.0:  return "red"
        if v >= 1.0:  return "yellow"
        return "green"

    def _tc_gap_color(v: float) -> str:
        """Positive Tc gap = surface below Tc (cap intact, red). Negative = broken."""
        if v >= 20:   return "bright_red"
        if v >= 10:   return "red"
        if v >= 3:    return "yellow"
        if v >= -3:   return "bright_yellow"
        return "green"

    def _bust_risk_color(v: float) -> str:
        if v >= 0.7:  return "bright_red"
        if v >= 0.5:  return "red"
        if v >= 0.3:  return "yellow"
        if v >= 0.15: return "bright_yellow"
        return "green"

    def _cap_behavior_color(cb) -> str:
        mapping = {
            "CLEAN_EROSION":   "bright_green",
            "EARLY_EROSION":   "green",
            "LATE_EROSION":    "yellow",
            "NO_EROSION":      "red",
            "BOUNDARY_FORCED": "cyan",
            "RECONSTITUTED":   "magenta",
        }
        return mapping.get(cb.value if hasattr(cb, "value") else cb, "white")

    def _event_class_color(ec) -> str:
        mapping = {
            "SIGNIFICANT_OUTBREAK": "bright_red",
            "ISOLATED_SIGNIFICANT": "red",
            "MARGINAL_EVENT":       "yellow",
            "NULL_BUST":            "bright_blue",
            "ACTIVE_NULL":          "blue",
        }
        return mapping.get(ec.value if hasattr(ec, "value") else ec, "white")

    def _net_tendency_color(v: float) -> str:
        if v <= -15:  return "bright_green"
        if v <= -5:   return "green"
        if v <= 0:    return "bright_yellow"
        return "red"

    def _cin_cell_color(v: float) -> str:
        if v >= 150: return "bright_red"
        if v >= 75:  return "red"
        if v >= 30:  return "yellow"
        if v >= 10:  return "bright_yellow"
        return "green"

    def _hrs_color(v) -> str:
        if v is None:  return "red"
        if v <= 2:     return "bright_green"
        if v <= 6:     return "green"
        if v <= 12:    return "yellow"
        return "red"

    # ── Print summary report ─────────────────────────────────────────────────
    table = Table(title=f"Cap Erosion Report — {case_id}", show_lines=True)
    table.add_column("Metric", style="cyan")
    table.add_column("Value")

    ec_color = _event_class_color(case.event_class)
    cb_color = _cap_behavior_color(cap_behavior)

    table.add_row("Event Class",
                  f"[{ec_color}]{case.event_class.value}[/{ec_color}]")
    table.add_row("Cap Behavior",
                  f"[{cb_color}]{cap_behavior.value}[/{cb_color}]")
    table.add_row("Primary Mechanism", trajectory.primary_mechanism.value)
    table.add_row("Erosion Achieved",
                  "[bright_green]YES[/bright_green]" if trajectory.erosion_achieved
                  else "[red]NO[/red]")
    table.add_row(
        "Erosion Time",
        trajectory.erosion_time.strftime("%H:%M UTC") if trajectory.erosion_time else "[dim]—[/dim]"
    )
    br = trajectory.bust_risk_score
    br_color = _bust_risk_color(br)
    table.add_row("Bust Risk Score", f"[{br_color}]{br:.2f}[/{br_color}]")

    cape = case.sounding_12Z.MLCAPE
    cin  = case.sounding_12Z.MLCIN
    cap  = case.sounding_12Z.cap_strength
    table.add_row("12Z MLCAPE",
                  f"[{_cape_color(cape)}]{cape:.0f} J/kg[/{_cape_color(cape)}]")
    table.add_row("12Z MLCIN",
                  f"[{_cin_color(cin)}]{cin:.0f} J/kg[/{_cin_color(cin)}]")
    table.add_row("12Z Cap Strength",
                  f"[{_cap_strength_color(cap)}]{cap:.1f}°C[/{_cap_strength_color(cap)}]")
    if case.convective_temp_gap_12Z is not None:
        g = case.convective_temp_gap_12Z
        table.add_row("12Z Tc Gap",
                      f"[{_tc_gap_color(g)}]{g:+.1f}°F[/{_tc_gap_color(g)}]")
    if case.convective_temp_gap_15Z is not None:
        g = case.convective_temp_gap_15Z
        table.add_row("15Z Tc Gap",
                      f"[{_tc_gap_color(g)}]{g:+.1f}°F[/{_tc_gap_color(g)}]")
    if case.convective_temp_gap_18Z is not None:
        g = case.convective_temp_gap_18Z
        table.add_row("18Z Tc Gap",
                      f"[{_tc_gap_color(g)}]{g:+.1f}°F[/{_tc_gap_color(g)}]")
    table.add_row("Forcing Window Close",
                  forcing_window_close.strftime("%H:%M UTC"))

    console.print(table)

    # Budget timeline
    budget_table = Table(title="Cap Erosion Budget Timeline", show_lines=True)
    budget_table.add_column("Time", style="cyan")
    budget_table.add_column("CIN (J/kg)", justify="right")
    budget_table.add_column("Heating", justify="right")
    budget_table.add_column("Dynamic", justify="right")
    budget_table.add_column("Net", justify="right")
    budget_table.add_column("Hrs to 0", justify="right")

    for b in trajectory.budget_history:
        hrs = b.hours_to_erosion
        cin_c  = _cin_cell_color(b.current_CIN)
        net_c  = _net_tendency_color(b.net_tendency)
        hrs_c  = _hrs_color(hrs)
        # Heating/dynamic: green when eroding (negative), dim when near zero
        def _forcing_str(v: float) -> str:
            if v <= -5:  return f"[green]{v:+.1f}[/green]"
            if v < 0:    return f"[bright_yellow]{v:+.1f}[/bright_yellow]"
            if v > 5:    return f"[red]{v:+.1f}[/red]"
            return f"[dim]{v:+.1f}[/dim]"

        budget_table.add_row(
            b.valid_time.strftime("%H:%M Z"),
            f"[{cin_c}]{b.current_CIN:.0f}[/{cin_c}]",
            _forcing_str(b.heating_forcing),
            _forcing_str(b.dynamic_forcing),
            f"[{net_c}]{b.net_tendency:+.1f}[/{net_c}]",
            f"[{hrs_c}]{hrs:.1f}[/{hrs_c}]" if hrs is not None
            else f"[{hrs_c}]∞[/{hrs_c}]",
        )

    console.print(budget_table)


# ── Shared analogue helpers ────────────────────────────────────────────────────

def _dangerous_capped_flag(
    indices,
    kinematics,
) -> tuple[bool, list[str]]:
    """
    Return (flag, reasons) when a strong cap sits over a violent kinematic
    environment — the BOUNDARY_FORCED miss pattern.

    Thresholds are intentionally modest: we want early warning, not perfect
    precision. A false positive on a busted severe day is far less costly than
    a missed outbreak.
    """
    if indices is None or indices.MLCIN < 80:
        return False, []

    reasons: list[str] = []
    if kinematics:
        if kinematics.SRH_0_1km > 150:
            reasons.append(f"SRH 0–1km {kinematics.SRH_0_1km:.0f} m²/s²")
        if kinematics.SRH_0_3km > 300:
            reasons.append(f"SRH 0–3km {kinematics.SRH_0_3km:.0f} m²/s²")
        if kinematics.EHI is not None and kinematics.EHI > 2.5:
            reasons.append(f"EHI {kinematics.EHI:.2f}")
        if kinematics.BWD_0_6km > 50:
            reasons.append(f"Shear 0–6km {kinematics.BWD_0_6km:.0f} kt")

    return bool(reasons), reasons


def _feature_vector(
    indices,
    kinematics,
    tc_gap_12z: float | None,
    mode: str,
) -> list[float] | None:
    """
    Build a normalized, weighted feature vector for analogue distance scoring.

    Returns None if required fields are missing.
    All components are already multiplied by their weight so that
    Euclidean distance reflects relative importance directly.

    Modes
    -----
    cap        — cap/thermodynamic similarity (default)
    full       — cap + kinematics blended
    kinematics — shear/helicity similarity; ignores cap state
    """
    if indices is None:
        return None

    # Normalize to [0, 1] using domain-typical Oklahoma ranges
    def _norm(v, lo, hi):
        return max(0.0, min(1.0, (v - lo) / (hi - lo)))

    mlcin    = _norm(indices.MLCIN,              0,   350)   # J/kg
    cap      = _norm(indices.cap_strength,       0,   7.0)   # °C
    cape     = _norm(indices.MLCAPE,             0,  5000)   # J/kg
    lapse    = _norm(indices.lapse_rate_700_500, 5.0, 10.0)  # °C/km
    tc_gap   = _norm(tc_gap_12z if tc_gap_12z is not None else 20, -15, 70)

    if mode == "cap":
        weights = [0.30, 0.28, 0.20, 0.12, 0.10]
        raw     = [mlcin, cap, tc_gap, cape, lapse]
    elif mode == "kinematics":
        if kinematics is None:
            return None
        srh1  = _norm(kinematics.SRH_0_1km,           0, 600)
        srh3  = _norm(kinematics.SRH_0_3km,           0, 800)
        shear = _norm(kinematics.BWD_0_6km,            0,  80)
        ehi   = _norm((kinematics.EHI or 0),           0,   8)
        cape2 = _norm(indices.MLCAPE,                  0, 5000)
        weights = [0.30, 0.25, 0.20, 0.15, 0.10]
        raw     = [srh1, srh3, shear, ehi, cape2]
    else:  # full — cap + kinematics blended
        srh    = _norm(kinematics.SRH_0_3km if kinematics else 0,    0, 600)
        shear  = _norm(kinematics.BWD_0_6km if kinematics else 0,    0,  80)
        stp    = _norm((kinematics.STP or 0) if kinematics else 0,   0,   8)
        weights = [0.22, 0.20, 0.14, 0.12, 0.08, 0.12, 0.08, 0.04]
        raw     = [mlcin, cap, tc_gap, cape, lapse, srh, shear, stp]

    return [w * v for w, v in zip(weights, raw)]


def _analogue_distance(a: list[float], b: list[float]) -> float:
    return sum((x - y) ** 2 for x, y in zip(a, b)) ** 0.5


def _print_analogues(
    console,
    target_id: str,
    analogues: list[tuple[float, object]],  # (dist, HistoricalCase)
) -> None:
    from ok_weather_model.models import CapBehavior

    def _cb_color(cb):
        return {
            "CLEAN_EROSION":   "bright_green",
            "EARLY_EROSION":   "green",
            "LATE_EROSION":    "yellow",
            "NO_EROSION":      "red",
            "BOUNDARY_FORCED": "cyan",
        }.get(cb.value if cb else "", "white")

    def _ec_color(ec):
        return {
            "SIGNIFICANT_OUTBREAK": "bright_red",
            "ISOLATED_SIGNIFICANT": "red",
            "MARGINAL_EVENT":       "yellow",
            "NULL_BUST":            "bright_blue",
            "ACTIVE_NULL":          "blue",
        }.get(ec.value if ec else "", "white")

    tbl = Table(
        title=f"Historical Analogues — {target_id}",
        show_lines=True,
    )
    tbl.add_column("Case",         style="cyan",  min_width=12)
    tbl.add_column("Dist",         justify="right")
    tbl.add_column("MLCAPE",       justify="right")
    tbl.add_column("MLCIN",        justify="right")
    tbl.add_column("Cap °C",       justify="right")
    tbl.add_column("12Z Tc Gap",   justify="right")
    tbl.add_column("Cap Behavior")
    tbl.add_column("Event Class")
    tbl.add_column("Tornadoes",    justify="right")

    for dist, c in analogues:
        idx   = c.sounding_12Z
        cb    = c.cap_behavior
        ec    = c.event_class
        cb_c  = _cb_color(cb)
        ec_c  = _ec_color(ec)
        cape  = f"{idx.MLCAPE:.0f}" if idx else "—"
        cin   = f"{idx.MLCIN:.0f}"  if idx else "—"
        cap   = f"{idx.cap_strength:.1f}" if idx else "—"
        gap   = f"{c.convective_temp_gap_12Z:+.1f}°F" if c.convective_temp_gap_12Z is not None else "—"

        tbl.add_row(
            c.case_id,
            f"{dist:.3f}",
            cape,
            cin,
            cap,
            gap,
            f"[{cb_c}]{cb.value if cb else '—'}[/{cb_c}]",
            f"[{ec_c}]{ec.value if ec else '—'}[/{ec_c}]",
            str(c.tornado_count),
        )

    console.print(tbl)

    # Outcome distribution summary
    outcomes: dict[str, int] = {}
    for _, c in analogues:
        key = c.cap_behavior.value if c.cap_behavior else "UNKNOWN"
        outcomes[key] = outcomes.get(key, 0) + 1

    console.print("\n[bold]Outcome distribution:[/bold]")
    total = len(analogues)
    for k, n in sorted(outcomes.items(), key=lambda x: -x[1]):
        bar = "█" * n + "░" * (total - n)
        pct = 100 * n / total
        color = {
            "CLEAN_EROSION": "bright_green", "EARLY_EROSION": "green",
            "LATE_EROSION": "yellow", "NO_EROSION": "red",
            "BOUNDARY_FORCED": "cyan",
        }.get(k, "white")
        console.print(f"  [{color}]{k:<20}[/{color}]  {bar}  {n}/{total} ({pct:.0f}%)")


# ── find-analogues ────────────────────────────────────────────────────────────

@cli.command("find-analogues")
@click.argument("case_ref")
@click.option("--top", default=10, show_default=True,
              help="Number of closest analogues to return")
@click.option("--mode", type=click.Choice(["cap", "full", "kinematics"]),
              default="cap", show_default=True,
              help="cap = cap diagnostics; full = cap+kinematics; kinematics = shear/SRH only")
@click.option("--min-year", default=None, type=int,
              help="Restrict analogues to cases from this year onward")
def find_analogues(case_ref: str, top: int, mode: str, min_year: int | None):
    """
    Find historical cases with the most similar 12Z sounding signature.

    CASE_REF: case_id (e.g. 19990503_OK) or date (e.g. 1999-05-03)

    In 'cap' mode the distance metric weights MLCIN, cap strength, and
    convective temperature gap most heavily — best for finding cases with
    a similar erosion challenge.

    In 'kinematics' mode the metric weights SRH 0-1km, SRH 0-3km, bulk shear,
    and EHI — useful for asking "what happened on other days with this wind
    profile?" regardless of cap state.

    In 'full' mode kinematic parameters (SRH, bulk shear, STP) are also
    included — best for finding end-to-end analogues for a storm event.
    """
    from ok_weather_model.storage import Database

    db = Database()
    case_id, case_date = _resolve_case_ref(case_ref)
    target = db.load_case(case_id)

    if target is None:
        console.print(f"[red]Case {case_id} not found.[/red]")
        return
    if target.sounding_12Z is None:
        console.print(f"[red]{case_id} has no 12Z sounding data.[/red]")
        return

    target_vec = _feature_vector(
        target.sounding_12Z,
        target.kinematics_12Z,
        target.convective_temp_gap_12Z,
        mode,
    )
    if target_vec is None:
        console.print("[red]Cannot build feature vector for target case.[/red]")
        return

    console.rule(f"[bold]Analogues for {case_id} — mode={mode}[/bold]")

    all_cases = db.query_parameter_space({
        "start_date": str(date(min_year or CASE_LIBRARY_START_YEAR, 1, 1)),
        "end_date":   str(date(CASE_LIBRARY_END_YEAR, 12, 31)),
    })

    scored: list[tuple[float, object]] = []
    for c in all_cases:
        if c.case_id == case_id:
            continue
        if c.sounding_12Z is None:
            continue
        vec = _feature_vector(
            c.sounding_12Z,
            c.kinematics_12Z,
            c.convective_temp_gap_12Z,
            mode,
        )
        if vec is None:
            continue
        scored.append((_analogue_distance(target_vec, vec), c))

    scored.sort(key=lambda x: x[0])
    top_analogues = scored[:top]

    if not top_analogues:
        console.print("[yellow]No analogues found — run enrich-all first.[/yellow]")
        return

    _print_analogues(console, case_id, top_analogues)


# ── analyze-now ───────────────────────────────────────────────────────────────

@cli.command("analyze-now")
@click.option("--station", default="OUN", show_default=True,
              help="Sounding station (OUN, LMN, AMA, DDC)")
@click.option("--hour", default=None, type=int,
              help="Specific UTC hour to fetch (auto-detects latest if omitted)")
@click.option("--analogues", "n_analogues", default=8, show_default=True,
              help="Number of historical analogues to display")
@click.option("--mode", type=click.Choice(["cap", "full", "kinematics"]),
              default="cap", show_default=True,
              help="Analogue scoring: cap = thermodynamic; full = cap+kinematics; kinematics = shear/SRH only")
def analyze_now(station: str, hour: int | None, n_analogues: int, mode: str):
    """
    Fetch the latest available sounding and analyze current cap conditions.

    Computes thermodynamic indices, runs the CES heating model, and finds
    the closest historical analogues from the case library.

    Useful for real-time situational awareness on active convective days.
    """
    from ok_weather_model.storage import Database
    from ok_weather_model.ingestion import SoundingClient, MesonetClient
    from ok_weather_model.processing import (
        compute_thermodynamic_indices,
        compute_kinematic_profile,
    )
    from ok_weather_model.processing.cap_calculator import compute_ces_from_sounding
    from ok_weather_model.models import OklahomaSoundingStation

    now_utc = datetime.now(tz=timezone.utc)
    today   = now_utc.date()

    # Resolve station enum
    try:
        stn = OklahomaSoundingStation[station.upper()]
    except KeyError:
        console.print(f"[red]Unknown station '{station}'. Use OUN, LMN, AMA, or DDC.[/red]")
        return

    # Auto-detect the latest available sounding hour
    standard_hours = (21, 18, 15, 12, 9, 6, 3, 0)
    if hour is not None:
        hours_to_try = [hour]
    else:
        current_utc_hour = now_utc.hour
        hours_to_try = [h for h in standard_hours if h <= current_utc_hour]
        if not hours_to_try:
            hours_to_try = [0]

    console.rule(f"[bold]Real-Time Cap Analysis — {stn.value} / {today.isoformat()}[/bold]")

    profile = None
    fetched_hour = None
    with console.status("Fetching latest sounding..."):
        with SoundingClient() as sc:
            for h in hours_to_try:
                profile = sc.get_sounding(stn, today, h)
                if profile is not None:
                    fetched_hour = h
                    break

    if profile is None:
        console.print(f"[red]No sounding found for {stn.value} on {today}. "
                      f"Tried hours: {hours_to_try}[/red]")
        return

    console.print(
        f"[green]Sounding: {stn.value} {today} {fetched_hour:02d}Z "
        f"({len(profile.levels)} levels)[/green]"
    )

    with console.status("Computing thermodynamic indices..."):
        try:
            indices   = compute_thermodynamic_indices(profile)
            kinematics = compute_kinematic_profile(profile, indices)
        except Exception as exc:
            console.print(f"[red]MetPy computation failed: {exc}[/red]")
            return

    # ── Color helpers ─────────────────────────────────────────────────────────
    def _cape_c(v):
        if v >= 3000: return "bright_red"
        if v >= 2000: return "red"
        if v >= 1000: return "yellow"
        if v >= 500:  return "green"
        return "white"

    def _cin_c(v):
        if v >= 200:  return "bright_red"
        if v >= 100:  return "red"
        if v >= 50:   return "yellow"
        if v >= 20:   return "bright_yellow"
        return "green"

    def _cap_c(v):
        if v >= 4.0:  return "bright_red"
        if v >= 2.0:  return "red"
        if v >= 1.0:  return "yellow"
        return "green"

    def _srh_c(v):
        if v >= 400: return "bright_red"
        if v >= 250: return "red"
        if v >= 150: return "yellow"
        return "white"

    def _ehi_c(v):
        if v >= 4.0: return "bright_red"
        if v >= 2.5: return "red"
        if v >= 1.5: return "yellow"
        return "white"

    # ── Fetch secondary station (LMN — northern OK, 36.7°N) ──────────────────
    # LMN covers the Grant/Kay/Garfield county corridor — the most common
    # northern-OK initiation zone. Fetching it alongside OUN reveals N-S
    # thermodynamic variation that a single-station view misses entirely.
    lmn_indices   = None
    lmn_kinematics = None
    lmn_fetched   = False
    if stn == OklahomaSoundingStation.OUN:
        with console.status("Fetching LMN (Lamont, northern OK) sounding..."):
            with SoundingClient() as sc2:
                lmn_profile = sc2.get_sounding(
                    OklahomaSoundingStation.LMN, today, fetched_hour
                )
        if lmn_profile is not None:
            try:
                lmn_indices    = compute_thermodynamic_indices(lmn_profile)
                lmn_kinematics = compute_kinematic_profile(lmn_profile, lmn_indices)
                lmn_fetched    = True
            except Exception:
                pass  # LMN failure is non-fatal; proceed with OUN only

    # ── Multi-station comparison table ────────────────────────────────────────
    def _fmt_val(val, fmt, color_fn=None):
        """Format a value with optional Rich color markup."""
        import math as _math
        if val is None or (isinstance(val, float) and _math.isnan(val)):
            return "—"
        s = fmt.format(val)
        if color_fn:
            c = color_fn(val)
            return f"[{c}]{s}[/{c}]"
        return s

    snd_tbl = Table(
        title=f"Sounding Comparison — {fetched_hour:02d}Z  {today}",
        show_lines=True,
    )
    snd_tbl.add_column("Parameter",       style="cyan")
    snd_tbl.add_column("OUN  Norman 35.2°N", justify="right")
    if lmn_fetched:
        snd_tbl.add_column("LMN  Lamont 36.7°N", justify="right")

    def _row(label, oun_val, lmn_val, fmt, color_fn=None):
        cols = [label, _fmt_val(oun_val, fmt, color_fn)]
        if lmn_fetched:
            cols.append(_fmt_val(lmn_val, fmt, color_fn))
        snd_tbl.add_row(*cols)

    _row("MLCAPE",        indices.MLCAPE,             lmn_indices.MLCAPE             if lmn_indices else None, "{:.0f} J/kg",   _cape_c)
    _row("MLCIN",         indices.MLCIN,              lmn_indices.MLCIN              if lmn_indices else None, "{:.0f} J/kg",   _cin_c)
    _row("Cap Strength",  indices.cap_strength,       lmn_indices.cap_strength       if lmn_indices else None, "{:.1f}°C",      _cap_c)
    _row("LCL Height",    indices.LCL_height,         lmn_indices.LCL_height         if lmn_indices else None, "{:.0f} m AGL")
    _row("LFC Height",    indices.LFC_height,         lmn_indices.LFC_height         if lmn_indices else None, "{:.0f} m AGL")
    _row("Lapse 700–500", indices.lapse_rate_700_500, lmn_indices.lapse_rate_700_500 if lmn_indices else None, "{:.1f} °C/km")
    _row("Conv. Temp Tc", indices.convective_temperature, lmn_indices.convective_temperature if lmn_indices else None, "{:.1f}°F")

    # Kinematics rows
    oun_srh1  = kinematics.SRH_0_1km  if kinematics else None
    oun_srh3  = kinematics.SRH_0_3km  if kinematics else None
    oun_bwd6  = kinematics.BWD_0_6km  if kinematics else None
    oun_ehi   = kinematics.EHI        if kinematics else None
    oun_stp   = kinematics.STP        if kinematics else None
    oun_scp   = kinematics.SCP        if kinematics else None
    lmn_srh1  = lmn_kinematics.SRH_0_1km if lmn_kinematics else None
    lmn_srh3  = lmn_kinematics.SRH_0_3km if lmn_kinematics else None
    lmn_bwd6  = lmn_kinematics.BWD_0_6km if lmn_kinematics else None
    lmn_ehi   = lmn_kinematics.EHI       if lmn_kinematics else None
    lmn_stp   = lmn_kinematics.STP       if lmn_kinematics else None
    lmn_scp   = lmn_kinematics.SCP       if lmn_kinematics else None

    _row("SRH 0–1km",    oun_srh1, lmn_srh1, "{:.0f} m²/s²", _srh_c)
    _row("SRH 0–3km",    oun_srh3, lmn_srh3, "{:.0f} m²/s²", _srh_c)
    _row("Shear 0–6km",  oun_bwd6, lmn_bwd6, "{:.0f} kt")
    _row("EHI",          oun_ehi,  lmn_ehi,  "{:.2f}",        _ehi_c)
    _row("STP",          oun_stp,  lmn_stp,  "{:.2f}")
    _row("SCP",          oun_scp,  lmn_scp,  "{:.2f}")

    console.print(snd_tbl)

    # ── DANGEROUS CAPPED warning ──────────────────────────────────────────────
    # Check both stations; the warning fires if EITHER environment is dangerous.
    # A capped northern-OK environment with violent shear is exactly what
    # produced yesterday's tornadoes while OUN looked benign.
    flag_oun, reasons_oun = _dangerous_capped_flag(indices, kinematics)
    flag_lmn, reasons_lmn = _dangerous_capped_flag(lmn_indices, lmn_kinematics)

    if flag_oun or flag_lmn:
        from rich.panel import Panel
        lines = ["[bold red]Cap is strong but the kinematic environment is in violent-tornado range.[/bold red]",
                 "Any boundary-forced initiation — dryline surge, outflow, differential heating —",
                 "could produce significant tornadoes with little or no warning time.", ""]
        if flag_oun and reasons_oun:
            lines.append(f"[yellow]OUN (central OK):[/yellow]  {' | '.join(reasons_oun)}")
        if flag_lmn and reasons_lmn:
            lines.append(f"[yellow]LMN (northern OK):[/yellow] {' | '.join(reasons_lmn)}")
        console.print(Panel(
            "\n".join(lines),
            title="[bold red on white] ⚠  DANGEROUS CAPPED ENVIRONMENT [/bold red on white]",
            border_style="red",
            padding=(1, 2),
        ))

    # ── Mesonet: surface temp + dryline detection ────────────────────────────────
    # Fetch the current snapshot plus snapshots 1 hr and 2 hrs ago so that the
    # dryline detector has enough temporal spread to compute a surge rate.
    # Each fetch retries up to 3 earlier 5-min boundaries to handle archive lag.
    from ok_weather_model.models import OklahomaCounty
    from ok_weather_model.models.mesonet import MesonetTimeSeries as _MTS
    from ok_weather_model.processing import detect_dryline, compute_dryline_surge_rate
    from datetime import timedelta as _td

    surface_temp_c = profile.levels[0].temperature if profile.levels else None
    ces: dict | None = None

    station_series: dict[str, _MTS] = {}
    snap_times_fetched: list[datetime] = []

    with console.status("Fetching Mesonet observations..."), MesonetClient() as mc:
        for hrs_back in (0, 1, 2):
            target = now_utc - _td(hours=hrs_back)
            for lb in range(4):
                snap_time = target - _td(minutes=5 * lb)
                try:
                    snap_obs = mc.get_snapshot_observations(snap_time)
                except Exception:
                    continue
                for o in snap_obs:
                    stid = o.station_id
                    if stid not in station_series:
                        station_series[stid] = _MTS(
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
                snap_times_fetched.append(snap_time)
                break  # got this hour's snapshot; move to the next hrs_back

    # Surface temp for CES: nearest Cleveland county observation to now
    if snap_times_fetched:
        current_snap = snap_times_fetched[0]
        for ts in station_series.values():
            if ts.county == OklahomaCounty.CLEVELAND:
                for o in ts.observations:
                    if abs((o.valid_time - current_snap).total_seconds()) <= 7 * 60:
                        surface_temp_c = (o.temperature - 32) * 5 / 9
                        console.print(
                            f"[dim]Surface temp from Mesonet (NRMN, "
                            f"{current_snap.strftime('%H:%MZ')}): "
                            f"{o.temperature:.1f}°F ({surface_temp_c:.1f}°C)[/dim]"
                        )
                        break
                break

    # ── Dryline detection ─────────────────────────────────────────────────────
    current_dryline = None   # set below if Mesonet data was available
    if snap_times_fetched:
        current_snap = snap_times_fetched[0]
        current_dryline = detect_dryline(station_series, current_snap)

        prev_dryline = None
        if len(snap_times_fetched) >= 2:
            prev_dryline = detect_dryline(station_series, snap_times_fetched[-1])

        if current_dryline is not None:
            surge = (
                compute_dryline_surge_rate(prev_dryline, current_dryline)
                if prev_dryline is not None else None
            )
            dl_tbl = Table(
                title=f"Dryline — detected {current_snap.strftime('%H:%MZ')}",
                show_lines=True,
            )
            dl_tbl.add_column("Parameter", style="cyan")
            dl_tbl.add_column("Value")

            # Polyline: one row per point, labelled S→N
            pts = list(zip(current_dryline.position_lat, current_dryline.position_lon))
            labels = ["S endpoint", "central", "N endpoint"]
            if len(pts) == 2:
                labels = ["S endpoint", "N endpoint"]
            for i, (lat, lon) in enumerate(pts):
                label = labels[i] if i < len(labels) else f"pt {i+1}"
                dl_tbl.add_row(f"Polyline ({label})", f"{lat:.1f}°N, {lon:.1f}°W")
            dl_tbl.add_row(
                "Confidence",
                f"[{'green' if current_dryline.confidence >= 0.6 else 'yellow' if current_dryline.confidence >= 0.35 else 'red'}]"
                f"{current_dryline.confidence:.2f}[/]",
            )
            if surge is not None:
                surge_dir = "eastward" if surge > 0 else "retrograding"
                surge_color = "red" if surge > 15 else "yellow" if surge > 5 else "green"
                dl_tbl.add_row(
                    "Surge rate",
                    f"[{surge_color}]{surge:+.1f} mph ({surge_dir})[/{surge_color}]",
                )
            if current_dryline.counties_intersected:
                county_names = ", ".join(
                    c.name for c in current_dryline.counties_intersected[:6]
                )
                if len(current_dryline.counties_intersected) > 6:
                    county_names += f" +{len(current_dryline.counties_intersected) - 6} more"
                dl_tbl.add_row("Counties intersected", county_names)

            console.print(dl_tbl)
        else:
            console.print("[dim]Dryline: not detected in current Mesonet network.[/dim]")

    if fetched_hour == 12 and surface_temp_c is not None:
        ces = compute_ces_from_sounding(indices, surface_temp_c, today)
        cb  = ces["cap_behavior"]

        def _cb_c(v):
            return {"CLEAN_EROSION": "bright_green", "EARLY_EROSION": "green",
                    "LATE_EROSION": "yellow", "NO_EROSION": "red",
                    "BOUNDARY_FORCED": "cyan"}.get(v.value if v else "", "white")

        ces_tbl = Table(title="CES Projection (12Z Heating Model)", show_lines=True)
        ces_tbl.add_column("Parameter", style="cyan")
        ces_tbl.add_column("Value")

        cb_color = _cb_c(cb)
        ces_tbl.add_row("Projected Cap Behavior",
                        f"[{cb_color}]{cb.value if cb else '—'}[/{cb_color}]")
        ces_tbl.add_row("Projected Erosion",
                        ces["cap_erosion_time"].strftime("%H:%M UTC")
                        if ces["cap_erosion_time"] else "[red]No erosion projected[/red]")
        if ces["convective_temp_gap_12Z"] is not None:
            g = ces["convective_temp_gap_12Z"]
            gc = "green" if g < 0 else ("yellow" if g < 10 else "red")
            ces_tbl.add_row("12Z Tc Gap", f"[{gc}]{g:+.1f}°F[/{gc}]")
        if ces["convective_temp_gap_15Z"] is not None:
            g = ces["convective_temp_gap_15Z"]
            gc = "green" if g < 0 else ("yellow" if g < 10 else "red")
            ces_tbl.add_row("15Z Tc Gap", f"[{gc}]{g:+.1f}°F[/{gc}]")
        if ces["convective_temp_gap_18Z"] is not None:
            g = ces["convective_temp_gap_18Z"]
            gc = "green" if g < 0 else ("yellow" if g < 10 else "red")
            ces_tbl.add_row("18Z Tc Gap", f"[{gc}]{g:+.1f}°F[/{gc}]")

        console.print(ces_tbl)
    elif fetched_hour != 12:
        console.print(
            f"[dim]CES projection uses the 12Z sounding — "
            f"fetched {fetched_hour:02d}Z, skipping CES.[/dim]"
        )

    # ── Risk zone map ─────────────────────────────────────────────────────────
    # Prefer HRRR 3km analysis (actual per-county model data) when available.
    # Fall back to OUN/LMN sounding interpolation for pre-2016 dates or when
    # the HRRR fetch fails.
    console.rule("[bold]County Risk Zones[/bold]")
    from ok_weather_model.processing.risk_zone import (
        compute_risk_zones, compute_risk_zones_from_hrrr,
    )
    from ok_weather_model.ingestion import HRRRClient

    _dryline_for_risk = current_dryline if snap_times_fetched else None
    hrrr_snap = None
    risk_source = "OUN+LMN interpolated"

    # Two HRRR snapshots:
    #   baseline — at the sounding hour (morning environment, what we projected from)
    #   current  — most recent run available right now (how conditions have evolved)
    # The delta between them is the tendency signal: is the cap eroding faster than
    # expected? Are kinematics tightening? This is the automated forecast evolution.
    _hrrr_baseline: object | None = None   # HRRRCountySnapshot at sounding time
    _hrrr_baseline_valid: datetime | None = None
    _hrrr_valid: datetime | None = None
    _hrrr_hour = now_utc.hour

    with console.status("Fetching HRRR snapshots (baseline + current)..."):
        try:
            with HRRRClient() as hc:
                # Baseline: sounding hour (may be several hours ago)
                _base_vt = datetime(today.year, today.month, today.day,
                                    fetched_hour, 0, tzinfo=timezone.utc)
                _hrrr_baseline = hc.get_county_snapshot(_base_vt)
                if _hrrr_baseline is not None:
                    _hrrr_baseline_valid = _base_vt

                # Current: walk back from now until we find a posted run
                # (HRRR analysis files appear ~45-60 min after valid time)
                for _h_back in range(4):
                    _try_hour = (_hrrr_hour - _h_back) % 24
                    _try_date = today if _hrrr_hour >= _h_back else (
                        today - timedelta(days=1)
                    )
                    _vt = datetime(_try_date.year, _try_date.month, _try_date.day,
                                   _try_hour, 0, tzinfo=timezone.utc)
                    # Skip if same as baseline — no tendency to compute
                    if _vt == _base_vt:
                        hrrr_snap = _hrrr_baseline
                        _hrrr_valid = _vt
                        break
                    hrrr_snap = hc.get_county_snapshot(_vt)
                    if hrrr_snap is not None:
                        _hrrr_valid = _vt
                        break
        except Exception as _hrrr_err:
            logger.debug("HRRR fetch failed: %s", _hrrr_err)

    if hrrr_snap is not None:
        risk_zones = compute_risk_zones_from_hrrr(
            hrrr_snap, dryline=_dryline_for_risk, min_tier="MARGINAL"
        )
        _hrrr_label = _hrrr_valid.strftime("%H:%MZ") if _hrrr_valid else "?"
        risk_source = f"HRRR 3km  {_hrrr_label}"
    else:
        risk_zones = compute_risk_zones(
            oun_indices=indices,
            oun_kinematics=kinematics,
            lmn_indices=lmn_indices,
            lmn_kinematics=lmn_kinematics,
            dryline=_dryline_for_risk,
            min_tier="MARGINAL",
        )

    if not risk_zones:
        console.print("[dim]No elevated risk areas identified.[/dim]")
    else:
        rz_tbl = Table(
            title=f"Risk Zones — {risk_source}  {today}",
            show_lines=True,
        )
        rz_tbl.add_column("Tier",         style="bold",    min_width=18)
        rz_tbl.add_column("Counties",                      min_width=8)
        rz_tbl.add_column("Center",       justify="right")
        rz_tbl.add_column("Span",         justify="right")
        rz_tbl.add_column("Peak CAPE",    justify="right")
        rz_tbl.add_column("Peak CIN",     justify="right")
        rz_tbl.add_column("Peak SRH1",    justify="right")
        rz_tbl.add_column("Peak EHI",     justify="right")

        for zone in risk_zones:
            county_names = ", ".join(c.name for c in zone.counties[:6])
            if len(zone.counties) > 6:
                county_names += f" +{len(zone.counties) - 6} more"
            span_str = f"{zone.span_ew_mi:.0f}×{zone.span_ns_mi:.0f} mi"
            center_str = f"{zone.center_lat:.1f}°N, {abs(zone.center_lon):.1f}°W"
            rz_tbl.add_row(
                f"[{zone.color}]{zone.tier}[/{zone.color}]",
                county_names,
                center_str,
                span_str,
                f"{zone.peak_MLCAPE:.0f} J/kg",
                f"{zone.peak_MLCIN:.0f} J/kg",
                f"{zone.peak_SRH_0_1km:.0f} m²/s²",
                f"{zone.peak_EHI:.2f}",
            )

        console.print(rz_tbl)

        # Narrative summary for the highest tier
        top = risk_zones[0]
        if top.tier_rank >= 3:  # DANGEROUS_CAPPED or higher
            county_list = ", ".join(c.name for c in top.counties[:8])
            if len(top.counties) > 8:
                county_list += f" and {len(top.counties) - 8} others"
            console.print(
                f"\n[bold {top.color}]Primary risk corridor:[/bold {top.color}] "
                f"{county_list}\n"
                f"  ~{top.span_ew_mi:.0f} mi wide × {top.span_ns_mi:.0f} mi tall, "
                f"centered {top.center_lat:.1f}°N / {abs(top.center_lon):.1f}°W"
            )

        # ── Per-county drill-down (HRRR only, elevated tiers) ─────────────────
        # When HRRR data is available, show a full parameter breakdown for every
        # county in the top risk tiers so the user can see exactly which counties
        # are driving the risk and compare neighboring counties directly.
        if hrrr_snap is not None and any(z.tier_rank >= 3 for z in risk_zones):
            # Collect counties in DANGEROUS_CAPPED or higher
            threat_counties: list[tuple[str, object]] = []  # (tier, HRRRCountyPoint)
            for zone in risk_zones:
                if zone.tier_rank < 3:
                    continue
                for county in zone.counties:
                    pt = hrrr_snap.get(county)
                    if pt is not None:
                        threat_counties.append((zone.tier, pt))

            if threat_counties:
                # Sort: tier rank desc, then SRH 0-1km desc
                from ok_weather_model.processing.risk_zone import _TIER_RANK as _TR, _TIER_COLOR as _TC
                threat_counties.sort(
                    key=lambda x: (_TR.get(x[0], 0), x[1].SRH_0_1km),
                    reverse=True,
                )

                drill_tbl = Table(
                    title=f"County Drill-Down — HRRR {_hrrr_label}  {today}",
                    show_lines=True,
                )
                drill_tbl.add_column("County",    style="cyan",        min_width=12)
                drill_tbl.add_column("Tier",       min_width=14)
                drill_tbl.add_column("Lat",        justify="right")
                drill_tbl.add_column("MLCAPE",     justify="right")
                drill_tbl.add_column("MLCIN",      justify="right")
                drill_tbl.add_column("SRH 0–1km",  justify="right")
                drill_tbl.add_column("SRH 0–3km",  justify="right")
                drill_tbl.add_column("Shear 0–6",  justify="right")
                drill_tbl.add_column("EHI",        justify="right")
                drill_tbl.add_column("STP",        justify="right")
                drill_tbl.add_column("Td 2m",      justify="right")

                for tier, pt in threat_counties:
                    color = _TC.get(tier, "white")
                    ehi_str = f"{pt.EHI:.2f}" if pt.EHI is not None else "—"
                    stp_str = f"{pt.STP:.2f}" if pt.STP is not None else "—"
                    drill_tbl.add_row(
                        pt.county.name,
                        f"[{color}]{tier}[/{color}]",
                        f"{pt.county.lat:.1f}°N",
                        f"[{_cape_c(pt.MLCAPE)}]{pt.MLCAPE:.0f}[/{_cape_c(pt.MLCAPE)}]",
                        f"[{_cin_c(pt.MLCIN)}]{pt.MLCIN:.0f}[/{_cin_c(pt.MLCIN)}]",
                        f"[{_srh_c(pt.SRH_0_1km)}]{pt.SRH_0_1km:.0f}[/{_srh_c(pt.SRH_0_1km)}]",
                        f"[{_srh_c(pt.SRH_0_3km)}]{pt.SRH_0_3km:.0f}[/{_srh_c(pt.SRH_0_3km)}]",
                        f"{pt.BWD_0_6km:.0f} kt",
                        f"[{_ehi_c(pt.EHI or 0)}]{ehi_str}[/{_ehi_c(pt.EHI or 0)}]",
                        stp_str,
                        f"{pt.dewpoint_2m_F:.0f}°F",
                    )

                console.print(drill_tbl)

        # ── Environment tendency (baseline → current) ─────────────────────────
        # Shows how the threat-area environment has evolved since the sounding.
        # Green = conditions trending toward storm initiation (cap eroding,
        # kinematics tightening). Red = cap rebuilding or shear weakening.
        # Compute baseline risk zones to check if the morning had elevated tiers
        _baseline_risk_zones = []
        if _hrrr_baseline is not None and _hrrr_baseline is not hrrr_snap:
            from ok_weather_model.processing.risk_zone import compute_risk_zones_from_hrrr as _rzh
            _baseline_risk_zones = _rzh(
                _hrrr_baseline, dryline=_dryline_for_risk, min_tier="MARGINAL"
            )

        _has_tendency = (
            _hrrr_baseline is not None
            and hrrr_snap is not None
            and _hrrr_valid != _hrrr_baseline_valid
        )
        _elevated_now      = any(z.tier_rank >= 3 for z in risk_zones)
        _elevated_baseline = any(z.tier_rank >= 3 for z in _baseline_risk_zones)

        if _has_tendency and (_elevated_now or _elevated_baseline):
            baseline_label = _hrrr_baseline_valid.strftime("%H:%MZ")
            current_label  = _hrrr_valid.strftime("%H:%MZ")
            hrs_elapsed = (
                (_hrrr_valid - _hrrr_baseline_valid).total_seconds() / 3600.0
            )

            tend_tbl = Table(
                title=(
                    f"Environment Tendency  "
                    f"{baseline_label} → {current_label}  "
                    f"(+{hrs_elapsed:.0f} hr)  {today}"
                ),
                show_lines=True,
            )
            tend_tbl.add_column("County",      style="cyan", min_width=12)
            tend_tbl.add_column("Tier",         min_width=14)
            tend_tbl.add_column("ΔMLCIN",       justify="right",
                                header_style="bold", style="")
            tend_tbl.add_column("ΔMLCAPE",      justify="right")
            tend_tbl.add_column("ΔSRH 0–1km",   justify="right")
            tend_tbl.add_column("ΔSRH 0–3km",   justify="right")
            tend_tbl.add_column("ΔEHI",          justify="right")
            tend_tbl.add_column("Trend",         justify="center")

            from ok_weather_model.processing.risk_zone import _TIER_RANK as _TR, _TIER_COLOR as _TC

            def _delta_cin_str(d: float) -> str:
                """Negative ΔCIN = cap eroding = good (green). Positive = rebuilding (red)."""
                if d <= -30:  return f"[bright_green]{d:+.0f}[/bright_green]"
                if d <= -10:  return f"[green]{d:+.0f}[/green]"
                if d <    0:  return f"[bright_yellow]{d:+.0f}[/bright_yellow]"
                if d <=  10:  return f"[yellow]{d:+.0f}[/yellow]"
                return f"[red]{d:+.0f}[/red]"

            def _delta_cape_str(d: float) -> str:
                if d >= 500:  return f"[bright_green]{d:+.0f}[/bright_green]"
                if d >= 200:  return f"[green]{d:+.0f}[/green]"
                if d >= -200: return f"[bright_yellow]{d:+.0f}[/bright_yellow]"
                return f"[red]{d:+.0f}[/red]"

            def _delta_srh_str(d: float) -> str:
                if d >= 50:   return f"[bright_green]{d:+.0f}[/bright_green]"
                if d >= 20:   return f"[green]{d:+.0f}[/green]"
                if d >= -20:  return f"[bright_yellow]{d:+.0f}[/bright_yellow]"
                return f"[red]{d:+.0f}[/red]"

            def _delta_ehi_str(d: float) -> str:
                if d >= 0.5:  return f"[bright_green]{d:+.2f}[/bright_green]"
                if d >= 0.1:  return f"[green]{d:+.2f}[/green]"
                if d >= -0.1: return f"[bright_yellow]{d:+.2f}[/bright_yellow]"
                return f"[red]{d:+.2f}[/red]"

            def _trend_arrow(d_cin: float, d_srh: float, d_cape: float) -> str:
                """Summarize overall trend as an arrow + label."""
                score = 0
                if d_cin  <  -10: score += 1
                if d_cin  <= -30: score += 1
                if d_srh  >   20: score += 1
                if d_srh  >   50: score += 1
                if d_cape >  200: score += 1
                if d_cin  >   10: score -= 1
                if d_srh  <  -20: score -= 1
                if score >= 3:  return "[bright_green]▲▲ INCREASING[/bright_green]"
                if score == 2:  return "[green]▲ Increasing[/green]"
                if score == 1:  return "[bright_yellow]→ Slight ▲[/bright_yellow]"
                if score == 0:  return "[yellow]→ Steady[/yellow]"
                if score == -1: return "[red]▼ Decreasing[/red]"
                return "[bright_red]▼▼ DECREASING[/bright_red]"

            # Collect threat counties — prefer current elevated zones; fall back
            # to baseline elevated zones when current tiers have dropped (the
            # collapse itself is the signal worth reporting).
            _ref_zones = risk_zones if _elevated_now else _baseline_risk_zones
            _tend_counties: list[tuple[str, object, object]] = []
            _seen_counties: set = set()
            for zone in _ref_zones:
                if zone.tier_rank < 3:
                    continue
                for county in zone.counties:
                    if county in _seen_counties:
                        continue
                    _seen_counties.add(county)
                    pt_now  = hrrr_snap.get(county)
                    pt_base = _hrrr_baseline.get(county)
                    if pt_now is not None and pt_base is not None:
                        _tend_counties.append((zone.tier, pt_now, pt_base))

            _tend_counties.sort(
                key=lambda x: (_TR.get(x[0], 0), x[1].SRH_0_1km),
                reverse=True,
            )

            for tier, pt_now, pt_base in _tend_counties:
                color   = _TC.get(tier, "white")
                d_cin   = pt_now.MLCIN  - pt_base.MLCIN
                d_cape  = pt_now.MLCAPE - pt_base.MLCAPE
                d_srh1  = pt_now.SRH_0_1km - pt_base.SRH_0_1km
                d_srh3  = pt_now.SRH_0_3km - pt_base.SRH_0_3km
                d_ehi   = (pt_now.EHI or 0.0) - (pt_base.EHI or 0.0)
                tend_tbl.add_row(
                    pt_now.county.name,
                    f"[{color}]{tier}[/{color}]",
                    _delta_cin_str(d_cin),
                    _delta_cape_str(d_cape),
                    _delta_srh_str(d_srh1),
                    _delta_srh_str(d_srh3),
                    _delta_ehi_str(d_ehi),
                    _trend_arrow(d_cin, d_srh1, d_cape),
                )

            console.print(tend_tbl)

            # Headline: summarize the dominant trend across the threat area
            _increasing = sum(
                1 for _, pt_now, pt_base in _tend_counties
                if (pt_now.MLCIN - pt_base.MLCIN) < -10 or
                   (pt_now.SRH_0_1km - pt_base.SRH_0_1km) > 20
            )
            _total_tc = len(_tend_counties)
            if _total_tc > 0:
                _pct = _increasing / _total_tc
                if _pct >= 0.6:
                    console.print(
                        f"[bold bright_green]Threat trend:[/bold bright_green] "
                        f"Environment actively evolving toward initiation — "
                        f"{_increasing}/{_total_tc} counties showing cap erosion or "
                        f"tightening kinematics."
                    )
                elif _pct >= 0.3:
                    console.print(
                        f"[bold yellow]Threat trend:[/bold yellow] "
                        f"Mixed signals — {_increasing}/{_total_tc} counties trending "
                        f"favorable. Monitor closely."
                    )
                else:
                    console.print(
                        f"[bold dim]Threat trend:[/bold dim] "
                        f"Environment relatively steady to unfavorable since "
                        f"{baseline_label}."
                    )

    # ── Historical analogues ──────────────────────────────────────────────────
    console.rule("[bold]Historical Analogues[/bold]")

    db = Database()
    all_cases = db.query_parameter_space({
        "start_date": str(date(CASE_LIBRARY_START_YEAR, 1, 1)),
        "end_date":   str(date(CASE_LIBRARY_END_YEAR, 12, 31)),
    })

    ces_tc_gap = ces.get("convective_temp_gap_12Z") if ces is not None else None
    target_vec = _feature_vector(indices, kinematics, ces_tc_gap, mode)

    if target_vec is None:
        console.print("[yellow]Cannot compute analogue vector.[/yellow]")
        return

    scored = []
    for c in all_cases:
        if c.sounding_12Z is None:
            continue
        vec = _feature_vector(c.sounding_12Z, c.kinematics_12Z, c.convective_temp_gap_12Z, mode)
        if vec is None:
            continue
        scored.append((_analogue_distance(target_vec, vec), c))

    scored.sort(key=lambda x: x[0])
    _print_analogues(console, f"{stn.value}/{today}/{fetched_hour:02d}Z", scored[:n_analogues])


# ── compute-ces ───────────────────────────────────────────────────────────────

@cli.command("compute-ces")
@click.option("--start-year", default=CASE_LIBRARY_START_YEAR, show_default=True,
              help="First year to process")
@click.option("--end-year", default=CASE_LIBRARY_END_YEAR, show_default=True,
              help="Last year to process")
@click.option("--force", is_flag=True, default=False,
              help="Recompute even if cap_behavior is already set")
def compute_ces(start_year: int, end_year: int, force: bool):
    """
    Compute Cap Erosion Score for all sounding-enriched cases.

    Uses the Oklahoma climatological heating model to project when the
    convective temperature (Tc) will be reached from the 12Z surface
    temperature.  Does not require Mesonet data.

    Populates: convective_temp_gap_12Z/15Z/18Z, cap_erosion_time, cap_behavior.
    """
    from ok_weather_model.storage import Database
    from ok_weather_model.processing.cap_calculator import compute_ces_from_sounding

    db = Database()
    cases = db.query_parameter_space({
        "start_date": str(date(start_year, 1, 1)),
        "end_date": str(date(end_year, 12, 31)),
    })

    enriched = [c for c in cases if c.sounding_data_available and c.sounding_12Z is not None]
    to_process = enriched if force else [c for c in enriched if c.cap_behavior is None]

    console.print(
        f"Found [cyan]{len(enriched)}[/cyan] enriched cases. "
        f"Processing [green]{len(to_process)}[/green] "
        f"(skipping [yellow]{len(enriched) - len(to_process)}[/yellow] already done)."
    )

    processed = skipped = errors = 0

    for case in track(to_process, description="Computing CES..."):
        try:
            valid_time = datetime(
                case.date.year, case.date.month, case.date.day, 12, 0, tzinfo=timezone.utc
            )
            sounding = db.load_sounding(OklahomaSoundingStation.OUN, valid_time)

            if sounding is None or not sounding.levels:
                logger.debug("No sounding Parquet for %s — skipping CES", case.case_id)
                skipped += 1
                continue

            surface_temp_c = sounding.levels[0].temperature
            ces = compute_ces_from_sounding(case.sounding_12Z, surface_temp_c, case.date)

            case.convective_temp_gap_12Z = ces["convective_temp_gap_12Z"]
            case.convective_temp_gap_15Z = ces["convective_temp_gap_15Z"]
            case.convective_temp_gap_18Z = ces["convective_temp_gap_18Z"]
            case.cap_erosion_time = ces["cap_erosion_time"]
            case.cap_behavior = ces["cap_behavior"]
            case.recompute_completeness()
            db.save_case(case)
            processed += 1

        except Exception as exc:
            errors += 1
            logger.exception("CES failed for %s: %s", case.case_id, exc)

    console.print(
        f"\n[bold green]Processed: {processed}[/bold green]  "
        f"Skipped (no sounding): {skipped}  "
        f"Errors: {errors}"
    )

    # ── Distribution summary ──────────────────────────────────────────────────
    updated = db.query_parameter_space({
        "start_date": str(date(start_year, 1, 1)),
        "end_date": str(date(end_year, 12, 31)),
    })

    behavior_counts: dict[str, int] = {}
    for c in updated:
        key = c.cap_behavior.value if c.cap_behavior else "NOT_COMPUTED"
        behavior_counts[key] = behavior_counts.get(key, 0) + 1

    behavior_labels = {
        "EARLY_EROSION":   "Eroded before 18Z — early afternoon initiation window",
        "CLEAN_EROSION":   "Eroded 18Z–21Z — peak storm window (1–4pm CDT)",
        "LATE_EROSION":    "Eroded after 21Z — marginal or late initiation",
        "NO_EROSION":      "Cap held through 02Z — bust candidate",
        "BOUNDARY_FORCED": "Boundary-forced erosion (set manually)",
        "NOT_COMPUTED":    "No sounding data available",
    }

    table = Table(
        title=f"Cap Behavior Distribution ({start_year}–{end_year})",
        show_lines=True,
    )
    table.add_column("Cap Behavior", style="cyan")
    table.add_column("Cases", justify="right", style="green")
    table.add_column("Description")

    for beh, count in sorted(behavior_counts.items(), key=lambda x: -x[1]):
        table.add_row(beh, str(count), behavior_labels.get(beh, ""))

    console.print(table)

    # ── Cross-tab: cap_behavior vs event_class ────────────────────────────────
    cross: dict[str, dict[str, int]] = {}
    for c in updated:
        beh = c.cap_behavior.value if c.cap_behavior else "NOT_COMPUTED"
        cls = c.event_class.value
        if beh not in cross:
            cross[beh] = {}
        cross[beh][cls] = cross[beh].get(cls, 0) + 1

    event_classes = sorted({c.event_class.value for c in updated})
    cross_table = Table(
        title="Cap Behavior × Event Class",
        show_lines=True,
    )
    cross_table.add_column("Cap Behavior", style="cyan")
    for ec in event_classes:
        cross_table.add_column(ec, justify="right")

    for beh in sorted(cross.keys()):
        row = [beh] + [str(cross[beh].get(ec, 0)) for ec in event_classes]
        cross_table.add_row(*row)

    console.print(cross_table)


# ── build-bust-database ───────────────────────────────────────────────────────

@cli.command("build-bust-database")
@click.option("--spc-threshold", default=0.10, show_default=True,
              help="SPC tornado probability threshold for bust identification (0.0–1.0)")
def build_bust_database(spc_threshold: float):
    """
    Identify bust days (high SPC prob, low outcome) and alarm-bell days.
    Cross-references SPC outlooks against actual tornado counts.

    Note: Requires SPC_max_tornado_prob to be populated in cases.
    Days without that data are skipped.
    """
    from ok_weather_model.storage import Database

    db = Database()
    all_cases = db.query_parameter_space({})

    cases_with_probs = [c for c in all_cases if c.SPC_max_tornado_prob is not None]

    if not cases_with_probs:
        console.print(
            "[yellow]No cases with SPC probability data found. "
            "Enrich cases with SPC outlook data first.[/yellow]"
        )
        return

    bust_cases = [
        c for c in cases_with_probs
        if c.SPC_max_tornado_prob >= spc_threshold and c.tornado_count == 0
    ]
    alarm_bell_cases = [
        c for c in cases_with_probs
        if c.SPC_max_tornado_prob < 0.05 and c.tornado_count >= 10
    ]
    verified_cases = [
        c for c in cases_with_probs
        if c.SPC_max_tornado_prob >= spc_threshold and c.tornado_count >= 5
    ]

    console.rule("[bold red]Bust Database Summary[/bold red]")

    table = Table(show_lines=True)
    table.add_column("Category", style="cyan")
    table.add_column("Count", justify="right", style="green")
    table.add_column("Description")

    table.add_row("Total cases with SPC prob", str(len(cases_with_probs)), "")
    table.add_row(
        f"Bust cases (prob ≥ {spc_threshold:.0%}, 0 tornadoes)",
        str(len(bust_cases)),
        "High forecast, no outcome"
    )
    table.add_row(
        "Alarm bell cases (prob < 5%, ≥ 10 tornadoes)",
        str(len(alarm_bell_cases)),
        "Low forecast, major outbreak"
    )
    table.add_row(
        f"Verified cases (prob ≥ {spc_threshold:.0%}, ≥ 5 tornadoes)",
        str(len(verified_cases)),
        "Good forecast skill"
    )

    console.print(table)

    if bust_cases:
        console.print("\n[bold red]Top 10 Bust Cases:[/bold red]")
        bust_table = Table(show_lines=True)
        bust_table.add_column("Date")
        bust_table.add_column("SPC Prob", justify="right")
        bust_table.add_column("Actual Tornadoes", justify="right")
        bust_table.add_column("Cap Behavior")

        for case in sorted(bust_cases, key=lambda c: c.SPC_max_tornado_prob or 0, reverse=True)[:10]:
            bust_table.add_row(
                case.date.isoformat(),
                f"{case.SPC_max_tornado_prob:.0%}",
                str(case.tornado_count),
                case.cap_behavior.value if case.cap_behavior else "—",
            )
        console.print(bust_table)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _resolve_case_ref(case_ref: str) -> tuple[str, date]:
    """
    Accept either a case_id (YYYYMMDD_OK) or a date string (YYYY-MM-DD or YYYYMMDD).
    Returns (case_id, case_date).
    """
    import re

    if re.fullmatch(r"\d{8}_OK", case_ref):
        d = date(int(case_ref[:4]), int(case_ref[4:6]), int(case_ref[6:8]))
        return case_ref, d

    # Try to parse as date
    for fmt in ("%Y-%m-%d", "%Y%m%d"):
        try:
            d = datetime.strptime(case_ref, fmt).date()
            return HistoricalCase.make_case_id(d), d
        except ValueError:
            continue

    raise click.BadParameter(
        f"Cannot parse case reference '{case_ref}'. "
        f"Expected YYYYMMDD_OK or YYYY-MM-DD or YYYYMMDD."
    )


def _classify_cap_behavior(
    trajectory,
    case_date: date,
) -> CapBehavior:
    """
    Classify CapBehavior from the erosion trajectory.
    """
    from ok_weather_model.processing.cap_calculator import MARGINAL_CAP, MODERATE_CAP

    if not trajectory.erosion_achieved:
        return CapBehavior.NO_EROSION

    if trajectory.erosion_time is None:
        return CapBehavior.NO_EROSION

    # Check if boundary forcing drove erosion
    if trajectory.primary_mechanism.value == "BOUNDARY":
        return CapBehavior.BOUNDARY_FORCED

    # Convert erosion datetime to hours-since-midnight UTC for the case date
    # so that next-day projections (e.g. 10Z+1) don't misclassify as EARLY.
    case_midnight = datetime(
        case_date.year, case_date.month, case_date.day, 0, 0, tzinfo=timezone.utc
    )
    erosion_hours = (trajectory.erosion_time - case_midnight).total_seconds() / 3600.0

    # Timing-based classification (Oklahoma climatology reference times)
    if erosion_hours <= 18:  # before 18Z (1pm CDT)
        return CapBehavior.EARLY_EROSION
    elif erosion_hours <= 21:  # 18Z–21Z (1–4pm CDT)
        return CapBehavior.CLEAN_EROSION
    elif erosion_hours <= 26:  # 21Z–02Z (4pm–9pm CDT) — late or nocturnal
        return CapBehavior.LATE_EROSION
    else:
        return CapBehavior.NO_EROSION


if __name__ == "__main__":
    cli()
