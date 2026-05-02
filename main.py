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


# ── analyze-now: API cache helper ────────────────────────────────────────────

def _try_api_state(max_age_s: int = 900) -> "dict | None":
    """
    Try to load the pre-computed dashboard state from the running backend.

    Returns the state dict if the backend is reachable and the data is fresh
    (updated within *max_age_s* seconds).  Returns None otherwise — callers
    should fall back to direct network fetches.

    The state includes a ``state_hash`` field (12-char SHA-256 fingerprint) that
    changes whenever HRRR data, tier map, alert count, or outlook category
    changes.  Callers can compare hashes across runs to detect silent updates.
    """
    import urllib.request as _ur
    import json as _json
    try:
        with _ur.urlopen("http://localhost:8000/api/state", timeout=2) as r:
            state = _json.loads(r.read())
        updated_at = state.get("updated_at")
        if not updated_at:
            return None
        age_s = (
            datetime.now(tz=timezone.utc)
            - datetime.fromisoformat(updated_at)
        ).total_seconds()
        if age_s > max_age_s:
            return None          # stale
        state["_cache_age_s"] = int(age_s)
        return state
    except Exception:
        return None              # backend not running, or network error


# ── analyze-now forecast helper ──────────────────────────────────────────────

def _analyze_now_forecast(
    forecast_hour: int,
    now_utc: datetime,
    api_state: "dict | None" = None,
) -> None:
    """
    Fetch an HRRR forecast snapshot for a future valid time and display county
    risk zones.  Called by analyze-now when --forecast-hour N is given.

    When *api_state* is provided (backend cache is available) and forecast_hour
    is 0, the cached hrrr_counties + tier_map are used directly, skipping the
    S3/Herbie fetch entirely.

    Finds the most recently initialized HRRR run whose fxx reaches the desired
    valid time within the 1–48 h forecast window, then calls
    compute_risk_zones_from_hrrr() exactly as the current-conditions path does.
    """
    from ok_weather_model.ingestion import HRRRClient
    from ok_weather_model.processing.risk_zone import (
        compute_risk_zones_from_hrrr,
        _TIER_RANK as _TR,
        _TIER_COLOR as _TC,
    )

    # Desired valid time (round to the current whole hour, then add offset)
    valid_time = (
        now_utc.replace(minute=0, second=0, microsecond=0)
        + timedelta(hours=forecast_hour)
    )
    valid_date = valid_time.date()

    console.rule(
        f"[bold]HRRR County Forecast — F+{forecast_hour}  "
        f"valid {valid_time.strftime('%Y-%m-%d %H:%MZ')}[/bold]"
    )

    # ── Cache path for F+0 (current conditions) ───────────────────────────────
    # When the backend is running and forecast_hour == 0, its hrrr_counties is
    # the same data we'd fetch — skip the S3/Herbie round-trip entirely.
    if forecast_hour == 0 and api_state and api_state.get("hrrr_counties") and api_state.get("hrrr_valid"):
        from ok_weather_model.models.hrrr import HRRRCountyPoint as _HCP, HRRRCountySnapshot as _HCS
        try:
            _pts  = [_HCP(**_p) for _p in api_state["hrrr_counties"]]
            _hvt  = datetime.fromisoformat(api_state["hrrr_valid"].replace("Z", "+00:00"))
            _snap = _HCS(run_time=_hvt, valid_time=_hvt, counties=_pts)
            _age  = api_state.get("_cache_age_s", 0)
            _hash = api_state.get("state_hash", "?")
            console.print(
                f"[dim]HRRR from API cache — {_age}s old  {_hvt.strftime('%H:%MZ')}  "
                f"hash:[bold]{_hash}[/bold][/dim]"
            )
            _rzones = compute_risk_zones_from_hrrr(_snap, min_tier="MARGINAL")
            if not _rzones:
                console.print("[dim]No elevated risk areas identified.[/dim]")
                return
            _rz_tbl = Table(
                title=f"HRRR F+0  {_hvt.strftime('%Y-%m-%d %H:%MZ')}  (cached)",
                show_lines=True,
            )
            for _col, _kw in [("Tier", {"style":"bold","min_width":18}),
                               ("Counties", {"min_width":8}),
                               ("Center", {"justify":"right"}), ("Span", {"justify":"right"}),
                               ("Peak CAPE", {"justify":"right"}), ("Peak CIN", {"justify":"right"}),
                               ("Peak SRH1", {"justify":"right"}), ("Peak EHI", {"justify":"right"})]:
                _rz_tbl.add_column(_col, **_kw)
            for _z in _rzones:
                _cn = ", ".join(c.name for c in _z.counties[:6])
                if len(_z.counties) > 6:
                    _cn += f" +{len(_z.counties)-6} more"
                _rz_tbl.add_row(
                    f"[{_z.color}]{_z.tier}[/{_z.color}]", _cn,
                    f"{_z.center_lat:.1f}°N, {abs(_z.center_lon):.1f}°W",
                    f"{_z.span_ew_mi:.0f}×{_z.span_ns_mi:.0f} mi",
                    f"{_z.peak_MLCAPE:.0f} J/kg", f"{_z.peak_MLCIN:.0f} J/kg",
                    f"{_z.peak_SRH_0_1km:.0f} m²/s²", f"{_z.peak_EHI:.2f}",
                )
            console.print(_rz_tbl)
            return
        except Exception as _ce:
            logger.debug("Cache F+0 reconstruction failed: %s", _ce)
            # Fall through to live fetch

    # Find the best posted HRRR run for this valid time.
    # HRRR posts runs incrementally — not all fxx hours are available immediately.
    # Strategy: build a ranked list of (run_time, fxx) candidates (most recently
    # initialized first, i.e. smallest fxx), then probe each with Herbie until we
    # find one whose GRIB file is actually on S3.
    try:
        from herbie import Herbie as _Herbie
    except ImportError:
        console.print("[red]herbie-data is not installed.[/red]")
        return

    now_floor  = now_utc.replace(minute=0, second=0, microsecond=0)
    posted_cut = now_floor - timedelta(hours=1)   # ~60-min posting lag

    # Build candidate list: iterate h_back from 0 (most recent) to 47 (oldest)
    candidates: list[tuple[datetime, int]] = []
    for h_back in range(48):
        run_try  = posted_cut - timedelta(hours=h_back)
        fxx_try  = int((valid_time - run_try).total_seconds() / 3600)
        if 1 <= fxx_try <= 48:
            candidates.append((run_try, fxx_try))

    if not candidates:
        console.print(
            f"[red]Cannot map F+{forecast_hour} to any HRRR run within the "
            f"1–48 h forecast window.[/red]"
        )
        return

    # Probe candidates until we find one with GRIB files on S3
    best_run_time: datetime | None = None
    best_fxx: int | None = None
    with console.status(f"Locating best posted HRRR run for F+{forecast_hour}..."):
        for run_try, fxx_try in candidates[:8]:   # check up to 8 candidates
            run_str = run_try.strftime("%Y-%m-%d %H:%M")
            try:
                H_probe = _Herbie(run_str, model="hrrr", product="sfc",
                                  fxx=fxx_try, verbose=False)
                if H_probe.grib is not None:
                    best_run_time = run_try
                    best_fxx = fxx_try
                    break
            except Exception:
                continue

    if best_fxx is None or best_run_time is None:
        console.print(
            f"[red]No posted HRRR run found for valid {valid_time.strftime('%H:%MZ')} "
            f"{valid_date}. Try again in a few minutes.[/red]"
        )
        return

    run_label = best_run_time.strftime("%Y-%m-%d %H:%MZ")
    console.print(
        f"[dim]Using HRRR run {run_label}  F{best_fxx:02d}  "
        f"→ valid {valid_time.strftime('%H:%MZ')} {valid_date}[/dim]"
    )

    hrrr_fc: object | None = None
    with console.status(
        f"Fetching HRRR {run_label} F{best_fxx:02d}  "
        f"(valid {valid_time.strftime('%H:%MZ')})..."
    ):
        try:
            with HRRRClient() as hc:
                hrrr_fc = hc.get_county_snapshot(valid_time, fxx=best_fxx)
        except Exception as exc:
            logger.debug("HRRR forecast fetch failed: %s", exc)

    if hrrr_fc is None:
        console.print(
            f"[red]HRRR fields could not be extracted for run {run_label} F{best_fxx:02d}. "
            f"Try again in a few minutes.[/red]"
        )
        return

    # ── Risk zones ────────────────────────────────────────────────────────────
    risk_zones = compute_risk_zones_from_hrrr(hrrr_fc, min_tier="MARGINAL")

    def _cape_c(v):
        if v >= 3000: return "bright_red"
        if v >= 2000: return "red"
        if v >= 1000: return "yellow"
        return "white"

    def _cin_c(v):
        if v >= 200: return "bright_red"
        if v >= 100: return "red"
        if v >= 50:  return "yellow"
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

    if not risk_zones:
        console.print("[dim]No elevated risk areas identified in the HRRR forecast.[/dim]")
        return

    rz_tbl = Table(
        title=(
            f"HRRR Forecast Risk Zones — {run_label} F{best_fxx:02d}  "
            f"valid {valid_time.strftime('%H:%MZ')} {valid_date}"
        ),
        show_lines=True,
    )
    rz_tbl.add_column("Tier",       style="bold", min_width=18)
    rz_tbl.add_column("Counties",               min_width=8)
    rz_tbl.add_column("Center",     justify="right")
    rz_tbl.add_column("Span",       justify="right")
    rz_tbl.add_column("Peak CAPE",  justify="right")
    rz_tbl.add_column("Peak CIN",   justify="right")
    rz_tbl.add_column("Peak SRH1",  justify="right")
    rz_tbl.add_column("Peak EHI",   justify="right")

    for zone in risk_zones:
        county_names = ", ".join(c.name for c in zone.counties[:6])
        if len(zone.counties) > 6:
            county_names += f" +{len(zone.counties) - 6} more"
        rz_tbl.add_row(
            f"[{zone.color}]{zone.tier}[/{zone.color}]",
            county_names,
            f"{zone.center_lat:.1f}°N, {abs(zone.center_lon):.1f}°W",
            f"{zone.span_ew_mi:.0f}×{zone.span_ns_mi:.0f} mi",
            f"{zone.peak_MLCAPE:.0f} J/kg",
            f"{zone.peak_MLCIN:.0f} J/kg",
            f"{zone.peak_SRH_0_1km:.0f} m²/s²",
            f"{zone.peak_EHI:.2f}",
        )

    console.print(rz_tbl)

    # Narrative for highest tier
    top = risk_zones[0]
    if top.tier_rank >= 3:
        county_list = ", ".join(c.name for c in top.counties[:8])
        if len(top.counties) > 8:
            county_list += f" and {len(top.counties) - 8} others"
        console.print(
            f"\n[bold {top.color}]Primary risk corridor:[/bold {top.color}] "
            f"{county_list}\n"
            f"  ~{top.span_ew_mi:.0f} mi wide × {top.span_ns_mi:.0f} mi tall, "
            f"centered {top.center_lat:.1f}°N / {abs(top.center_lon):.1f}°W"
        )

    # ── Per-county drill-down (elevated tiers only) ───────────────────────────
    if any(z.tier_rank >= 3 for z in risk_zones):
        threat_counties: list[tuple[str, object]] = []
        seen: set = set()
        for zone in risk_zones:
            if zone.tier_rank < 3:
                continue
            for county in zone.counties:
                if county in seen:
                    continue
                seen.add(county)
                pt = hrrr_fc.get(county)
                if pt is not None:
                    threat_counties.append((zone.tier, pt))

        threat_counties.sort(
            key=lambda x: (_TR.get(x[0], 0), x[1].SRH_0_1km),
            reverse=True,
        )

        drill_tbl = Table(
            title=(
                f"County Drill-Down — HRRR F{best_fxx:02d}  "
                f"valid {valid_time.strftime('%H:%MZ')} {valid_date}"
            ),
            show_lines=True,
        )
        drill_tbl.add_column("County",   style="cyan",  min_width=12)
        drill_tbl.add_column("Tier",                    min_width=14)
        drill_tbl.add_column("Lat",      justify="right")
        drill_tbl.add_column("MLCAPE",   justify="right")
        drill_tbl.add_column("MLCIN",    justify="right")
        drill_tbl.add_column("SRH 0–1",  justify="right")
        drill_tbl.add_column("SRH 0–3",  justify="right")
        drill_tbl.add_column("Shear 6",  justify="right")
        drill_tbl.add_column("EHI",      justify="right")
        drill_tbl.add_column("STP",      justify="right")
        drill_tbl.add_column("Td 2m",    justify="right")

        for tier, pt in threat_counties:
            color   = _TC.get(tier, "white")
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

    # ── Per-county model predictions (HRRR inference path) ───────────────────
    # Models were trained on 12Z sounding features. Here we feed HRRR county
    # fields into the same feature vector (12/28 fields populated, rest NaN →
    # imputed to training median). Predictions are labelled "(HRRR-based)" to
    # signal the input source. Models are NOT retrained — inference only.
    console.rule("[bold]Forecast Model Predictions (HRRR-based)[/bold]")
    try:
        from ok_weather_model.modeling import SeverityClassifier, TornadoRegressor, load_model
        from ok_weather_model.modeling.features import extract_features_from_hrrr

        _clf: SeverityClassifier = load_model("severity_classifier")
        _reg: TornadoRegressor   = load_model("tornado_regressor")

        if _clf is None or _reg is None:
            console.print(
                "[dim]No trained models found. Run [cyan]train-models[/cyan] first.[/dim]"
            )
        else:
            # Score every threat county; display a compact table for elevated tiers
            from ok_weather_model.processing.risk_zone import _TIER_COLOR as _TC2
            import pandas as _pd

            scored_counties: list[tuple[str, object, dict, dict]] = []
            seen2: set = set()
            for zone in risk_zones:
                if zone.tier_rank < 2:   # MODERATE and above
                    continue
                for county in zone.counties:
                    if county in seen2:
                        continue
                    seen2.add(county)
                    pt = hrrr_fc.get(county)
                    if pt is None:
                        continue
                    feat = extract_features_from_hrrr(pt)
                    X_c  = _pd.DataFrame([feat], columns=_clf.feature_importances_.index.tolist()
                                         if _clf.feature_importances_ is not None
                                         else list(feat.keys()))
                    # Use the pipelines directly (they include the imputer)
                    probs = _clf._pipeline.predict_proba(
                        _pd.DataFrame([feat],
                                      columns=list(feat.keys()))
                    )[0]
                    prob_map = dict(zip(_clf._pipeline.classes_, probs))
                    clf_result = {
                        "significant": round(float(prob_map.get(1, 0.0)), 3),
                        "weak":        round(float(prob_map.get(0, 0.0)), 3),
                    }
                    import math as _math
                    log_pred   = float(_reg._pipeline.predict(
                        _pd.DataFrame([feat], columns=list(feat.keys()))
                    )[0])
                    reg_result = {
                        "expected_count": round(max(0.0, float(__import__("numpy").expm1(log_pred))), 1),
                    }
                    scored_counties.append((zone.tier, pt, clf_result, reg_result))

            if scored_counties:
                mdl_tbl = Table(
                    title=(
                        f"County Model Scores — HRRR F{best_fxx:02d}  "
                        f"valid {valid_time.strftime('%H:%MZ')} {valid_date}  "
                        f"[dim](12/28 features from HRRR; rest imputed to training median)[/dim]"
                    ),
                    show_lines=True,
                )
                mdl_tbl.add_column("County",      style="cyan", min_width=12)
                mdl_tbl.add_column("Tier",                      min_width=14)
                mdl_tbl.add_column("P(Sig.)",     justify="right")
                mdl_tbl.add_column("Exp. Count",  justify="right")
                mdl_tbl.add_column("EHI",         justify="right")
                mdl_tbl.add_column("SRH 0–3",     justify="right")

                _thr = _clf.threshold_
                for tier, pt, clf_r, reg_r in scored_counties:
                    color   = _TC2.get(tier, "white")
                    sig_p   = clf_r["significant"]
                    sig_c   = "bright_red" if sig_p >= _thr else "red" if sig_p >= _thr * 0.7 else "yellow" if sig_p >= _thr * 0.4 else "white"
                    ehi_str = f"{pt.EHI:.2f}" if pt.EHI is not None else "—"
                    mdl_tbl.add_row(
                        pt.county.name,
                        f"[{color}]{tier}[/{color}]",
                        f"[{sig_c}]{sig_p:.0%}[/{sig_c}]",
                        f"{reg_r['expected_count']:.1f}",
                        ehi_str,
                        f"{pt.SRH_0_3km:.0f} m²/s²",
                    )

                console.print(mdl_tbl)
                console.print(
                    "[dim]Caveat: 16/28 features are NaN (no sounding available for "
                    "forecast times). Probabilities are underestimates — "
                    "the HRRR field magnitudes above are the primary signal.[/dim]"
                )
            else:
                console.print("[dim]No MODERATE+ counties to score.[/dim]")
    except Exception as _me:
        logger.debug("Forecast model scoring failed: %s", _me)
        console.print("[dim]Model scoring unavailable (run train-models first).[/dim]")

    # ── HRRR prs virtual sounding → full 28-feature model predictions ────────
    # Extracts a SoundingProfile at OUN from the HRRR prs (pressure-level) product
    # valid at 12Z of the forecast date — the same pre-convective window the models
    # were trained on.  Closes the feature-imputation gap from the HRRR sfc path.
    console.rule("[bold]Full Model Predictions (HRRR Virtual Sounding — OUN 12Z)[/bold]")
    try:
        from ok_weather_model.modeling import (
            SeverityClassifier, TornadoRegressor, load_model,
            extract_features_from_indices, FEATURE_NAMES,
        )
        from ok_weather_model.processing import (
            compute_thermodynamic_indices,
            compute_kinematic_profile,
            extract_virtual_sounding_from_hrrr,
            find_best_hrrr_prs_run,
        )
        import pandas as _pd3
        import numpy as _np3

        _clf3: SeverityClassifier = load_model("severity_classifier")
        _reg3: TornadoRegressor   = load_model("tornado_regressor")

        if _clf3 is None or _reg3 is None:
            console.print("[dim]No trained models found — run [cyan]train-models[/cyan] first.[/dim]")
        else:
            # Target sounding: 12Z of the forecast date (pre-convective initialisation)
            sounding_target = datetime(
                valid_time.year, valid_time.month, valid_time.day, 12,
                tzinfo=timezone.utc,
            )
            _OUN_LAT, _OUN_LON = 35.22, -97.44

            _vs_result   = None
            _prs_run_lbl = "unknown"
            _prs_fxx_lbl = 0
            with console.status(
                f"Fetching HRRR prs virtual sounding at OUN "
                f"(target {sounding_target.strftime('%H:%MZ %Y-%m-%d')})..."
            ):
                _best_prs = find_best_hrrr_prs_run(sounding_target, now_utc)
                if _best_prs is not None:
                    _prs_run_t, _prs_fxx_lbl = _best_prs
                    _prs_run_lbl = _prs_run_t.strftime("%Y-%m-%d %H:%MZ")
                    _vs_result = extract_virtual_sounding_from_hrrr(
                        sounding_target, _prs_fxx_lbl, _OUN_LAT, _OUN_LON
                    )

            if _vs_result is None:
                console.print(
                    f"[dim]HRRR prs virtual sounding unavailable for "
                    f"{sounding_target.strftime('%Y-%m-%d %H:%MZ')}. "
                    f"HRRR prs may not be posted this far ahead yet.[/dim]"
                )
                console.print(
                    f"[dim]CES and historical analogues require a real sounding.[/dim]"
                )
            else:
                _vs_idx  = compute_thermodynamic_indices(_vs_result)
                _vs_kin  = compute_kinematic_profile(_vs_result, _vs_idx)
                _vs_feat = extract_features_from_indices(_vs_idx, _vs_kin)
                _vs_X    = _pd3.DataFrame([_vs_feat], columns=FEATURE_NAMES)

                _vs_probs   = _clf3._pipeline.predict_proba(_vs_X)[0]
                _vs_prob_map = dict(zip(_clf3._pipeline.classes_, _vs_probs))
                _vs_sig_p   = float(_vs_prob_map.get(1, 0.0))

                _vs_log   = float(_reg3._pipeline.predict(_vs_X)[0])
                _vs_count = max(0.0, float(_np3.expm1(_vs_log)))

                _thr3 = _clf3.threshold_
                _sig_c = (
                    "bright_red" if _vs_sig_p >= _thr3 else
                    "red"        if _vs_sig_p >= _thr3 * 0.7 else
                    "yellow"     if _vs_sig_p >= _thr3 * 0.4 else "white"
                )
                console.print(
                    f"[dim]HRRR prs {_prs_run_lbl} F{_prs_fxx_lbl:02d}  "
                    f"→ valid {sounding_target.strftime('%H:%MZ %Y-%m-%d')} at OUN[/dim]"
                )
                console.print(
                    f"  Severity P(Significant): [{_sig_c}]{_vs_sig_p:.0%}[/{_sig_c}]    "
                    f"Expected tornado count: {_vs_count:.1f}"
                )
                console.print(
                    f"  MLCAPE [yellow]{_vs_feat['MLCAPE']:.0f}[/yellow] J/kg  "
                    f"  MLCIN [red]{_vs_feat['MLCIN']:.0f}[/red] J/kg  "
                    f"  LCL {_vs_feat['LCL_height']:.0f} m  "
                    f"  SRH 0–3 {_vs_feat['SRH_0_3km']:.0f} m²/s²  "
                    f"  BWD 0–6 {_vs_feat['BWD_0_6km']:.1f} kt"
                )
                console.print(
                    "[dim](28/28 features populated — no NaN imputation)[/dim]"
                )
                console.print(
                    f"\n[dim]CES and historical analogues require a real radiosonde "
                    f"sounding — not available in forecast mode.[/dim]"
                )
    except Exception as _vse:
        logger.debug("HRRR virtual sounding model run failed: %s", _vse, exc_info=True)
        console.print(
            f"[dim]Virtual sounding model unavailable. "
            f"CES and historical analogues require a real sounding.[/dim]"
        )


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
@click.option("--forecast-hour", "forecast_hour", default=0, type=int,
              help="HRRR forecast valid N hours from now (e.g. 24 = tomorrow). "
                   "Shows county risk map only; skips sounding, CES, and analogues.")
@click.option("--no-cache", "no_cache", is_flag=True, default=False,
              help="Skip API cache; force fresh fetches from all upstream sources.")
def analyze_now(station: str, hour: int | None, n_analogues: int, mode: str,
                forecast_hour: int, no_cache: bool):
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

    # ── API cache probe ────────────────────────────────────────────────────────
    # If the web backend is running, its state is more current than anything
    # we'd fetch fresh: it runs a continuous polling loop and already has HRRR,
    # Mesonet, SPC, and tier data.  We use it for everything except soundings
    # (not cached by the backend) and analogues (computed from local DB).
    #
    # The state_hash field is a 12-char SHA-256 fingerprint that changes when
    # HRRR valid time, tier map, alert count, or outlook category changes — use
    # it to detect silent updates between successive analyze-now runs.
    _api_state: "dict | None" = None if no_cache else _try_api_state(max_age_s=900)
    if _api_state:
        _age  = _api_state.get("_cache_age_s", 0)
        _hash = _api_state.get("state_hash", "?")
        console.print(
            f"[dim]Using API cache — {_age}s old  hash:[bold]{_hash}[/bold][/dim]"
        )

    # ── Forecast mode ─────────────────────────────────────────────────────────
    # When --forecast-hour N is specified, skip sounding/CES/analogues and
    # show HRRR county risk zones for the future valid time instead.
    if forecast_hour > 0:
        _analyze_now_forecast(forecast_hour, now_utc, api_state=_api_state)
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
    # northern-OK initiation zone. FWD (Fort Worth) anchors the southern end
    # of the interpolation corridor (~32.83°N). Together FWD→OUN→LMN give a
    # three-station meridional gradient without requiring model data.
    lmn_indices    = None
    lmn_kinematics = None
    lmn_fetched    = False
    fwd_indices    = None
    fwd_kinematics = None
    if stn == OklahomaSoundingStation.OUN:
        with console.status("Fetching LMN (Lamont) + FWD (Fort Worth) soundings..."):
            with SoundingClient() as sc2:
                lmn_profile = sc2.get_sounding(
                    OklahomaSoundingStation.LMN, today, fetched_hour
                )
            with SoundingClient() as sc3:
                fwd_profile = sc3.get_sounding(
                    OklahomaSoundingStation.FWD, today, fetched_hour
                )
        if lmn_profile is not None:
            try:
                lmn_indices    = compute_thermodynamic_indices(lmn_profile)
                lmn_kinematics = compute_kinematic_profile(lmn_profile, lmn_indices)
                lmn_fetched    = True
            except Exception:
                pass  # LMN failure is non-fatal
        if fwd_profile is not None:
            try:
                fwd_indices    = compute_thermodynamic_indices(fwd_profile)
                fwd_kinematics = compute_kinematic_profile(fwd_profile, fwd_indices)
            except Exception:
                pass  # FWD failure is non-fatal

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
    # Prefer API cache (backend already has current Mesonet + pre-detected dryline).
    # Fall back to direct Mesonet fetch when the backend is not running.
    from ok_weather_model.models import OklahomaCounty
    from ok_weather_model.models.mesonet import MesonetTimeSeries as _MTS
    from ok_weather_model.processing import detect_dryline, compute_dryline_surge_rate
    from datetime import timedelta as _td

    surface_temp_c = profile.levels[0].temperature if profile.levels else None
    ces: dict | None = None

    station_series: dict[str, _MTS] = {}
    snap_times_fetched: list[datetime] = []

    if _api_state and _api_state.get("mesonet_obs"):
        # Reconstruct a minimal station_series from cached obs so dryline + surface
        # temp code below can run unchanged.
        from ok_weather_model.models.mesonet import StationObservation as _SO
        _obs_list = _api_state["mesonet_obs"]
        _snap_time = datetime.fromisoformat(_api_state["updated_at"])
        snap_times_fetched = [_snap_time]
        for _o in _obs_list:
            try:
                _obs = _SO(
                    station_id   = _o["station_id"],
                    county       = OklahomaCounty[_o["county"]] if isinstance(_o.get("county"), str) else _o.get("county"),
                    valid_time   = _snap_time,
                    temperature  = _o["temp_f"],
                    dewpoint     = _o["dewpoint_f"],
                    wind_speed   = _o.get("wind_speed", 0),
                    wind_dir     = _o.get("wind_dir", 0),
                    lat          = _o.get("lat", 0),
                    lon          = _o.get("lon", 0),
                )
                stid = _obs.station_id
                if stid not in station_series:
                    station_series[stid] = _MTS(
                        station_id=stid, county=_obs.county,
                        start_time=_snap_time, end_time=_snap_time, observations=[],
                    )
                station_series[stid].observations.append(_obs)
            except Exception:
                pass
    else:
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

    # ── Moisture return diagnostics ───────────────────────────────────────────
    moisture_return = None
    modified_indices = None
    if snap_times_fetched and station_series:
        from ok_weather_model.processing import compute_moisture_return
        from ok_weather_model.processing.sounding_parser import compute_modified_indices

        current_snap = snap_times_fetched[0]
        current_obs = [
            o for ts in station_series.values()
            for o in ts.observations
            if abs((o.valid_time - current_snap).total_seconds()) <= 7 * 60
        ]

        if current_obs:
            moisture_return = compute_moisture_return(current_obs)

            # Find current surface Td from the OUN-nearest station (Cleveland co.)
            surface_td_f: float | None = None
            for o in current_obs:
                if o.county == OklahomaCounty.CLEVELAND:
                    surface_td_f = o.dewpoint
                    break
            # Fallback: statewide mean Td
            if surface_td_f is None and moisture_return is not None:
                surface_td_f = moisture_return.state_mean_dewpoint_f

            if surface_td_f is not None and surface_temp_c is not None:
                surface_td_c = (surface_td_f - 32) * 5 / 9
                try:
                    modified_indices = compute_modified_indices(
                        profile, surface_temp_c, surface_td_c
                    )
                except Exception as _me:
                    logger.debug("Modified indices failed: %s", _me)

    if moisture_return is not None:
        console.rule("[bold]Surface Moisture / Return Flow[/bold]")
        mr_tbl = Table(show_header=False, box=None, padding=(0, 2))
        mr_tbl.add_column(style="dim")
        mr_tbl.add_column()

        td_color = (
            "green" if moisture_return.state_mean_dewpoint_f >= 60
            else "yellow" if moisture_return.state_mean_dewpoint_f >= 55
            else "red"
        )
        mr_tbl.add_row(
            "State-mean surface Td",
            f"[{td_color}]{moisture_return.state_mean_dewpoint_f:.1f}°F[/{td_color}]"
            f"  ({moisture_return.n_stations} stations)",
        )
        mr_tbl.add_row(
            "South OK / North OK Td",
            f"{moisture_return.south_ok_dewpoint_f:.1f}°F / {moisture_return.north_ok_dewpoint_f:.1f}°F"
            f"  (gradient {moisture_return.moisture_return_gradient_f:+.1f}°F)",
        )
        gulf_color = (
            "green" if moisture_return.gulf_moisture_fraction >= 0.60
            else "yellow" if moisture_return.gulf_moisture_fraction >= 0.30
            else "red"
        )
        mr_tbl.add_row(
            "Gulf moisture coverage",
            f"[{gulf_color}]{moisture_return.gulf_moisture_fraction:.0%}[/{gulf_color}]"
            " of stations ≥ 60°F dewpoint",
        )
        if moisture_return.moisture_axis_lat is not None:
            mr_tbl.add_row(
                "60°F Td axis (est.)",
                f"{moisture_return.moisture_axis_lat:.1f}°N",
            )
        if modified_indices is not None:
            mcape_color = (
                "red" if modified_indices.MLCAPE >= 2000
                else "yellow" if modified_indices.MLCAPE >= 1000
                else "green"
            )
            mr_tbl.add_row(
                "Modified MLCAPE / MLCIN",
                f"[{mcape_color}]{modified_indices.MLCAPE:.0f}[/{mcape_color}]"
                f" / {modified_indices.MLCIN:.0f} J/kg"
                f"  [dim](12Z sounding aloft + current surface Td)[/dim]",
            )
        console.print(mr_tbl)

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

    if _api_state and _api_state.get("hrrr_counties") and _api_state.get("hrrr_valid"):
        # Reconstruct HRRRCountySnapshot from cached API data — skips S3/Herbie fetch.
        from ok_weather_model.models.hrrr import HRRRCountyPoint as _HCP, HRRRCountySnapshot as _HCS
        try:
            _pts = [_HCP(**_p) for _p in _api_state["hrrr_counties"]]
            _hrrr_valid = datetime.fromisoformat(_api_state["hrrr_valid"].replace("Z", "+00:00"))
            hrrr_snap = _HCS(run_time=_hrrr_valid, valid_time=_hrrr_valid, counties=_pts)
            risk_source = f"HRRR 3km  {_hrrr_valid.strftime('%H:%MZ')}  [dim](cached)[/dim]"
        except Exception as _ce:
            logger.debug("Cache HRRR reconstruction failed: %s", _ce)

    if hrrr_snap is None:
        with console.status("Fetching HRRR snapshots (baseline + current)..."):
            try:
                with HRRRClient() as hc:
                    _base_vt = datetime(today.year, today.month, today.day,
                                        fetched_hour, 0, tzinfo=timezone.utc)
                    _hrrr_baseline = hc.get_county_snapshot(_base_vt)
                    if _hrrr_baseline is not None:
                        _hrrr_baseline_valid = _base_vt

                    for _h_back in range(4):
                        _try_hour = (_hrrr_hour - _h_back) % 24
                        _try_date = today if _hrrr_hour >= _h_back else (
                            today - timedelta(days=1)
                        )
                        _vt = datetime(_try_date.year, _try_date.month, _try_date.day,
                                       _try_hour, 0, tzinfo=timezone.utc)
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
            fwd_indices=fwd_indices,
            fwd_kinematics=fwd_kinematics,
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

    # ── Model predictions ─────────────────────────────────────────────────────
    console.rule("[bold]Forecast Model Predictions[/bold]")
    try:
        from ok_weather_model.modeling import SeverityClassifier, TornadoRegressor, load_model
        _clf: SeverityClassifier = load_model("severity_classifier")
        _reg: TornadoRegressor   = load_model("tornado_regressor")

        if _clf is not None and _reg is not None:
            _ctg = ces.get("convective_temp_gap_12Z") if ces is not None else None
            _mr_kwargs = {}
            if moisture_return is not None:
                _mr_kwargs = {
                    "surface_dewpoint_f":           moisture_return.state_mean_dewpoint_f,
                    "moisture_return_gradient_f":   moisture_return.moisture_return_gradient_f,
                    "gulf_moisture_fraction":       moisture_return.gulf_moisture_fraction,
                }
            if modified_indices is not None:
                _mr_kwargs["modified_MLCAPE"] = modified_indices.MLCAPE
                _mr_kwargs["modified_MLCIN"]  = modified_indices.MLCIN

            _clf_result = _clf.predict_proba(indices, kinematics, _ctg, **_mr_kwargs)
            _reg_result = _reg.predict(indices, kinematics, _ctg, **_mr_kwargs)

            sig_pct = _clf_result["significant"]
            sig_color = "red" if sig_pct >= 0.6 else "yellow" if sig_pct >= 0.35 else "green"

            model_table = Table(show_header=True, header_style="bold", box=None)
            model_table.add_column("Model")
            model_table.add_column("Prediction", justify="right")
            model_table.add_column("Detail")
            model_table.add_row(
                "Severity",
                f"[{sig_color}]{sig_pct:.0%} SIGNIFICANT[/{sig_color}]",
                f"WEAK {_clf_result['weak']:.0%}  (n={_clf.n_training_cases_} training cases)",
            )
            model_table.add_row(
                "Tornado count",
                f"{_reg_result['expected_count']:.1f}",
                f"80% PI: {_reg_result['interval_low']:.0f}–{_reg_result['interval_high']:.0f}",
            )
            console.print(model_table)
        else:
            console.print("[dim]No trained models found. Run [cyan]train-models[/cyan] to enable predictions.[/dim]")
    except Exception as _model_exc:
        logger.debug("Model prediction skipped: %s", _model_exc)

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


# ── dashboard ────────────────────────────────────────────────────────────────

@cli.command("dashboard")
@click.option("--station", default="OUN", show_default=True,
              help="Primary sounding station (OUN, LMN, AMA, DDC)")
def dashboard(station: str):
    """
    Launch the real-time TUI dashboard.

    Four panes update asynchronously on independent timers:
      Risk Zones   — HRRR 3km county tiers          (every 15 min)
      Environment  — OUN + LMN sounding + CES        (every 60 min)
      Dryline      — Mesonet surface network          (every 5 min)
      Tendency     — HRRR baseline vs current deltas  (every 15 min)

    Alert log at the bottom scrolls all tier changes and dryline events.
    Press R to force-refresh all data, Q to quit.

    Requires: pip install textual
    """
    try:
        from kronos_dash import KronosDashboard
    except ImportError:
        console.print("[red]textual is required: pip install textual[/red]")
        return
    KronosDashboard(station=station).run()


# ── watch-now ─────────────────────────────────────────────────────────────────

@cli.command("watch-now")
@click.option("--interval", default=15, show_default=True,
              help="Poll interval in minutes")
@click.option("--min-tier", default="MODERATE",
              type=click.Choice(["MARGINAL", "MODERATE", "DANGEROUS_CAPPED", "HIGH", "EXTREME"]),
              show_default=True,
              help="Minimum tier change that triggers an alert")
@click.option("--station", default="OUN", show_default=True,
              help="Primary sounding station")
@click.option("--notify", is_flag=True, default=False,
              help="Send macOS system notifications on EXTREME tier, new Tornado Warnings, "
                   "and PDS Watch issuances (macOS only; uses osascript).")
def watch_now(interval: int, min_tier: str, station: str, notify: bool):
    """
    Continuously monitor conditions and alert on meaningful changes.

    Polls HRRR every INTERVAL minutes.  Only prints output when:
      - A county's risk tier changes (upgrade or downgrade)
      - The dominant tendency flips direction (favorable ↔ unfavorable)
      - A new HRRR run becomes available

    Designed to run in a terminal during an active convective day.
    Press Ctrl-C to stop.
    """
    import time
    from rich.panel import Panel
    from ok_weather_model.ingestion import HRRRClient
    from ok_weather_model.processing.risk_zone import (
        compute_risk_zones_from_hrrr,
        _TIER_RANK as _TR,
        _TIER_COLOR as _TC,
    )
    from ok_weather_model.models import OklahomaSoundingStation
    from ok_weather_model.ingestion import SoundingClient
    from ok_weather_model.processing import compute_thermodynamic_indices, compute_kinematic_profile

    try:
        stn = OklahomaSoundingStation[station.upper()]
    except KeyError:
        console.print(f"[red]Unknown station '{station}'.[/red]")
        return

    min_rank = _TR.get(min_tier, 2)

    # ── State tracking ────────────────────────────────────────────────────────
    # prev_county_tiers: county → tier string from last cycle
    # prev_hrrr_valid:   datetime of last seen HRRR run
    # prev_trend:        "favorable" | "unfavorable" | "steady"
    prev_county_tiers: dict = {}
    prev_hrrr_valid:   datetime | None = None
    prev_trend: str = "steady"
    prev_dryline_lon:  float | None = None   # last known dryline center longitude
    prev_alert_keys:   set[str] = set()      # seen NWS alert headlines
    prev_md_numbers:   set[int] = set()      # seen SPC MD numbers
    cycle = 0

    def _macos_notify(title: str, message: str) -> None:
        """Send a macOS system notification via osascript. No-op on other platforms."""
        if not notify:
            return
        import subprocess, sys
        if sys.platform != "darwin":
            return
        try:
            script = (
                f'display notification "{message}" '
                f'with title "{title}" '
                f'sound name "Sosumi"'
            )
            subprocess.run(["osascript", "-e", script],
                           capture_output=True, timeout=5)
        except Exception:
            pass

    def _classify_trend(tend_counties) -> str:
        if not tend_counties:
            return "steady"
        inc = sum(
            1 for _, now, base in tend_counties
            if (now.MLCIN - base.MLCIN) < -10 or
               (now.SRH_0_1km - base.SRH_0_1km) > 20
        )
        dec = sum(
            1 for _, now, base in tend_counties
            if (now.MLCIN - base.MLCIN) > 10 or
               (now.SRH_0_1km - base.SRH_0_1km) < -20
        )
        total = len(tend_counties)
        if inc / total >= 0.5:   return "favorable"
        if dec / total >= 0.5:   return "unfavorable"
        return "steady"

    def _fetch_cycle() -> tuple:
        """
        Returns (hrrr_snap, hrrr_valid, baseline_snap, baseline_valid,
                 risk_zones, baseline_risk_zones, tend_counties, sounding_hour,
                 current_dryline, prev_dryline, surface_temp_c)
        or None on hard failure.
        """
        from ok_weather_model.ingestion import MesonetClient
        from ok_weather_model.models import OklahomaCounty
        from ok_weather_model.models.mesonet import MesonetTimeSeries as _MTS
        from ok_weather_model.processing import detect_dryline, compute_dryline_surge_rate

        now_utc = datetime.now(tz=timezone.utc)
        today   = now_utc.date()

        # Latest sounding
        standard_hours = (21, 18, 15, 12, 9, 6, 3, 0)
        hours_to_try   = [h for h in standard_hours if h <= now_utc.hour] or [0]
        profile = fetched_hour = None
        with SoundingClient() as sc:
            for h in hours_to_try:
                p = sc.get_sounding(stn, today, h)
                if p is not None:
                    profile      = p
                    fetched_hour = h
                    break
        if profile is None:
            return None

        # Mesonet: current snapshot + one from ~1 hr ago for dryline surge rate
        # and surface temp (CES Tc-gap update). Each fetch tries up to 4 earlier
        # 5-min boundaries to handle archive lag.
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
                    break  # got this hour's snapshot

        # Dryline detection
        current_dryline = prev_dryline = None
        if len(snap_times) >= 1:
            current_dryline = detect_dryline(station_series, snap_times[0])
        if len(snap_times) >= 2:
            prev_dryline = detect_dryline(station_series, snap_times[-1])

        # Surface temp for CES (nearest Cleveland county obs to now)
        surface_temp_c = profile.levels[0].temperature if profile.levels else None
        if snap_times:
            current_snap_t = snap_times[0]
            for ts in station_series.values():
                if ts.county == OklahomaCounty.CLEVELAND:
                    for o in ts.observations:
                        if abs((o.valid_time - current_snap_t).total_seconds()) <= 420:
                            surface_temp_c = (o.temperature - 32) * 5 / 9
                            break
                    break

        # HRRR: baseline at sounding hour + current most-recent
        base_vt = datetime(today.year, today.month, today.day,
                           fetched_hour, 0, tzinfo=timezone.utc)
        hrrr_baseline = curr_snap = curr_valid = None
        base_valid = None
        with HRRRClient() as hc:
            hrrr_baseline = hc.get_county_snapshot(base_vt)
            base_valid    = base_vt if hrrr_baseline else None

            for hb in range(4):
                try_h    = (now_utc.hour - hb) % 24
                try_date = today if now_utc.hour >= hb else today - timedelta(days=1)
                vt = datetime(try_date.year, try_date.month, try_date.day,
                              try_h, 0, tzinfo=timezone.utc)
                if vt == base_vt:
                    curr_snap  = hrrr_baseline
                    curr_valid = vt
                    break
                snap = hc.get_county_snapshot(vt)
                if snap is not None:
                    curr_snap  = snap
                    curr_valid = vt
                    break

        if curr_snap is None:
            return None

        # Pass dryline to risk zone scoring so counties near the dryline
        # get their tier boosted appropriately
        risk_zones = compute_risk_zones_from_hrrr(
            curr_snap, dryline=current_dryline, min_tier="MARGINAL"
        )
        base_risk_zones = compute_risk_zones_from_hrrr(
            hrrr_baseline, dryline=current_dryline, min_tier="MARGINAL"
        ) if hrrr_baseline and hrrr_baseline is not curr_snap else []

        # Tendency counties: use elevated zones from current or baseline
        _elevated_now  = any(z.tier_rank >= 3 for z in risk_zones)
        _elevated_base = any(z.tier_rank >= 3 for z in base_risk_zones)
        ref_zones = risk_zones if _elevated_now else base_risk_zones
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
                pt_base = hrrr_baseline.get(county) if hrrr_baseline else None
                if pt_now and pt_base and curr_valid != base_valid:
                    tend_counties.append((zone.tier, pt_now, pt_base))

        # ── SPC / NWS active products ─────────────────────────────────────────
        from ok_weather_model.ingestion import fetch_active_watches_warnings
        from ok_weather_model.ingestion.spc_products import fetch_active_mds
        nws_alerts = []
        spc_mds    = []
        try:
            nws_alerts = fetch_active_watches_warnings()
        except Exception:
            pass
        try:
            spc_mds = fetch_active_mds()
        except Exception:
            pass

        return (curr_snap, curr_valid, hrrr_baseline, base_valid,
                risk_zones, base_risk_zones, tend_counties, fetched_hour,
                current_dryline, prev_dryline, surface_temp_c,
                nws_alerts, spc_mds)

    # ── Main watch loop ───────────────────────────────────────────────────────
    from rich.table import Table as _RichTable
    from rich.panel import Panel as _Panel

    console.rule(
        f"[bold cyan]KRONOS-WX WATCH  —  {station}  "
        f"interval={interval}min  alert≥{min_tier}[/bold cyan]"
    )
    console.print(f"[dim]Started {datetime.now(tz=timezone.utc).strftime('%Y-%m-%d %H:%MZ')}  "
                  f"Press Ctrl-C to stop.[/dim]")

    # ── Tier legend ───────────────────────────────────────────────────────────
    legend = _RichTable(box=None, padding=(0, 2, 0, 0), show_header=False)
    legend.add_column(style="bold", min_width=20)
    legend.add_column()
    legend.add_row("[bright_red]EXTREME[/bright_red]",
                   "Near-zero cap (CIN<50), high instability (CAPE≥1500), extreme kinematics "
                   "(SRH1≥250, EHI≥3.5). Any storm that fires produces significant tornadoes.")
    legend.add_row("[red]HIGH[/red]",
                   "Manageable cap (CIN<100), strong instability (CAPE≥1000), strong kinematics "
                   "(SRH1≥150, EHI≥2.0). Likely significant tornadoes with initiation.")
    legend.add_row("[magenta]DANGEROUS_CAPPED[/magenta]",
                   "Strong cap (CIN≥80) but violent kinematics (SRH1≥150, EHI≥2.0). "
                   "Boundary-forced initiation — dryline surge, outflow, differential heating — "
                   "can produce violent tornadoes with minimal warning. [italic](Apr 23 2026 pattern)[/italic]")
    legend.add_row("[yellow]MODERATE[/yellow]",
                   "Some tornado potential if cap erodes (CIN<200, CAPE≥500, SRH1≥100). "
                   "Kinematics support organized convection but environment is limited.")
    legend.add_row("[green]MARGINAL[/green]",
                   "Isolated storm potential (CIN<250, CAPE≥200, SRH1≥50). "
                   "Weakly forced environment; storms possible but not likely to be significant.")
    console.print(_Panel(legend, title="[bold]Risk Tier Reference[/bold]",
                         border_style="dim", padding=(0, 1)))
    console.print(f"[dim]Alerts fire on tier changes ≥{min_tier}, trend flips, "
                  f"and dryline events. Quiet cycles show a one-line status.[/dim]\n")

    try:
        while True:
            cycle += 1
            now_str = datetime.now(tz=timezone.utc).strftime("%H:%MZ")

            with console.status(f"[dim]Cycle {cycle}  {now_str}  fetching...[/dim]"):
                result = _fetch_cycle()

            if result is None:
                console.print(f"[dim]{now_str}  Cycle {cycle}: no data available, retrying in {interval}min[/dim]")
                time.sleep(interval * 60)
                continue

            (curr_snap, curr_valid, base_snap, base_valid,
             risk_zones, base_zones, tend_counties, snd_hour,
             current_dryline, prev_dryline, surface_temp_c,
             nws_alerts, spc_mds) = result

            curr_valid_str = curr_valid.strftime("%H:%MZ")

            # ── Detect changes ─────────────────────────────────────────────────
            alerts: list[str] = []
            tier_changes: list[tuple] = []  # (county, old_tier, new_tier)

            # Build current tier map
            curr_county_tiers: dict = {}
            for zone in risk_zones:
                for county in zone.counties:
                    curr_county_tiers[county] = zone.tier

            # New HRRR run
            new_hrrr = (prev_hrrr_valid is None or curr_valid != prev_hrrr_valid)
            if new_hrrr and prev_hrrr_valid is not None:
                alerts.append(f"New HRRR run: {curr_valid_str}")

            # Dryline: appeared, disappeared, or surged significantly
            from ok_weather_model.processing import compute_dryline_surge_rate
            curr_dl_lon = None
            if current_dryline and current_dryline.position_lon:
                curr_dl_lon = sum(current_dryline.position_lon) / len(current_dryline.position_lon)

            if curr_dl_lon is not None and prev_dryline_lon is None:
                alerts.append(
                    f"Dryline appeared — center {abs(curr_dl_lon):.1f}°W  "
                    f"confidence {current_dryline.confidence:.2f}"
                )
            elif curr_dl_lon is None and prev_dryline_lon is not None:
                alerts.append("Dryline no longer detected")
            elif curr_dl_lon is not None and prev_dryline_lon is not None:
                surge = compute_dryline_surge_rate(prev_dryline, current_dryline) \
                        if prev_dryline and current_dryline else None
                if surge is not None and abs(surge) >= 10:
                    direction = "eastward" if surge > 0 else "retrograding"
                    surge_color = "red" if surge > 15 else "yellow"
                    alerts.append(
                        f"[{surge_color}]Dryline surging {direction}: "
                        f"{surge:+.0f} mph  now {abs(curr_dl_lon):.1f}°W[/{surge_color}]"
                    )

            # Tier changes for counties at or above min_tier (either direction)
            for county, new_tier in curr_county_tiers.items():
                old_tier = prev_county_tiers.get(county, "LOW")
                if old_tier == new_tier:
                    continue
                old_rank = _TR.get(old_tier, 0)
                new_rank = _TR.get(new_tier, 0)
                if max(old_rank, new_rank) >= min_rank:
                    tier_changes.append((county, old_tier, new_tier))

            # Counties that dropped out of elevated tiers
            for county, old_tier in prev_county_tiers.items():
                if county not in curr_county_tiers:
                    old_rank = _TR.get(old_tier, 0)
                    if old_rank >= min_rank:
                        tier_changes.append((county, old_tier, "LOW"))

            # Trend flip
            curr_trend = _classify_trend(tend_counties)
            trend_flip = (
                prev_trend != curr_trend and
                prev_trend != "steady" and
                any(z.tier_rank >= 3 for z in (risk_zones + base_zones))
            )
            if trend_flip:
                alerts.append(
                    f"Trend flip: {prev_trend.upper()} → {curr_trend.upper()}"
                )

            # ── NWS / SPC alert change detection ──────────────────────────────
            curr_alert_keys: set[str] = {a.headline for a in nws_alerts}
            curr_md_numbers: set[int] = {md.number for md in spc_mds}

            new_alerts  = [a for a in nws_alerts if a.headline not in prev_alert_keys]
            new_mds     = [md for md in spc_mds  if md.number  not in prev_md_numbers]
            gone_alerts = prev_alert_keys - curr_alert_keys   # expired products

            # ── Print output only when something changed ───────────────────────
            has_elevated = any(z.tier_rank >= min_rank for z in risk_zones)
            should_print = (
                bool(tier_changes) or trend_flip or bool(new_alerts) or
                bool(new_mds) or cycle == 1
            )

            if should_print:
                console.rule(f"[bold]{curr_valid_str}  Cycle {cycle}[/bold]")

                # Tier change summary
                if tier_changes:
                    upgrades   = [(c, o, n) for c, o, n in tier_changes
                                  if _TR.get(n, 0) > _TR.get(o, 0)]
                    downgrades = [(c, o, n) for c, o, n in tier_changes
                                  if _TR.get(n, 0) < _TR.get(o, 0)]

                    if upgrades:
                        lines = []
                        for county, old, new in upgrades:
                            oc = _TC.get(old, "white")
                            nc = _TC.get(new, "white")
                            lines.append(
                                f"  {county.name}: "
                                f"[{oc}]{old}[/{oc}] → [{nc}]{new}[/{nc}]"
                            )
                        console.print(Panel(
                            "\n".join(lines),
                            title="[bold red on white] ▲  TIER UPGRADES [/bold red on white]",
                            border_style="red",
                        ))

                    if downgrades:
                        lines = []
                        for county, old, new in downgrades:
                            oc = _TC.get(old, "white")
                            nc = _TC.get(new, "white")
                            lines.append(
                                f"  {county.name}: "
                                f"[{oc}]{old}[/{oc}] → [{nc}]{new}[/{nc}]"
                            )
                        console.print(Panel(
                            "\n".join(lines),
                            title="[bold yellow on black] ▼  TIER DOWNGRADES [/bold yellow on black]",
                            border_style="yellow",
                        ))

                # Trend flip
                if trend_flip:
                    trend_color = "bright_green" if curr_trend == "favorable" else (
                        "red" if curr_trend == "unfavorable" else "yellow"
                    )
                    console.print(Panel(
                        f"[{trend_color}]{prev_trend.upper()} → {curr_trend.upper()}[/{trend_color}]",
                        title="[bold] Trend Flip [/bold]",
                        border_style=trend_color,
                    ))

                # ── NWS active alerts ──────────────────────────────────────────
                if new_alerts:
                    lines = []
                    for a in sorted(new_alerts, key=lambda x: -x.priority):
                        expires = f"  expires {a.expires_label}" if a.expires_label else ""
                        if a.priority == 4:   # Tornado Warning
                            color = "bright_red"
                        elif a.priority == 3: # Tornado Watch
                            color = "red"
                        else:                 # SVR Warning
                            color = "yellow"
                        lines.append(f"[{color}]{a.event}[/{color}]  {a.headline}{expires}")
                        lines.append(f"  [dim]{a.area_desc[:120]}[/dim]")
                        # Notify for Tornado Warning or Tornado Watch
                        if a.priority >= 3:
                            _macos_notify(
                                f"KRONOS-WX: {a.event}",
                                a.headline[:80],
                            )
                    console.print(Panel(
                        "\n".join(lines),
                        title="[bold red on white] ⚡  NEW NWS ALERTS [/bold red on white]",
                        border_style="bright_red",
                        padding=(1, 2),
                    ))

                # Expired alerts
                if gone_alerts and cycle > 1:
                    for headline in gone_alerts:
                        console.print(f"[dim]{now_str}  Alert expired/cancelled: {headline[:80]}[/dim]")

                # All currently active alerts (compact, every print cycle)
                if nws_alerts and (new_alerts or cycle == 1):
                    al_tbl = Table(title="Active NWS Alerts — Oklahoma",
                                   show_lines=False, box=None, padding=(0, 2))
                    al_tbl.add_column("Product",  style="bold", min_width=28)
                    al_tbl.add_column("Expires",  justify="right", min_width=8)
                    al_tbl.add_column("Area (truncated)")
                    for a in sorted(nws_alerts, key=lambda x: -x.priority):
                        if a.priority == 4:   color = "bright_red"
                        elif a.priority == 3: color = "red"
                        else:                 color = "yellow"
                        al_tbl.add_row(
                            f"[{color}]{a.event}[/{color}]",
                            a.expires_label,
                            a.area_desc[:70],
                        )
                    console.print(al_tbl)

                # ── SPC Mesoscale Discussions ──────────────────────────────────
                if new_mds:
                    for md in new_mds:
                        ok_tag = " [OK]" if md.mentions_oklahoma else ""
                        body_preview = "  ".join(md.body_lines[:3]) if md.body_lines else ""
                        console.print(Panel(
                            f"[bold]MD #{md.number}{ok_tag}[/bold]  {md.areas_affected}\n"
                            f"[italic]{md.concerning}[/italic]\n\n"
                            f"{body_preview}\n\n"
                            f"[dim]{md.url}[/dim]",
                            title="[bold yellow on black] 🔔  NEW SPC MESOSCALE DISCUSSION [/bold yellow on black]",
                            border_style="yellow",
                            padding=(1, 2),
                        ))
                        if md.mentions_oklahoma:
                            _macos_notify(
                                f"KRONOS-WX: SPC MD #{md.number}",
                                f"{md.concerning[:80]}",
                            )

                # Other alerts (new HRRR run, dryline)
                for a in alerts:
                    console.print(f"[dim]{now_str}  {a}[/dim]")

                # EXTREME tier notification
                extreme_counties = [
                    z for z in risk_zones if z.tier == "EXTREME"
                ]
                if extreme_counties and _TR.get("EXTREME", 5) >= min_rank:
                    names = ", ".join(
                        c.name for z in extreme_counties for c in z.counties[:4]
                    )
                    _macos_notify(
                        "KRONOS-WX: EXTREME TIER",
                        f"{names} — violent tornado environment",
                    )

                # Current risk snapshot (elevated counties only)
                if has_elevated:
                    snap_tbl = Table(show_lines=True, title=f"Risk Snapshot  {curr_valid_str}")
                    snap_tbl.add_column("County",   style="cyan")
                    snap_tbl.add_column("Tier")
                    snap_tbl.add_column("MLCAPE",   justify="right")
                    snap_tbl.add_column("MLCIN",    justify="right")
                    snap_tbl.add_column("SRH 0–1",  justify="right")
                    snap_tbl.add_column("SRH 0–3",  justify="right")
                    snap_tbl.add_column("Shear",    justify="right")
                    snap_tbl.add_column("EHI",      justify="right")
                    snap_tbl.add_column("STP",      justify="right")

                    for zone in risk_zones:
                        if zone.tier_rank < min_rank:
                            continue
                        for county in zone.counties:
                            pt = curr_snap.get(county)
                            if pt is None:
                                continue
                            color = _TC.get(zone.tier, "white")
                            snap_tbl.add_row(
                                county.name,
                                f"[{color}]{zone.tier}[/{color}]",
                                f"{pt.MLCAPE:.0f}",
                                f"{pt.MLCIN:.0f}",
                                f"{pt.SRH_0_1km:.0f}",
                                f"{pt.SRH_0_3km:.0f}",
                                f"{pt.BWD_0_6km:.0f} kt",
                                f"{pt.EHI:.2f}" if pt.EHI else "—",
                                f"{pt.STP:.2f}" if pt.STP else "—",
                            )
                    console.print(snap_tbl)

                elif cycle == 1:
                    console.print(
                        f"[dim]No counties at or above {min_tier}. "
                        f"Monitoring…[/dim]"
                    )

            else:
                # Quiet cycle — print a single status line so the user knows
                # it's still running
                top_tier = risk_zones[0].tier if risk_zones else "LOW"
                top_color = _TC.get(top_tier, "dim")
                console.print(
                    f"[dim]{now_str}  No change  "
                    f"top tier: [{top_color}]{top_tier}[/{top_color}][/dim]"
                )

            # Update state
            prev_county_tiers = curr_county_tiers
            prev_hrrr_valid   = curr_valid
            prev_trend        = curr_trend
            prev_dryline_lon  = curr_dl_lon
            prev_alert_keys   = curr_alert_keys
            prev_md_numbers   = curr_md_numbers

            time.sleep(interval * 60)

    except KeyboardInterrupt:
        console.print("\n[dim]Watch stopped.[/dim]")


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
            # Try the case's own sounding station first (may be LMN for some cases),
            # then fall back to OUN — avoids skipping cases where OUN Parquet is
            # missing but an adjacent-station sounding was stored instead.
            preferred_station = (
                case.sounding_12Z.station if case.sounding_12Z is not None
                else OklahomaSoundingStation.OUN
            )
            sounding = db.load_sounding(preferred_station, valid_time)
            if sounding is None or not sounding.levels:
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
        "BOUNDARY_FORCED": "Boundary-forced erosion (ERA5 dynamic forcing or manual)",
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


# ── ingest-null-busts ────────────────────────────────────────────────────────

@cli.command("ingest-null-busts")
@click.option("--start-year", default=2009, show_default=True,
              help="First year to scan (KMZ archive confirmed from 2009)")
@click.option("--end-year", default=CASE_LIBRARY_END_YEAR, show_default=True,
              help="Last year to scan")
@click.option("--spc-threshold", default=0.10, show_default=True, type=float,
              help="Min SPC Day 1 tornado probability over OK to qualify as a bust (0–1)")
@click.option("--dry-run", is_flag=True, default=False,
              help="Show candidates but do not write to the database")
def ingest_null_busts(start_year: int, end_year: int, spc_threshold: float, dry_run: bool):
    """
    Scan SPC Day 1 KMZ outlook archive for bust days and add them to the case library.

    A bust day is defined as: SPC Day 1 tornado probability ≥ SPC_THRESHOLD over
    Oklahoma AND zero Oklahoma tornado reports in the SPC tornado database.

    KMZ files are cached locally in data/spc_outlooks/YYYY.json so subsequent
    runs only fetch files not yet cached.  Archive coverage: 2009-01-01 onward
    (KMZ format not available before 2009).

    After this command, run enrich-all to attach soundings and HRRR data to the
    new NULL_BUST skeletons before training models.
    """
    from ok_weather_model.ingestion.spc_client import SPCClient
    from ok_weather_model.storage.database import Database

    db = Database()
    start = date(start_year, 1, 1)
    end = date(end_year, 12, 31)

    # Load existing case dates to avoid duplicates
    existing = db.query_parameter_space({
        "start_date": str(start),
        "end_date": str(end),
    })
    existing_dates: set[date] = {c.date for c in existing}
    console.print(
        f"Scanning {start_year}–{end_year} for NULL_BUST candidates "
        f"(SPC torn prob ≥ {spc_threshold:.0%}, 0 OK tornadoes)…"
    )
    console.print(f"[dim]{len(existing_dates)} existing cases in date range (skipped)[/dim]")

    with SPCClient() as client:
        with console.status("Fetching SPC Day 1 outlooks (cached)…"):
            candidates = client.build_null_bust_skeletons(
                start, end,
                spc_threshold=spc_threshold,
                existing_dates=existing_dates,
            )

    if not candidates:
        console.print("[yellow]No new NULL_BUST candidates found.[/yellow]")
        return

    # Display table of candidates
    tbl = Table(title=f"NULL_BUST Candidates ({len(candidates)})", show_lines=True)
    tbl.add_column("Date")
    tbl.add_column("SPC Torn Prob", justify="right")
    tbl.add_column("Status")
    for c in sorted(candidates, key=lambda x: x.date):
        prob_str = f"{c.SPC_max_tornado_prob:.0%}" if c.SPC_max_tornado_prob else "?"
        status = "[dim]dry-run[/dim]" if dry_run else "[green]to save[/green]"
        tbl.add_row(c.date.isoformat(), prob_str, status)
    console.print(tbl)

    if dry_run:
        console.print(f"\n[yellow]Dry-run: {len(candidates)} candidates found, none saved.[/yellow]")
        return

    saved = 0
    for c in candidates:
        try:
            db.save_case(c)
            saved += 1
        except Exception as exc:
            console.print(f"[red]Failed to save {c.case_id}: {exc}[/red]")

    console.print(
        f"\n[bold green]Saved {saved} NULL_BUST skeletons.[/bold green] "
        f"Run [cyan]enrich-all {start_year} {end_year}[/cyan] to attach sounding data."
    )


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


# ── classify-boundary-forced ─────────────────────────────────────────────────

@cli.command("classify-boundary-forced")
@click.option("--start-year", default=CASE_LIBRARY_START_YEAR, show_default=True,
              help="First year to process")
@click.option("--end-year", default=CASE_LIBRARY_END_YEAR, show_default=True,
              help="Last year to process")
@click.option("--forcing-threshold", default=-15.0, show_default=True, type=float,
              help="dynamic_cap_forcing_jkg_hr cutoff; values ≤ this trigger reclassification")
@click.option("--dry-run", is_flag=True, default=False,
              help="Report candidates but do not write to the database")
@click.option("--force", is_flag=True, default=False,
              help="Re-evaluate even if cap_behavior is already BOUNDARY_FORCED")
def classify_boundary_forced(
    start_year: int,
    end_year: int,
    forcing_threshold: float,
    dry_run: bool,
    force: bool,
):
    """
    Reclassify NO_EROSION tornado cases to BOUNDARY_FORCED using ERA5 synoptic forcing.

    Targets cases where the CES sounding model predicted the cap would hold all
    day (NO_EROSION) but tornadoes actually occurred — the El Reno 2013 / April 23
    2026 pattern.  For each candidate, ERA5 12Z synoptic forcing is computed; if
    dynamic_cap_forcing_jkg_hr ≤ FORCING_THRESHOLD the cap erosion is attributed
    to dynamic boundary forcing rather than surface heating alone.

    Requires ERA5 access (CDS API key in ~/.cdsapirc).

    Run after compute-ces to ensure cap_behavior is populated.
    """
    from ok_weather_model.storage import Database
    from ok_weather_model.ingestion.era5_client import ERA5Client, PRESSURE_LEVELS, UPPER_AIR_VARS
    from ok_weather_model.processing.era5_diagnostics import compute_synoptic_cap_forcing
    from ok_weather_model.models.enums import ErosionMechanism

    # EventClasses that represent actual tornado activity — candidates for
    # BOUNDARY_FORCED.  NULL_BUST (no tornadoes) and SIGNIFICANT_SEVERE_NO_TORNADO
    # (no tornado, just hail/wind) are excluded.
    _TORNADO_CLASSES = {
        EventClass.SIGNIFICANT_OUTBREAK,
        EventClass.ISOLATED_SIGNIFICANT,
        EventClass.WEAK_OUTBREAK,
        EventClass.SURPRISING_OUTBREAK,
    }

    db = Database()
    all_cases = db.query_parameter_space({
        "start_date": str(date(start_year, 1, 1)),
        "end_date": str(date(end_year, 12, 31)),
    })

    # Candidates: NO_EROSION cap behavior + actual tornado activity
    candidates = [
        c for c in all_cases
        if c.cap_behavior == CapBehavior.NO_EROSION
        and c.event_class in _TORNADO_CLASSES
    ]

    if force:
        # Also re-evaluate existing BOUNDARY_FORCED to confirm/update
        already_bf = [
            c for c in all_cases
            if c.cap_behavior == CapBehavior.BOUNDARY_FORCED
            and c.event_class in _TORNADO_CLASSES
        ]
        candidates = candidates + already_bf

    console.rule("[bold cyan]BOUNDARY_FORCED Classification via ERA5[/bold cyan]")
    console.print(
        f"Year range: [cyan]{start_year}–{end_year}[/cyan]  "
        f"Candidates: [green]{len(candidates)}[/green]  "
        f"Forcing threshold: [yellow]{forcing_threshold:.1f} J/kg/hr[/yellow]"
    )
    if dry_run:
        console.print("[yellow]DRY RUN — no changes will be saved[/yellow]")

    reclassified = 0
    skipped_no_era5 = 0
    skipped_weak_forcing = 0

    results_table = Table(
        title="BOUNDARY_FORCED Candidates",
        show_lines=True,
        expand=True,
    )
    results_table.add_column("Case ID", style="cyan", no_wrap=True)
    results_table.add_column("Event Class", style="green")
    results_table.add_column("dyn. forcing\nJ/kg/hr", justify="right")
    results_table.add_column("T-adv 700mb\nK/hr", justify="right")
    results_table.add_column("omega 700mb\nPa/s", justify="right")
    results_table.add_column("Result")

    era5 = ERA5Client()

    for case in track(candidates, description="Fetching ERA5..."):
        try:
            ds = era5.get_upper_air_fields(
                case.date,
                pressure_levels=PRESSURE_LEVELS,
                variables=UPPER_AIR_VARS,
                hours=[12],
            )
        except Exception as exc:
            logger.warning("ERA5 unavailable for %s: %s", case.case_id, exc)
            skipped_no_era5 += 1
            results_table.add_row(
                case.case_id,
                case.event_class.value,
                "—", "—", "—",
                "[dim]ERA5 unavailable[/dim]",
            )
            continue

        valid_time = datetime(
            case.date.year, case.date.month, case.date.day, 12, 0,
            tzinfo=timezone.utc,
        )

        try:
            forcing = compute_synoptic_cap_forcing(ds, valid_time)
        except Exception as exc:
            logger.warning("Forcing computation failed for %s: %s", case.case_id, exc)
            skipped_no_era5 += 1
            continue

        dyn  = forcing["dynamic_cap_forcing_jkg_hr"]
        adv7 = forcing["thermal_advection_700mb"]
        om7  = forcing["vertical_motion_700mb"]

        if dyn <= forcing_threshold:
            # Strong enough synoptic support → BOUNDARY_FORCED
            result_label = "[bright_cyan]RECLASSIFIED[/bright_cyan]"
            if not dry_run:
                case.cap_behavior = CapBehavior.BOUNDARY_FORCED
                case.cap_erosion_mechanism = ErosionMechanism.DYNAMIC
                case.recompute_completeness()
                db.save_case(case)
            reclassified += 1
        else:
            result_label = "[dim]forcing too weak[/dim]"
            skipped_weak_forcing += 1

        results_table.add_row(
            case.case_id,
            case.event_class.value,
            f"{dyn:+.1f}",
            f"{adv7:+.4f}",
            f"{om7:+.4f}",
            result_label,
        )

    era5.close()

    console.print(results_table)
    console.print(
        f"\n[bold green]Reclassified: {reclassified}[/bold green]  "
        f"Forcing too weak: {skipped_weak_forcing}  "
        f"ERA5 unavailable: {skipped_no_era5}"
    )
    if dry_run and reclassified > 0:
        console.print(
            f"[yellow]DRY RUN: {reclassified} case(s) would be reclassified. "
            f"Re-run without --dry-run to save.[/yellow]"
        )


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


# ── train-models ─────────────────────────────────────────────────────────────

@cli.command("train-models")
@click.option("--start-year", default=CASE_LIBRARY_START_YEAR, show_default=True,
              help="First year of training data")
@click.option("--end-year", default=CASE_LIBRARY_END_YEAR, show_default=True,
              help="Last year of training data")
def train_models(start_year: int, end_year: int):
    """
    Train the severity classifier and tornado count regressor on the case library.

    Saves trained model artifacts to data/models/ for use by analyze-now and
    predict-day.  Prints training metrics (optimistic — use evaluate-models for
    honest leave-one-year-out performance).
    """
    from ok_weather_model.storage.database import Database
    from ok_weather_model.modeling import SeverityClassifier, TornadoRegressor, save_model

    db = Database()
    cases = db.query_parameter_space({
        "start_date": str(date(start_year, 1, 1)),
        "end_date": str(date(end_year, 12, 31)),
        "min_completeness": 0.3,
    })
    console.print(f"Loaded [cyan]{len(cases)}[/cyan] cases ({start_year}–{end_year}).")

    # ── Severity classifier ───────────────────────────────────────────────────
    console.print("\n[bold]Training severity classifier (SIGNIFICANT vs WEAK)[/bold]")
    clf = SeverityClassifier()
    clf_metrics = clf.train(cases)
    save_model("severity_classifier", clf)

    clf_table = Table(show_header=False, box=None)
    clf_table.add_column(style="dim")
    clf_table.add_column()
    clf_table.add_row("Cases trained",       str(clf_metrics["n_cases"]))
    clf_table.add_row("  SIGNIFICANT",       str(clf_metrics["n_significant"]))
    clf_table.add_row("  WEAK",              str(clf_metrics["n_weak"]))
    clf_table.add_row("Train accuracy",      f"{clf_metrics['train_accuracy']:.1%}")
    clf_table.add_row("Calibrated threshold", f"{clf_metrics['calibrated_threshold']:.3f}")
    console.print(clf_table)

    console.print("\n[bold]Top 10 features (severity classifier)[/bold]")
    top_feats = clf.feature_importances_.head(10)
    for feat, imp in top_feats.items():
        bar = "█" * int(imp * 80)
        console.print(f"  {feat:<28} {imp:.3f}  [green]{bar}[/green]")

    # ── Tornado count regressor ───────────────────────────────────────────────
    console.print("\n[bold]Training tornado count regressor[/bold]")
    reg = TornadoRegressor()
    reg_metrics = reg.train(cases)
    save_model("tornado_regressor", reg)

    reg_table = Table(show_header=False, box=None)
    reg_table.add_column(style="dim")
    reg_table.add_column()
    reg_table.add_row("Cases trained", str(reg_metrics["n_cases"]))
    reg_table.add_row("Train RMSE",    f"{reg_metrics['train_rmse']:.1f} tornadoes")
    reg_table.add_row("Train MAE",     f"{reg_metrics['train_mae']:.1f} tornadoes")
    console.print(reg_table)

    console.print("\n[bold]Top 10 features (tornado regressor)[/bold]")
    top_feats = reg.feature_importances_.head(10)
    for feat, imp in top_feats.items():
        bar = "█" * int(imp * 80)
        console.print(f"  {feat:<28} {imp:.3f}  [green]{bar}[/green]")

    console.print(
        "\n[bold green]Models saved to data/models/.[/bold green] "
        "Run [cyan]evaluate-models[/cyan] for honest cross-validated metrics."
    )


# ── train-cap-break-model ────────────────────────────────────────────────────

@cli.command("train-cap-break-model")
def train_cap_break_model():
    """
    Train the cap-break-probability logistic regression.

    Labels: EARLY_EROSION + CLEAN_EROSION = positive (cap erodes),
            NO_EROSION = negative (cap holds).
    Features: MLCAPE, MLCIN, SRH_0_1km, EHI, BWD_0_6km, lapse_rate_700_500.
    Saves to data/models/cap_break_prob_model.joblib.
    """
    from ok_weather_model.modeling.cap_break_classifier import train as _train
    console.print("[cyan]Training cap_break_prob logistic regression…[/cyan]")
    _train()
    console.print("[bold green]Done.[/bold green]")


# ── evaluate-models ───────────────────────────────────────────────────────────

@cli.command("evaluate-models")
@click.option("--start-year", default=CASE_LIBRARY_START_YEAR, show_default=True)
@click.option("--end-year", default=CASE_LIBRARY_END_YEAR, show_default=True)
def evaluate_models(start_year: int, end_year: int):
    """
    Leave-one-year-out cross-validation for both forecast models.

    Trains on all years except the held-out year, evaluates on the held-out
    year, and aggregates metrics across all folds.  This is the honest estimate
    of out-of-sample performance — training metrics from train-models are
    optimistic (in-sample).
    """
    from ok_weather_model.storage.database import Database
    from ok_weather_model.modeling import SeverityClassifier, TornadoRegressor

    db = Database()
    cases = db.query_parameter_space({
        "start_date": str(date(start_year, 1, 1)),
        "end_date": str(date(end_year, 12, 31)),
        "min_completeness": 0.3,
    })
    console.print(
        f"Evaluating on [cyan]{len(cases)}[/cyan] cases via leave-one-year-out CV..."
    )

    # ── Severity classifier ───────────────────────────────────────────────────
    console.print("\n[bold]Severity classifier (SIGNIFICANT vs WEAK)[/bold]")
    clf = SeverityClassifier()
    with console.status("Running LOYO folds..."):
        clf_eval = clf.evaluate(cases)

    if "error" in clf_eval:
        console.print(f"[red]{clf_eval['error']}[/red]")
    else:
        clf_table = Table(show_header=False, box=None)
        clf_table.add_column(style="dim")
        clf_table.add_column()
        clf_table.add_row("LOYO accuracy",      f"{clf_eval['loyo_accuracy']:.1%}")
        clf_table.add_row("LOYO ROC-AUC",       f"{clf_eval['loyo_roc_auc']:.3f}")
        clf_table.add_row("Tuned threshold",    f"{clf_eval['tuned_threshold']:.3f}")
        clf_table.add_row("Folds",              str(clf_eval["n_folds"]))
        clf_table.add_row("Predictions",        str(clf_eval["n_predictions"]))
        console.print(clf_table)
        console.print("\n[dim]Default threshold (0.5):[/dim]")
        console.print(clf_eval["classification_report"])
        console.print(f"\n[dim]Tuned threshold ({clf_eval['tuned_threshold']:.3f}):[/dim]")
        console.print(clf_eval["classification_report_tuned"])

    # ── Tornado count regressor ───────────────────────────────────────────────
    console.print("\n[bold]Tornado count regressor[/bold]")
    reg = TornadoRegressor()
    with console.status("Running LOYO folds..."):
        reg_eval = reg.evaluate(cases)

    if "error" in reg_eval:
        console.print(f"[red]{reg_eval['error']}[/red]")
    else:
        reg_table = Table(show_header=False, box=None)
        reg_table.add_column(style="dim")
        reg_table.add_column()
        reg_table.add_row("LOYO MAE",   f"{reg_eval['loyo_mae_counts']:.1f} tornadoes")
        reg_table.add_row("LOYO RMSE",  f"{reg_eval['loyo_rmse_counts']:.1f} tornadoes")
        reg_table.add_row("LOYO MAE (log-space)", f"{reg_eval['loyo_mae_log']:.3f}")
        reg_table.add_row("Folds",      str(reg_eval["n_folds"]))
        reg_table.add_row("Predictions", str(reg_eval["n_predictions"]))
        console.print(reg_table)


# ── predict-day ───────────────────────────────────────────────────────────────

@cli.command("predict-day")
@click.argument("case_ref")
def predict_day(case_ref: str):
    """
    Apply trained forecast models to a historical case.

    CASE_REF: case_id (e.g. 19990503_OK) or date string (YYYYMMDD).

    Useful for sanity-checking model outputs on known events — compare
    model predictions against the documented outcome.
    """
    from ok_weather_model.storage.database import Database
    from ok_weather_model.modeling import SeverityClassifier, TornadoRegressor, load_model

    # Normalize case_ref
    if not case_ref.endswith("_OK"):
        case_ref = f"{case_ref}_OK"

    db = Database()
    case = db.load_case(case_ref)
    if case is None:
        console.print(f"[red]Case not found: {case_ref}[/red]")
        raise SystemExit(1)

    if case.sounding_12Z is None or case.kinematics_12Z is None:
        console.print(f"[red]No sounding data for {case_ref} — cannot predict.[/red]")
        raise SystemExit(1)

    # Load trained models
    clf: SeverityClassifier = load_model("severity_classifier")
    reg: TornadoRegressor   = load_model("tornado_regressor")

    if clf is None or reg is None:
        console.print(
            "[yellow]Models not found. Run [cyan]train-models[/cyan] first.[/yellow]"
        )
        raise SystemExit(1)

    # Run predictions
    ctg = case.convective_temp_gap_12Z
    clf_result = clf.predict_proba(case.sounding_12Z, case.kinematics_12Z, ctg)
    reg_result = reg.predict(case.sounding_12Z, case.kinematics_12Z, ctg)

    # Display
    console.print(f"\n[bold]Model predictions for [cyan]{case_ref}[/cyan][/bold]")
    console.print(f"  Date: {case.date}  |  Actual outcome: [bold]{case.event_class.value}[/bold]  |  Actual tornadoes: {case.tornado_count}")

    from rich.table import Table as RTable
    pred_table = RTable(show_header=True, header_style="bold")
    pred_table.add_column("Model")
    pred_table.add_column("Prediction", justify="right")
    pred_table.add_column("Detail")

    sig_pct = clf_result["significant"]
    sig_color = "red" if sig_pct >= 0.6 else "yellow" if sig_pct >= 0.35 else "green"
    pred_table.add_row(
        "Severity classifier",
        f"[{sig_color}]{sig_pct:.0%} SIGNIFICANT[/{sig_color}]",
        f"WEAK {clf_result['weak']:.0%}",
    )
    pred_table.add_row(
        "Tornado count",
        f"{reg_result['expected_count']:.1f}",
        f"80% PI: {reg_result['interval_low']:.0f}–{reg_result['interval_high']:.0f}",
    )
    console.print(pred_table)

    # Feature summary
    console.print("\n[dim]Key environment (12Z sounding)[/dim]")
    s = case.sounding_12Z
    k = case.kinematics_12Z
    env_table = Table(show_header=False, box=None)
    env_table.add_column(style="dim")
    env_table.add_column()
    env_table.add_row("MLCAPE / MLCIN", f"{s.MLCAPE:.0f} / {s.MLCIN:.0f} J/kg")
    env_table.add_row("Cap strength",   f"{s.cap_strength:.1f}°C")
    env_table.add_row("SRH 0-3km",      f"{k.SRH_0_3km:.0f} m²/s²")
    env_table.add_row("BWD 0-6km",      f"{k.BWD_0_6km:.0f} kt")
    if k.STP is not None:
        env_table.add_row("STP", f"{k.STP:.2f}")
    if ctg is not None:
        env_table.add_row("Tc gap 12Z", f"{ctg:+.1f}°F")
    console.print(env_table)


if __name__ == "__main__":
    cli()
