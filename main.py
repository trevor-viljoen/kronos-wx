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
              help="Re-enrich already-enriched cases")
def enrich_all(start_year: int, end_year: int, force: bool):
    """
    Enrich all cases in a year range. Supports resume (skips already-enriched).

    START_YEAR END_YEAR: e.g.  enrich-all 1994 2023
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

    to_enrich = cases if force else [
        c for c in cases if not c.sounding_data_available
    ]

    console.print(
        f"Found {len(cases)} total cases. "
        f"Enriching {len(to_enrich)} (skipping {len(cases) - len(to_enrich)} already enriched)."
    )

    from ok_weather_model.ingestion import SoundingClient
    from ok_weather_model.processing import compute_thermodynamic_indices, compute_kinematic_profile

    errors = []
    enriched = 0

    with SoundingClient() as sc:
        for case in track(to_enrich, description="Enriching cases..."):
            try:
                profile = sc.get_sounding_with_fallback(
                    OklahomaSoundingStation.OUN, case.date, 12
                )
                if profile is None:
                    logger.debug("No sounding at any station for %s", case.case_id)
                    continue

                db.save_sounding(profile)
                indices    = compute_thermodynamic_indices(profile)
                kinematics = compute_kinematic_profile(profile, indices)

                case.sounding_12Z          = indices
                case.kinematics_12Z        = kinematics
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

    # Build budgets for OUN county (Cleveland Co.) using available data
    budgets: list[CapErosionBudget] = []
    county = OklahomaCounty.CLEVELAND

    for label, hour in [("12Z", 12), ("15Z", 15), ("18Z", 18), ("21Z", 21)]:
        valid_time = datetime(
            case_date.year, case_date.month, case_date.day, hour, 0,
            tzinfo=timezone.utc
        )

        mesonet_ts = db.load_mesonet_timeseries(
            county,
            valid_time - timedelta(minutes=15),
            valid_time + timedelta(minutes=15),
        )
        if mesonet_ts is None or not mesonet_ts.observations:
            continue

        from ok_weather_model.ingestion import MesonetClient
        with MesonetClient() as mc:
            surface_state = mc.compute_county_surface_state(county, valid_time, [mesonet_ts])

        if surface_state is None:
            continue

        try:
            budget = compute_cap_erosion_budget(case.sounding_12Z, surface_state)
            budgets.append(budget)
        except Exception as exc:
            logger.warning("Budget computation failed at %s: %s", label, exc)

    if not budgets:
        console.print("[red]No valid budget snapshots — check Mesonet data availability[/red]")
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

    # ── Print summary report ─────────────────────────────────────────────────
    table = Table(title=f"Cap Erosion Report — {case_id}", show_lines=True)
    table.add_column("Metric", style="cyan")
    table.add_column("Value", style="green")

    table.add_row("Event Class", case.event_class.value)
    table.add_row("Cap Behavior", cap_behavior.value)
    table.add_row("Primary Mechanism", trajectory.primary_mechanism.value)
    table.add_row("Erosion Achieved", "YES" if trajectory.erosion_achieved else "NO")
    table.add_row(
        "Erosion Time",
        trajectory.erosion_time.strftime("%H:%M UTC") if trajectory.erosion_time else "—"
    )
    table.add_row("Bust Risk Score", f"{trajectory.bust_risk_score:.2f}")
    table.add_row("12Z MLCAPE", f"{case.sounding_12Z.MLCAPE:.0f} J/kg")
    table.add_row("12Z MLCIN", f"{case.sounding_12Z.MLCIN:.0f} J/kg")
    table.add_row("12Z Cap Strength", f"{case.sounding_12Z.cap_strength:.1f}°C")
    if case.convective_temp_gap_12Z is not None:
        table.add_row("12Z Tc Gap", f"{case.convective_temp_gap_12Z:+.1f}°F")
    if case.convective_temp_gap_15Z is not None:
        table.add_row("15Z Tc Gap", f"{case.convective_temp_gap_15Z:+.1f}°F")
    if case.convective_temp_gap_18Z is not None:
        table.add_row("18Z Tc Gap", f"{case.convective_temp_gap_18Z:+.1f}°F")
    table.add_row("Forcing Window Close", forcing_window_close.strftime("%H:%M UTC"))

    console.print(table)

    # Budget timeline
    budget_table = Table(title="Cap Erosion Budget Timeline", show_lines=True)
    budget_table.add_column("Time")
    budget_table.add_column("CIN (J/kg)", justify="right")
    budget_table.add_column("Heating", justify="right")
    budget_table.add_column("Dynamic", justify="right")
    budget_table.add_column("Net", justify="right")
    budget_table.add_column("Hrs to 0", justify="right")

    for b in trajectory.budget_history:
        hrs = b.hours_to_erosion
        budget_table.add_row(
            b.valid_time.strftime("%H:%M Z"),
            f"{b.current_CIN:.0f}",
            f"{b.heating_forcing:+.1f}",
            f"{b.dynamic_forcing:+.1f}",
            f"[red]{b.net_tendency:+.1f}[/red]" if b.net_tendency < -10 else f"{b.net_tendency:+.1f}",
            f"{hrs:.1f}" if hrs is not None else "∞",
        )

    console.print(budget_table)


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

    erosion_hour = trajectory.erosion_time.hour if trajectory.erosion_time else 23

    # Check if boundary forcing drove erosion
    if trajectory.primary_mechanism.value == "BOUNDARY":
        return CapBehavior.BOUNDARY_FORCED

    # Timing-based classification (Oklahoma climatology reference times)
    if erosion_hour <= 18:  # before 18Z (1pm CDT)
        return CapBehavior.EARLY_EROSION
    elif erosion_hour <= 21:  # 18Z–21Z (1–4pm CDT)
        return CapBehavior.CLEAN_EROSION
    elif erosion_hour <= 23:  # 21Z–23Z (4–6pm CDT)
        return CapBehavior.LATE_EROSION
    else:
        return CapBehavior.LATE_EROSION


if __name__ == "__main__":
    cli()
