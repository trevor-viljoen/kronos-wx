"""
Storage layer: SQLite for case metadata + Parquet for time series data.

Design:
    SQLite  — case index, ThermodynamicIndices, KinematicProfile (JSON blob)
    Parquet — MesonetTimeSeries observations, SoundingProfile level data

This two-tier approach keeps structured queries fast while keeping
time series storage efficient.
"""

import json
import logging
from datetime import datetime, date
from pathlib import Path
from typing import Optional

import pandas as pd

from ..models import (
    EventClass,
    CapBehavior,
    OklahomaCounty,
    OklahomaSoundingStation,
    HistoricalCase,
    MesonetTimeSeries,
    MesonetObservation,
    SoundingProfile,
    SoundingLevel,
    HRRRCountyPoint,
    HRRRCountySnapshot,
)
from ..config import DATA_DIR

logger = logging.getLogger(__name__)

DB_PATH = DATA_DIR / "kronos_wx.db"
PARQUET_DIR = DATA_DIR / "parquet"


class Database:
    """
    Manages SQLite case library and Parquet time series storage.

    Usage::

        db = Database()
        db.save_case(case)
        case = db.load_case("19990503_OK")
    """

    def __init__(
        self,
        db_path: Path = DB_PATH,
        parquet_dir: Path = PARQUET_DIR,
    ):
        self.db_path = db_path
        self.parquet_dir = parquet_dir
        self.parquet_dir.mkdir(parents=True, exist_ok=True)
        self._engine = None
        self._init_db()

    def _get_engine(self):
        """Lazily initialize SQLAlchemy engine."""
        if self._engine is None:
            try:
                from sqlalchemy import create_engine
                self._engine = create_engine(f"sqlite:///{self.db_path}", echo=False)
            except ImportError:
                raise ImportError("sqlalchemy is required. Install with: pip install sqlalchemy")
        return self._engine

    def _init_db(self) -> None:
        """Create tables if they don't exist."""
        from sqlalchemy import text
        engine = self._get_engine()
        with engine.connect() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS historical_cases (
                    case_id TEXT PRIMARY KEY,
                    date TEXT NOT NULL,
                    event_class TEXT NOT NULL,
                    storm_mode TEXT,
                    cap_behavior TEXT,
                    tornado_count INTEGER DEFAULT 0,
                    max_tornado_rating TEXT,
                    significant_severe INTEGER DEFAULT 0,
                    data_completeness_score REAL DEFAULT 0.0,
                    mesonet_data_available INTEGER DEFAULT 0,
                    sounding_data_available INTEGER DEFAULT 0,
                    radar_data_available INTEGER DEFAULT 0,
                    cap_erosion_time TEXT,
                    cap_erosion_county TEXT,
                    cap_erosion_mechanism TEXT,
                    cap_erosion_time_val TEXT,
                    convective_temp_gap_12Z REAL,
                    convective_temp_gap_15Z REAL,
                    convective_temp_gap_18Z REAL,
                    SPC_max_tornado_prob REAL,
                    SPC_risk_category TEXT,
                    forecast_verification TEXT,
                    primary_bust_mechanism TEXT,
                    notes TEXT,
                    full_json TEXT NOT NULL,
                    created_at TEXT DEFAULT (datetime('now')),
                    updated_at TEXT DEFAULT (datetime('now'))
                )
            """))
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_cases_date
                ON historical_cases (date)
            """))
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_cases_event_class
                ON historical_cases (event_class)
            """))
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_cases_cap_behavior
                ON historical_cases (cap_behavior)
            """))
            conn.commit()
        logger.debug("Database initialized at %s", self.db_path)

    # ── Case CRUD ──────────────────────────────────────────────────────────────

    def save_case(self, case: HistoricalCase) -> None:
        """
        Insert or replace a HistoricalCase in the database.
        Uses full JSON serialization for round-trip fidelity.
        """
        from sqlalchemy import text
        engine = self._get_engine()

        full_json = case.model_dump_json()

        row = {
            "case_id": case.case_id,
            "date": case.date.isoformat(),
            "event_class": case.event_class.value,
            "storm_mode": case.storm_mode.value if case.storm_mode else None,
            "cap_behavior": case.cap_behavior.value if case.cap_behavior else None,
            "tornado_count": case.tornado_count,
            "max_tornado_rating": case.max_tornado_rating.value if case.max_tornado_rating else None,
            "significant_severe": int(case.significant_severe),
            "data_completeness_score": case.data_completeness_score,
            "mesonet_data_available": int(case.mesonet_data_available),
            "sounding_data_available": int(case.sounding_data_available),
            "radar_data_available": int(case.radar_data_available),
            "cap_erosion_county": case.cap_erosion_county.name if case.cap_erosion_county else None,
            "cap_erosion_mechanism": case.cap_erosion_mechanism.value if case.cap_erosion_mechanism else None,
            "cap_erosion_time_val": case.cap_erosion_time.isoformat() if case.cap_erosion_time else None,
            "convective_temp_gap_12Z": case.convective_temp_gap_12Z,
            "convective_temp_gap_15Z": case.convective_temp_gap_15Z,
            "convective_temp_gap_18Z": case.convective_temp_gap_18Z,
            "SPC_max_tornado_prob": case.SPC_max_tornado_prob,
            "SPC_risk_category": case.SPC_risk_category,
            "forecast_verification": case.forecast_verification.value if case.forecast_verification else None,
            "primary_bust_mechanism": case.primary_bust_mechanism,
            "notes": case.notes,
            "full_json": full_json,
        }

        with engine.connect() as conn:
            conn.execute(
                text("""
                    INSERT OR REPLACE INTO historical_cases
                    (case_id, date, event_class, storm_mode, cap_behavior,
                     tornado_count, max_tornado_rating, significant_severe,
                     data_completeness_score, mesonet_data_available,
                     sounding_data_available, radar_data_available,
                     cap_erosion_county, cap_erosion_mechanism, cap_erosion_time_val,
                     convective_temp_gap_12Z, convective_temp_gap_15Z,
                     convective_temp_gap_18Z, SPC_max_tornado_prob,
                     SPC_risk_category, forecast_verification,
                     primary_bust_mechanism, notes, full_json,
                     updated_at)
                    VALUES
                    (:case_id, :date, :event_class, :storm_mode, :cap_behavior,
                     :tornado_count, :max_tornado_rating, :significant_severe,
                     :data_completeness_score, :mesonet_data_available,
                     :sounding_data_available, :radar_data_available,
                     :cap_erosion_county, :cap_erosion_mechanism, :cap_erosion_time_val,
                     :convective_temp_gap_12Z, :convective_temp_gap_15Z,
                     :convective_temp_gap_18Z, :SPC_max_tornado_prob,
                     :SPC_risk_category, :forecast_verification,
                     :primary_bust_mechanism, :notes, :full_json,
                     datetime('now'))
                """),
                row,
            )
            conn.commit()

        logger.debug("Saved case %s", case.case_id)

    def load_case(self, case_id: str) -> Optional[HistoricalCase]:
        """Load a HistoricalCase by ID. Returns None if not found."""
        from sqlalchemy import text
        engine = self._get_engine()

        with engine.connect() as conn:
            row = conn.execute(
                text("SELECT full_json FROM historical_cases WHERE case_id = :id"),
                {"id": case_id},
            ).fetchone()

        if row is None:
            return None

        return HistoricalCase.model_validate_json(row[0])

    def get_cases_by_class(self, event_class: EventClass) -> list[HistoricalCase]:
        """Return all cases with a given EventClass."""
        return self._query_cases("event_class = :val", {"val": event_class.value})

    def get_cases_by_cap_behavior(self, behavior: CapBehavior) -> list[HistoricalCase]:
        """Return all cases with a given CapBehavior."""
        return self._query_cases("cap_behavior = :val", {"val": behavior.value})

    def query_parameter_space(self, filters: dict) -> list[HistoricalCase]:
        """
        Flexible query over indexed case fields.

        filters dict keys can include:
            event_class, cap_behavior, storm_mode, min_tornado_count,
            max_tornado_count, start_date, end_date, significant_severe,
            min_completeness
        """
        conditions = []
        params = {}

        if "event_class" in filters:
            conditions.append("event_class = :event_class")
            params["event_class"] = filters["event_class"]

        if "cap_behavior" in filters:
            conditions.append("cap_behavior = :cap_behavior")
            params["cap_behavior"] = filters["cap_behavior"]

        if "storm_mode" in filters:
            conditions.append("storm_mode = :storm_mode")
            params["storm_mode"] = filters["storm_mode"]

        if "min_tornado_count" in filters:
            conditions.append("tornado_count >= :min_tc")
            params["min_tc"] = filters["min_tornado_count"]

        if "max_tornado_count" in filters:
            conditions.append("tornado_count <= :max_tc")
            params["max_tc"] = filters["max_tornado_count"]

        if "start_date" in filters:
            conditions.append("date >= :start_date")
            params["start_date"] = filters["start_date"]

        if "end_date" in filters:
            conditions.append("date <= :end_date")
            params["end_date"] = filters["end_date"]

        if "significant_severe" in filters:
            conditions.append("significant_severe = :sig")
            params["sig"] = int(filters["significant_severe"])

        if "min_completeness" in filters:
            conditions.append("data_completeness_score >= :min_comp")
            params["min_comp"] = filters["min_completeness"]

        where = " AND ".join(conditions) if conditions else "1=1"
        return self._query_cases(where, params)

    def _query_cases(self, where: str, params: dict) -> list[HistoricalCase]:
        from sqlalchemy import text
        engine = self._get_engine()

        with engine.connect() as conn:
            rows = conn.execute(
                text(f"SELECT full_json FROM historical_cases WHERE {where} ORDER BY date"),
                params,
            ).fetchall()

        results = []
        for row in rows:
            try:
                results.append(HistoricalCase.model_validate_json(row[0]))
            except Exception as exc:
                logger.warning("Failed to deserialize case: %s", exc)

        return results

    def case_exists(self, case_id: str) -> bool:
        """Check if a case_id exists (for resume capability)."""
        from sqlalchemy import text
        engine = self._get_engine()
        with engine.connect() as conn:
            count = conn.execute(
                text("SELECT COUNT(*) FROM historical_cases WHERE case_id = :id"),
                {"id": case_id},
            ).scalar()
        return count > 0

    # ── Mesonet time series ────────────────────────────────────────────────────

    def save_mesonet_timeseries(self, ts: MesonetTimeSeries) -> None:
        """
        Persist a MesonetTimeSeries to Parquet.
        Partitioned by county/date for efficient range queries.
        """
        if not ts.observations:
            return

        records = [
            {
                "station_id": obs.station_id,
                "county": obs.county.name,
                "valid_time": obs.valid_time.isoformat(),
                "temperature": obs.temperature,
                "dewpoint": obs.dewpoint,
                "relative_humidity": obs.relative_humidity,
                "wind_direction": obs.wind_direction,
                "wind_speed": obs.wind_speed,
                "wind_gust": obs.wind_gust,
                "pressure": obs.pressure,
                "solar_radiation": obs.solar_radiation,
                "soil_temperature_5cm": obs.soil_temperature_5cm,
                "soil_moisture_5cm": obs.soil_moisture_5cm,
                "precipitation": obs.precipitation,
            }
            for obs in ts.observations
        ]

        df = pd.DataFrame(records)
        df["valid_time"] = pd.to_datetime(df["valid_time"])

        date_str = ts.start_time.strftime("%Y%m%d")
        out_dir = self.parquet_dir / "mesonet" / ts.county.name
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f"{ts.station_id}_{date_str}.parquet"

        df.to_parquet(out_path, index=False, engine="pyarrow")
        logger.debug("Saved Mesonet timeseries → %s", out_path)

    def load_mesonet_timeseries(
        self,
        county: OklahomaCounty,
        start: datetime,
        end: datetime,
    ) -> Optional[MesonetTimeSeries]:
        """
        Load Mesonet time series for a county over a date range.
        Reads all Parquet files for that county covering the range.
        """
        county_dir = self.parquet_dir / "mesonet" / county.name
        if not county_dir.exists():
            return None

        dfs = []
        current = start.date()
        while current <= end.date():
            pattern = f"*_{current.strftime('%Y%m%d')}.parquet"
            for path in county_dir.glob(pattern):
                try:
                    dfs.append(pd.read_parquet(path))
                except Exception as exc:
                    logger.warning("Failed to read %s: %s", path, exc)
            from datetime import timedelta
            current += timedelta(days=1)

        if not dfs:
            return None

        df = pd.concat(dfs, ignore_index=True)
        df["valid_time"] = pd.to_datetime(df["valid_time"])
        df = df.sort_values("valid_time")

        # Filter to requested time range
        df = df[(df["valid_time"] >= pd.Timestamp(start)) & (df["valid_time"] <= pd.Timestamp(end))]

        if df.empty:
            return None

        observations = [
            MesonetObservation(
                station_id=row["station_id"],
                county=county,
                valid_time=row["valid_time"].to_pydatetime(),
                temperature=row["temperature"],
                dewpoint=row["dewpoint"],
                relative_humidity=row["relative_humidity"],
                wind_direction=row["wind_direction"],
                wind_speed=row["wind_speed"],
                wind_gust=row.get("wind_gust"),
                pressure=row["pressure"],
                solar_radiation=row.get("solar_radiation"),
                soil_temperature_5cm=row.get("soil_temperature_5cm"),
                soil_moisture_5cm=row.get("soil_moisture_5cm"),
                precipitation=row.get("precipitation"),
            )
            for _, row in df.iterrows()
        ]

        station_ids = df["station_id"].unique()
        primary_station = station_ids[0] if len(station_ids) == 1 else county.mesonet_station_id

        ts = MesonetTimeSeries(
            station_id=primary_station,
            county=county,
            start_time=start,
            end_time=end,
            observations=observations,
        )
        ts.compute_tendencies()
        return ts

    # ── Sounding storage ───────────────────────────────────────────────────────

    def save_sounding(self, sounding: SoundingProfile) -> None:
        """Persist a SoundingProfile to Parquet."""
        records = [
            {
                "station": sounding.station.value,
                "valid_time": sounding.valid_time.isoformat(),
                "raw_source": sounding.raw_source,
                "pressure": lev.pressure,
                "height": lev.height,
                "temperature": lev.temperature,
                "dewpoint": lev.dewpoint,
                "wind_direction": lev.wind_direction,
                "wind_speed": lev.wind_speed,
            }
            for lev in sounding.levels
        ]

        df = pd.DataFrame(records)
        date_str = sounding.valid_time.strftime("%Y%m%d")
        hour_str = sounding.valid_time.strftime("%H")

        out_dir = self.parquet_dir / "soundings" / sounding.station.value
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f"{date_str}_{hour_str}Z.parquet"

        df.to_parquet(out_path, index=False, engine="pyarrow")
        logger.debug("Saved sounding → %s", out_path)

    def load_sounding(
        self,
        station: OklahomaSoundingStation,
        valid_time: datetime,
    ) -> Optional[SoundingProfile]:
        """Load a SoundingProfile from Parquet."""
        date_str = valid_time.strftime("%Y%m%d")
        hour_str = valid_time.strftime("%H")

        path = (
            self.parquet_dir
            / "soundings"
            / station.value
            / f"{date_str}_{hour_str}Z.parquet"
        )

        if not path.exists():
            return None

        df = pd.read_parquet(path)
        if df.empty:
            return None

        levels = [
            SoundingLevel(
                pressure=row["pressure"],
                height=row["height"],
                temperature=row["temperature"],
                dewpoint=row["dewpoint"],
                wind_direction=row["wind_direction"],
                wind_speed=row["wind_speed"],
            )
            for _, row in df.iterrows()
        ]

        return SoundingProfile(
            station=station,
            valid_time=valid_time,
            levels=levels,
            raw_source=df["raw_source"].iloc[0],
        )

    def get_case_statistics(self) -> dict:
        """Return summary statistics for the case library."""
        from sqlalchemy import text
        engine = self._get_engine()

        with engine.connect() as conn:
            rows = conn.execute(
                text("""
                    SELECT event_class, COUNT(*) as count,
                           AVG(data_completeness_score) as avg_completeness,
                           AVG(tornado_count) as avg_tornadoes
                    FROM historical_cases
                    GROUP BY event_class
                    ORDER BY count DESC
                """)
            ).fetchall()

        return {
            row[0]: {
                "count": row[1],
                "avg_completeness": round(row[2], 3),
                "avg_tornadoes": round(row[3], 1),
            }
            for row in rows
        }

    def save_hrrr_snapshot(self, snap: HRRRCountySnapshot, case_id: str) -> None:
        """Persist an HRRRCountySnapshot for a case to Parquet (77 rows × county fields)."""
        records = []
        for pt in snap.counties:
            records.append({
                "county":             pt.county.name,
                "valid_time":         snap.valid_time.isoformat(),
                "run_time":           snap.run_time.isoformat(),
                "fxx":                snap.fxx,
                "MLCAPE":             pt.MLCAPE,
                "MLCIN":              pt.MLCIN,
                "SBCAPE":             pt.SBCAPE,
                "SBCIN":              pt.SBCIN,
                "SRH_0_1km":          pt.SRH_0_1km,
                "SRH_0_3km":          pt.SRH_0_3km,
                "BWD_0_6km":          pt.BWD_0_6km,
                "lapse_rate_700_500": pt.lapse_rate_700_500,
                "dewpoint_2m_F":      pt.dewpoint_2m_F,
                "LCL_height_m":       pt.LCL_height_m,
                "EHI":                pt.EHI,
                "STP":                pt.STP,
            })
        df = pd.DataFrame(records)
        out_dir = self.parquet_dir / "hrrr"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f"{case_id}.parquet"
        df.to_parquet(out_path, index=False, engine="pyarrow")
        logger.debug("Saved HRRR snapshot → %s", out_path)

    def load_hrrr_snapshot(self, case_id: str) -> Optional[HRRRCountySnapshot]:
        """Load the 12Z HRRR snapshot for a case from Parquet."""
        path = self.parquet_dir / "hrrr" / f"{case_id}.parquet"
        if not path.exists():
            return None

        import math as _math
        from datetime import timezone as _tz

        def _opt(v):
            if v is None:
                return None
            try:
                f = float(v)
                return None if _math.isnan(f) else f
            except (TypeError, ValueError):
                return None

        def _parse_dt(s):
            s = str(s)
            dt = datetime.fromisoformat(s)
            return dt if dt.tzinfo else dt.replace(tzinfo=_tz.utc)

        df = pd.read_parquet(path)
        if df.empty:
            return None

        first = df.iloc[0]
        counties = []
        for _, row in df.iterrows():
            try:
                county_enum = OklahomaCounty[row["county"]]
            except KeyError:
                continue
            counties.append(HRRRCountyPoint(
                county=county_enum,
                MLCAPE=float(row["MLCAPE"]),
                MLCIN=float(row["MLCIN"]),
                SBCAPE=float(row["SBCAPE"]),
                SBCIN=float(row["SBCIN"]),
                SRH_0_1km=float(row["SRH_0_1km"]),
                SRH_0_3km=float(row["SRH_0_3km"]),
                BWD_0_6km=float(row["BWD_0_6km"]),
                lapse_rate_700_500=_opt(row["lapse_rate_700_500"]),
                dewpoint_2m_F=float(row["dewpoint_2m_F"]),
                LCL_height_m=_opt(row["LCL_height_m"]),
                EHI=_opt(row["EHI"]),
                STP=_opt(row["STP"]),
            ))

        return HRRRCountySnapshot(
            valid_time=_parse_dt(first["valid_time"]),
            run_time=_parse_dt(first["run_time"]),
            fxx=int(first["fxx"]),
            counties=counties,
        )
