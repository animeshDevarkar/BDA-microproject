"""Automated checks for the flight delay pipeline.

Every test builds its own small CSV in a temporary folder, so the suite does not
depend on (or modify) data/raw/flights.csv.  Run with:  pytest -q
"""
from __future__ import annotations

import csv
import json
import sys
from datetime import date
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import flight_delay_pipeline as pipeline  # noqa: E402  (configures Java/Hadoop env)
from make_sample_data import COLUMNS, GUIDE_ROWS, random_rows  # noqa: E402


def write_csv(path: Path, header: list[str], rows: list[list]) -> Path:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(header)
        writer.writerows(rows)
    return path


@pytest.fixture(scope="session")
def spark():
    session = pipeline.build_spark(driver_memory="1g", master="local[2]")
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture()
def guide_csv(tmp_path) -> Path:
    return write_csv(tmp_path / "flights.csv", COLUMNS, GUIDE_ROWS)


@pytest.fixture()
def guide_df(spark, guide_csv):
    return pipeline.load_and_standardize(spark, guide_csv)


# --- Loading & cleaning ------------------------------------------------------

def test_dates_routes_and_delay_flags(guide_df):
    df = guide_df
    assert "arrival_delayed_15" in df.columns
    assert "departure_delayed_15" in df.columns
    assert df.count() == 11
    assert df.filter(df.flight_date.isNull()).count() == 0
    assert df.filter(df.origin == df.destination).count() == 0
    for flag in ("arrival_delayed_15", "departure_delayed_15", "cancelled", "diverted"):
        values = {row[0] for row in df.select(flag).distinct().collect()}
        assert values.issubset({0, 1}), flag


def test_delay_flag_values(guide_df):
    rows = {(r.route, r.flight_date): r for r in guide_df.collect()}
    # 20 min late -> delayed; 18 -> delayed; 2 -> on time; cancelled -> never delayed.
    assert rows[("JFK-ORD", date(2024, 1, 1))].arrival_delayed_15 == 1
    assert rows[("ORD-JFK", date(2024, 2, 6))].arrival_delayed_15 == 1
    assert rows[("ORD-LAX", date(2024, 1, 2))].arrival_delayed_15 == 0
    cancelled = rows[("LAX-ORD", date(2024, 3, 9))]
    assert cancelled.cancelled == 1 and cancelled.arrival_delayed_15 == 0
    # Departure delay of exactly 15 counts as delayed (>= threshold).
    assert rows[("ATL-LAX", date(2024, 3, 8))].departure_delayed_15 == 1


def test_scheduled_hour_and_derived_fields(guide_df):
    row = guide_df.filter("route = 'ORD-LAX'").first()
    assert row.scheduled_departure_hour == 14
    assert row.year_month == "2024-01"
    assert row.day_of_week == 2


def test_cleaning_rules(spark, tmp_path):
    rows = [
        [2024, 1, 1, 1, "AA", "JFK", "ORD", 800, 0, 0, 0, 0, None, None, None, None, None],
        [2024, 1, 1, 1, "AA", "JFK", "ORD", 800, 0, 0, 0, 0, None, None, None, None, None],  # duplicate
        [2024, 1, 2, 2, "AA", "JFK", "JFK", 900, 0, 0, 0, 0, None, None, None, None, None],  # same airport
        [2024, 13, 1, 1, "AA", "JFK", "ORD", 900, 0, 0, 0, 0, None, None, None, None, None],  # bad month
        [2024, 2, 30, 1, "AA", "JFK", "ORD", 900, 0, 0, 0, 0, None, None, None, None, None],  # bad day
        [2024, 1, 3, 3, "", "JFK", "ORD", 900, 0, 0, 0, 0, None, None, None, None, None],    # no airline
        [2024, 1, 4, 4, " dl ", "atl", "lax", 2400, 0, 0, 0, 0, None, None, None, None, None],
        [2024, 1, 5, 5, "UA", "ORD", "SFO", 2575, 0, 0, 0, 0, None, None, None, None, None],  # bad time
    ]
    df = pipeline.load_and_standardize(spark, write_csv(tmp_path / "f.csv", COLUMNS, rows))
    result = {r.route: r for r in df.collect()}
    assert df.count() == 3
    assert set(result) == {"JFK-ORD", "ATL-LAX", "ORD-SFO"}
    assert result["ATL-LAX"].airline == "DL"                  # trimmed + upper-cased
    assert result["ATL-LAX"].scheduled_departure_hour == 0     # 2400 = midnight
    assert result["ORD-SFO"].scheduled_departure_hour is None  # invalid HHMM


def test_column_aliases_and_flight_date_only(spark, tmp_path):
    header = ["FL_DATE", "OP_UNIQUE_CARRIER", "ORIGIN", "DEST", "CRS_DEP_TIME",
              "DEP_DELAY", "ARR_DELAY", "CANCELLED", "DIVERTED"]
    rows = [["2024-05-03", "WN", "DAL", "HOU", "0705", "12.00", "16.00", "0.00", "0.00"]]
    df = pipeline.load_and_standardize(spark, write_csv(tmp_path / "f.csv", header, rows))
    row = df.first()
    assert (row.year, row.month, row.day) == (2024, 5, 3)
    assert row.airline == "WN" and row.route == "DAL-HOU"
    assert row.scheduled_departure_hour == 7
    assert row.day_of_week == 5  # Friday, Monday=1 convention
    assert row.arrival_delayed_15 == 1


def test_missing_input_raises(spark, tmp_path):
    with pytest.raises(FileNotFoundError):
        pipeline.load_and_standardize(spark, tmp_path / "missing.csv")


def test_missing_required_columns_raises(spark, tmp_path):
    path = write_csv(tmp_path / "f.csv", ["Year", "Month", "DayofMonth", "Origin"], [[2024, 1, 1, "JFK"]])
    with pytest.raises(ValueError, match="Missing required fields"):
        pipeline.load_and_standardize(spark, path)


# --- Summaries ---------------------------------------------------------------

def test_kpis_and_rate_reconcile(guide_df):
    tables = pipeline.build_summaries(guide_df, min_group_flights=1, min_route_flights=1)
    kpi = {r.metric: r.value for r in tables["kpi_summary.csv"].collect()}
    assert kpi["cleaned_records"] == 11
    assert kpi["operated_flights_with_arrival_data"] == 9
    assert kpi["arrival_delayed_flights_15_min_or_more"] == 6
    assert kpi["arrival_delay_rate_pct"] == pytest.approx(66.67, abs=0.01)
    assert kpi["cancelled_flights"] == 1
    assert kpi["diverted_flights"] == 1
    assert kpi["first_flight_date_yyyymmdd"] == 20240101
    assert kpi["last_flight_date_yyyymmdd"] == 20240310


def test_delay_cause_totals(guide_df):
    causes = {r.delay_cause: r for r in pipeline.build_summaries(guide_df, 1, 1)["delay_cause_summary.csv"].collect()}
    assert causes["Carrier"].total_reported_delay_minutes == 75
    assert causes["Weather"].total_reported_delay_minutes == 10
    assert causes["National Air System"].total_reported_delay_minutes == 83
    assert causes["Security"].total_reported_delay_minutes == 0
    assert causes["Late Aircraft"].total_reported_delay_minutes == 60
    assert sum(c.share_of_reported_minutes_pct for c in causes.values()) == pytest.approx(100, abs=0.05)


def test_group_summaries_are_consistent(spark, tmp_path):
    rows = random_rows(3000, 2024, seed=7)
    df = pipeline.load_and_standardize(spark, write_csv(tmp_path / "f.csv", COLUMNS, rows)).cache()
    tables = pipeline.build_summaries(df, min_group_flights=100, min_route_flights=20)
    kpi = {r.metric: r.value for r in tables["kpi_summary.csv"].collect()}

    monthly = tables["monthly_summary.csv"].collect()
    assert len(monthly) == 12
    assert sum(r.operated_flights_with_arrival_data for r in monthly) == kpi["operated_flights_with_arrival_data"]
    assert sum(r.arrival_delayed_flights for r in monthly) == kpi["arrival_delayed_flights_15_min_or_more"]

    for name in ("airline_summary.csv", "destination_airport_summary.csv", "route_summary.csv", "hourly_summary.csv",
                 "monthly_summary.csv", "weekday_summary.csv"):
        for r in tables[name].collect():
            assert 0 <= r.arrival_delay_rate_pct <= 100, name
    for r in tables["origin_airport_summary.csv"].collect():
        assert 0 <= r.departure_delay_rate_pct <= 100
        assert r.flights >= 100
    assert all(r.flights >= 20 for r in tables["route_summary.csv"].collect())
    assert {r.airline_name for r in tables["airline_summary.csv"].collect()} >= {"Delta Air Lines", "JetBlue Airways"}
    assert [r.day_name for r in tables["weekday_summary.csv"].collect()] == ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]


def test_volume_thresholds_filter_small_groups(guide_df):
    tables = pipeline.build_summaries(guide_df)  # defaults: 100 / 50 flights
    for name in ("airline_summary.csv", "origin_airport_summary.csv",
                 "destination_airport_summary.csv", "route_summary.csv"):
        assert tables[name].count() == 0, name


def test_website_tables(spark, tmp_path):
    from build_website_data import build_tables

    rows = random_rows(3000, 2024, seed=11)
    df = pipeline.load_and_standardize(spark, write_csv(tmp_path / "f.csv", COLUMNS, rows)).cache()
    data = build_tables(df)
    tables = {name: [dict(zip(data[name]["cols"], row)) for row in data[name]["rows"]]
              for name in ("airports", "routes", "route_airlines", "airport_hours", "months")}

    assert data["overall"]["flights"] == df.count()
    assert len(tables["months"]) == 12
    assert sum(m["flights"] for m in tables["months"]) == df.count()
    assert tables["routes"] and all(r["flights"] >= 30 for r in tables["routes"])
    assert all(r["flights"] >= 20 for r in tables["route_airlines"])
    assert all(0 <= r["hour"] <= 23 for r in tables["airport_hours"])
    for name, rows_ in tables.items():
        for r in rows_:
            assert r["late_pct"] is None or 0 <= r["late_pct"] <= 100, name
            assert 0 <= r["cancel_pct"] <= 100, name
    # A route's flights equal the sum over its airlines when every airline clears the minimum.
    routes = {(r["origin"], r["destination"]): r["flights"] for r in tables["routes"]}
    by_route: dict = {}
    for r in tables["route_airlines"]:
        by_route[(r["origin"], r["destination"])] = by_route.get((r["origin"], r["destination"]), 0) + r["flights"]
    assert all(total <= routes.get(key, total) for key, total in by_route.items())
    json.dumps(data, allow_nan=False)  # must serialise for the website


def test_download_month_range():
    from download_bts_data import month_range
    assert list(month_range(date(2025, 11, 1), date(2026, 2, 1))) == [(2025, 11), (2025, 12), (2026, 1), (2026, 2)]


# --- End to end ----------------------------------------------------------------
# Keep this test last: main() stops the shared SparkSession when it finishes.

def test_end_to_end_writes_all_outputs(tmp_path, monkeypatch):
    import build_dashboard

    source = write_csv(tmp_path / "flights.csv", COLUMNS, random_rows(1500, 2024, seed=3))
    processed, output, dash = tmp_path / "parquet", tmp_path / "powerbi", tmp_path / "dashboard"
    monkeypatch.setattr(build_dashboard, "DASHBOARD_DIR", dash)
    monkeypatch.setattr(build_dashboard, "ROOT", tmp_path)
    monkeypatch.setattr(pipeline, "ROOT", tmp_path)  # keep run_summary.txt out of the project

    assert pipeline.main(["--input", str(source), "--processed", str(processed), "--output", str(output),
                          "--min-group-flights", "50", "--min-route-flights", "10", "--driver-memory", "1g"]) == 0

    expected = ["monthly_summary.csv", "airline_summary.csv", "origin_airport_summary.csv",
                "destination_airport_summary.csv", "route_summary.csv", "hourly_summary.csv",
                "delay_cause_summary.csv", "kpi_summary.csv", "weekday_summary.csv"]
    for name in expected:
        assert (output / name).stat().st_size > 0, name
    assert not list(output.glob("*_spark_tmp"))
    assert list(processed.glob("year_month=*/*.parquet"))
    assert (tmp_path / "output" / "run_summary.txt").exists()

    text = (dash / "data.js").read_text(encoding="utf-8")
    assert text.startswith("window.DASHBOARD_DATA = ")
    data = json.loads(text[len("window.DASHBOARD_DATA = "):].rstrip().rstrip(";"))
    assert data["kpi"]["cleaned_records"] == 1500
    assert len(data["monthly"]) == 12 and data["routes"]

    site = (tmp_path / "website" / "data" / "site-data.js").read_text(encoding="utf-8")
    assert site.startswith("window.SITE_DATA = ")
    site_data = json.loads(site[len("window.SITE_DATA = "):].rstrip().rstrip(";"))
    assert site_data["overall"]["flights"] == 1500 and site_data["airline_names"]["DL"] == "Delta Air Lines"
