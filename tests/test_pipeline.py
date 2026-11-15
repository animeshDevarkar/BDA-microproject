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


def read_registered_js(path: Path, registry: str, key: str) -> dict:
    """Parse a data file of the form `window.X = window.X || {};
window.X["key"] = {...};`."""
    lines = path.read_text(encoding="utf-8").splitlines()
    assert lines[0] == f"window.{registry} = window.{registry} || {{}};"
    prefix = f'window.{registry}["{key}"] = '
    assert lines[1].startswith(prefix)
    return json.loads(lines[1][len(prefix):].rstrip(";"))


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


def test_india_simulation_is_valid_and_calibrated(spark, tmp_path):
    from datetime import date as day
    import make_india_data as india

    frame = india.generate(day(2025, 12, 1), day(2026, 1, 31), flights_per_day=400, seed=5)
    again = india.generate(day(2025, 12, 1), day(2026, 1, 31), flights_per_day=400, seed=5)
    assert frame.equals(again)  # reproducible from the seed
    assert list(frame.columns) == india.COLUMNS
    assert len(frame) == 400 * 62
    assert set(frame["Reporting_Airline"]) <= set(india.AIRLINES)
    assert set(frame["Origin"]) | set(frame["Dest"]) <= set(india.AIRPORTS)
    assert (frame["Origin"] != frame["Dest"]).all()

    operated = frame[frame["ArrDelay"].notna()]
    late = operated[operated["ArrDelay"] >= 15]
    # Cause minutes add up to the arrival delay for late flights, as in BTS data.
    causes = late[india.CAUSES].sum(axis=1)
    assert (causes - late["ArrDelay"]).abs().max() < 1e-6
    assert operated.loc[operated["ArrDelay"] < 15, india.CAUSES].isna().all().all()
    # SpiceJet must be far less punctual than IndiGo, as in DGCA data.
    rate = operated.groupby("Reporting_Airline")["ArrDelay"].apply(lambda s: (s >= 15).mean())
    assert rate["SG"] > rate["6E"] + 0.15
    # Winter fog makes Delhi departures later than Bengaluru ones.
    by_origin = operated.groupby("Origin")["ArrDelay"].apply(lambda s: (s >= 15).mean())
    assert by_origin["DEL"] > by_origin["BLR"]

    path = tmp_path / "india.csv"
    frame.to_csv(path, index=False, float_format="%.0f")
    df = pipeline.load_and_standardize(spark, path)
    assert df.count() == len(frame)
    row = df.filter("origin = 'BOM'").first()
    assert row.origin_city == "Mumbai, Maharashtra"
    names = {r.airline_name for r in pipeline.build_summaries(df, 1, 1)["airline_summary.csv"].collect()}
    assert {"IndiGo", "Air India", "SpiceJet"} <= names


def fare_rows(quarters: list[tuple[int, int]], seed: int) -> list[list]:
    """Table 1a-style fares for every pair of the sample airports: distance, season and airline effects."""
    import random
    from make_sample_data import AIRPORTS

    rng = random.Random(seed)
    codes = sorted(AIRPORTS)
    pairs = [(a, b) for i, a in enumerate(codes) for b in codes[i + 1:]]
    rows = []
    for k, (a, b) in enumerate(pairs):
        miles = 300 + 97 * k % 2200
        for year, quarter in quarters:
            base = (60 + 0.16 * miles) * (1.15 if quarter == 3 else 1.0) * (1.02 ** (year - 2022))
            fare_lg, fare_low = base * 1.1 * rng.uniform(0.97, 1.03), base * 0.8 * rng.uniform(0.97, 1.03)
            rows.append([year, quarter, b, a, a, b, miles, 500 + 10 * k, round((fare_lg + fare_low) / 2, 2),
                         "DL", 0.6, round(fare_lg, 2), "WN" if k % 3 else "DL", 0.4, round(fare_low, 2)])
    return rows


def test_fare_labels_are_one_row_per_pair_quarter_and_airline(spark, tmp_path):
    import fare_price_model as fares
    from download_fare_data import COLUMNS as FARE_COLUMNS

    rows = [[2024, 1, "LAX", "JFK", "Los Angeles", "New York", 2475, 900, 300, "DL", 0.5, 340, "B6", 0.3, 250],
            [2024, 1, "ATL", "ORD", "Atlanta", "Chicago", 606, 700, 200, "DL", 0.6, 210, "DL", 0.6, 210],
            [2024, 1, "ATL", "ATL", "Atlanta", "Atlanta", 10, 5, 99, "DL", 1, 99, "DL", 1, 99],   # same airport
            [2024, 5, "ATL", "DEN", "Atlanta", "Denver", 1199, 400, 250, "DL", 0.6, 260, "UA", 0.3, 230]]  # bad quarter
    df = fares.load_fares(spark, write_csv(tmp_path / "fares.csv", FARE_COLUMNS, rows))
    got = {(r.airport_a, r.airport_b, r.carrier): r.fare for r in df.collect()}
    # Pairs are unordered (JFK < LAX); the market average is the "ALL" airline.
    assert got == {("JFK", "LAX", "ALL"): 300, ("JFK", "LAX", "DL"): 340, ("JFK", "LAX", "B6"): 250,
                   ("ATL", "ORD", "ALL"): 200, ("ATL", "ORD", "DL"): 210}
    assert {r.period for r in df.collect()} == {2024 * 4}
    with pytest.raises(FileNotFoundError):
        fares.load_fares(spark, tmp_path / "missing.csv")


def test_fare_model_trains_forecasts_and_exports(spark, tmp_path):
    from pyspark.ml import PipelineModel
    import fare_price_model as fares
    from download_fare_data import COLUMNS as FARE_COLUMNS

    quarters = [(y, q) for y in (2022, 2023, 2024) for q in (1, 2, 3, 4)]
    fare_csv = write_csv(tmp_path / "fares.csv", FARE_COLUMNS, fare_rows(quarters, seed=4))
    flights = pipeline.load_and_standardize(
        spark, write_csv(tmp_path / "flights.csv", COLUMNS, random_rows(8000, 2024, seed=9))).cache()
    locations = {"fare_model": tmp_path / "model", "summaries": tmp_path / "powerbi",
                 "price_data": tmp_path / "price-data-usa.js"}

    result = fares.run(spark, "usa", fare_csv, flights, locations, fast=True)

    assert result["test_period"] == "2024Q1-2024Q4"
    assert {r["name"] for r in result["results"]} == set(fares.candidate_models(32))
    assert result["best"]["mape_pct"] < 10  # fares follow distance and season closely
    low, high = result["interval"]
    assert 0 < low <= 1 <= high
    for name in ("fare_model_metrics.csv", "fare_feature_importance.csv", "fare_forecast.csv", "fare_trend.csv"):
        assert (tmp_path / "powerbi" / name).stat().st_size > 0, name
    assert PipelineModel.load(str(tmp_path / "model")).stages

    data = read_registered_js(tmp_path / "price-data-usa.js", "PRICE_DATASETS", "usa")
    assert data["dataset"]["currency"] == "USD"
    assert data["quarters"] == [[2025, 1], [2025, 2], [2025, 3], [2025, 4]]  # the 4 quarters after the data
    cols = data["pairs"]["cols"]
    assert cols == ["a", "b", "carrier", "2025Q1", "2025Q2", "2025Q3", "2025Q4", "last_year"]
    rows = [dict(zip(cols, r)) for r in data["pairs"]["rows"]]
    assert all(r["a"] < r["b"] for r in rows)
    assert {r["carrier"] for r in rows} >= {"ALL", "DL"}
    assert all(r[q] > 0 for r in rows for q in cols[3:7])
    # Longer pairs cost more, and the summer quarter more than spring, as in the training data.
    market = {(r["a"], r["b"]): r for r in rows if r["carrier"] == "ALL"}
    jfk_lax, atl_mco = market[("JFK", "LAX")], market[("ATL", "MCO")]
    miles = {(a, b): m for a, b, m in ((r[3], r[2], r[6]) for r in fare_rows([(2024, 1)], 0))}
    longer, shorter = sorted([jfk_lax, atl_mco], key=lambda r: -miles[(r["a"], r["b"])])
    assert longer["2025Q1"] > shorter["2025Q1"]
    assert sum(r["2025Q3"] > r["2025Q2"] for r in market.values()) > 0.8 * len(market)


def test_india_fares_are_reproducible_and_priced_by_airline():
    import pandas as pd
    import make_india_fares as india_fares
    from download_fare_data import COLUMNS as FARE_COLUMNS

    mix = pd.DataFrame({"a": ["BOM", "BOM", "BOM", "IXL", "BLR"], "b": ["DEL", "DEL", "DEL", "DEL", "MAA"],
                        "Reporting_Airline": ["6E", "AI", "QP", "6E", "6E"], "flights": [600, 300, 100, 50, 400]})
    frame = india_fares.generate(mix, (2022, 1), (2023, 4), seed=1)
    assert frame.equals(india_fares.generate(mix, (2022, 1), (2023, 4), seed=1))
    assert list(frame.columns) == FARE_COLUMNS
    assert len(frame) == 3 * 8
    bom_del = frame[(frame.airport_1 == "BOM") & (frame.airport_2 == "DEL")]
    assert (bom_del.carrier_lg == "6E").all()
    # Akasa only flies from 2022Q3, so it cannot be the low-fare airline before then.
    early = bom_del[(bom_del.year == 2022) & (bom_del.quarter <= 2)]
    assert not (early.carrier_low == "QP").any()
    assert ((frame.fare > 0) & (frame.fare_low <= frame.fare_lg * 1.2)).all()
    # Delhi-Mumbai (about 700 miles) costs more than Bengaluru-Chennai (about 180 miles).
    blr_maa = frame[(frame.airport_1 == "BLR") & (frame.airport_2 == "MAA")]
    assert bom_del.fare.mean() > blr_maa.fare.mean() * 1.5
    assert 680 < india_fares.great_circle_miles("BOM", "DEL") < 730


# --- Trip cost ---------------------------------------------------------------------

def booking_inputs():
    """A small route mix and fare anchors on the sample airports, for the booking generator."""
    import pandas as pd
    import make_booking_data as booking_data

    mix = pd.DataFrame({"origin": ["ATL", "LAX", "JFK", "LAX", "ATL", "ORD"],
                        "destination": ["LAX", "ATL", "LAX", "JFK", "ORD", "ATL"],
                        "airline": ["DL", "DL", "NK", "DL", "WN", "DL"], "flights": [900, 900, 400, 700, 500, 300]})
    anchors = pd.DataFrame([{"a": a, "b": b, "quarter": q, "nsmiles": miles, "fare": fare, "carrier_lg": "DL",
                             "fare_lg": fare * 1.05, "carrier_low": "NK", "fare_low": fare * 0.6}
                            for a, b, miles, fare in (("ATL", "LAX", 1946, 320.0), ("JFK", "LAX", 2475, 380.0),
                                                      ("ATL", "ORD", 606, 190.0)) for q in (1, 2, 3, 4)])
    return booking_data.priced_routes(mix, anchors)


def test_bookings_are_reproducible_and_add_up():
    import pandas as pd
    import make_booking_data as booking_data

    routes, anchor = booking_inputs()
    assert len(routes) == 6 and anchor.shape == (6, 4)
    args = (routes, anchor, "usa", 6000, date(2025, 9, 1), date(2026, 8, 31), 7)
    frame = pd.concat(booking_data.generate(*args, dirty=False), ignore_index=True)
    assert frame.equals(pd.concat(booking_data.generate(*args, dirty=False), ignore_index=True))
    assert list(frame.columns) == booking_data.COLUMNS and len(frame) == 6000
    legs = frame.passengers * frame.trip_type.map({"ONE_WAY": 1, "ROUND_TRIP": 2})
    assert ((frame.ticket_fare * legs + frame.baggage_fee + frame.seat_fee - frame.total_paid).abs() < 0.01).all()
    assert ((frame.taxes_fees > 15.30) & (frame.taxes_fees < frame.ticket_fare)).all()
    lead = (pd.to_datetime(frame.travel_date) - pd.to_datetime(frame.booking_date)).dt.days
    assert (lead == frame.days_before_departure).all() and lead.min() >= 0
    # Southwest and Spirit sell economy only; business fares include the bags.
    assert set(frame[frame.airline.isin(["WN", "NK"])].cabin) == {"Economy"}
    assert (frame[frame.cabin == "Business"].baggage_fee == 0).all()
    economy = frame[(frame.cabin == "Economy") & (frame.airline == "DL")]
    assert economy[lead[economy.index] <= 2].ticket_fare.mean() > 1.3 * economy[lead[economy.index] >= 21].ticket_fare.mean()
    dirty = pd.concat(booking_data.generate(*args), ignore_index=True)
    assert len(dirty) > len(frame) and dirty.booking_id.duplicated().any() and (dirty.ticket_fare < 0).any()


def test_trip_cost_cleans_bookings_and_recovers_the_factors(spark, tmp_path):
    import make_booking_data as booking_data
    import trip_cost_model as trip_cost

    routes, anchor = booking_inputs()
    csv_path = tmp_path / "bookings.csv"
    for i, frame in enumerate(booking_data.generate(routes, anchor, "usa", 40000, date(2024, 1, 1),
                                                    date(2024, 12, 31), seed=5, chunk=20000)):
        frame.to_csv(csv_path, index=False, mode="w" if i == 0 else "a", header=i == 0)
    flights = pipeline.load_and_standardize(
        spark, write_csv(tmp_path / "flights.csv", COLUMNS, random_rows(8000, 2024, seed=9)))
    locations = {"bookings_processed": tmp_path / "bookings_clean", "summaries": tmp_path / "powerbi",
                 "cost_data": tmp_path / "cost-data-usa.js"}

    result = trip_cost.run(spark, "usa", csv_path, flights, locations)

    quality = result["quality"]
    assert quality["raw_rows"] == 40000 + 2 * 15          # each chunk breaks 60 rows; 15 of them are added duplicates
    assert quality["clean_rows"] == 40000 - 2 * 45        # the other 3 kinds of broken row each lose a booking
    assert {quality[k] for k in ("missing_route_or_airline", "invalid_dates", "invalid_amounts",
                                 "duplicate_rows")} == {30}
    assert list((tmp_path / "bookings_clean").glob("year_month=*/*.parquet"))
    for name in trip_cost.CSV_TABLES.values():
        assert (tmp_path / "powerbi" / name).stat().st_size > 0, name

    data = read_registered_js(tmp_path / "cost-data-usa.js", "COST_DATASETS", "usa")
    assert data["bookings"] == quality["clean_rows"] and data["dataset"]["currency"] == "USD"
    assert data["cabins"] == ["Economy", "Premium Economy", "Business"]
    economy, business = data["fare_index"]["Economy"], data["fare_index"]["Business"]
    assert economy[0] > economy[2] > economy[4]                      # late bookings cost more
    assert all(b > e for b, e in zip(business, economy))             # business costs more than economy
    days = data["weekday_index"]
    assert days[1] < days[4] < days[6]                               # Tue < Fri < Sun, as generated
    fixed, rate = data["taxes"]["Economy"]
    assert fixed == pytest.approx(15.30 / 1.075, abs=0.3) and rate == pytest.approx(0.075 / 1.075, abs=0.002)
    assert data["extras"]["DL"]["one_bag"] == pytest.approx(35, abs=1.5)
    assert data["extras"]["NK"]["one_bag"] > data["extras"]["DL"]["one_bag"]
    assert data["airline_cabins"]["NK"] == ["Economy"]
    pairs = {(r[0], r[1]): r for r in data["pairs"]["rows"]}
    assert pairs[("ATL", "LAX")][2] == 1946 and set(pairs) == {("ATL", "LAX"), ("JFK", "LAX"), ("ATL", "ORD")}
    # Routes are joined with the delay figures of the same route in the flight records.
    route = {r["route"]: r for r in result["tables"]["routes"].collect()}["ATL-LAX"]
    assert route["flights"] > 0 and route["cost_with_delay"] >= route["cost_per_traveller_direction"]

    # The calculation: 2 travellers, round trip, one bag each, booked 3-6 days ahead.
    q = trip_cost.quote(data, 300.0, airline="DL", lead="3-6", travellers=2, round_trip=True, bags=1,
                        late_pct=20, late_avg_min=60, value_of_time=30)
    assert q["ticket"] == pytest.approx(300 * economy[1] * 4, abs=0.01)
    assert q["baggage"] == pytest.approx(data["extras"]["DL"]["one_bag"] * 4, abs=0.01)
    assert q["total"] == pytest.approx(q["ticket"] + q["baggage"], abs=0.01) and q["per_traveller"] == pytest.approx(q["total"] / 2, abs=0.01)
    assert 0 < q["taxes_included"] < q["ticket"]
    assert q["expected_delay_hours"] == pytest.approx(0.2 * 4) and q["delay_time_cost"] == pytest.approx(24)
    included = trip_cost.quote(data, 300.0, airline="DL", cabin="Business", bags=2, seat=True)
    assert included["baggage"] == 0 and included["seats"] == 0 and included["total"] > q["total"] / 4
    with pytest.raises(FileNotFoundError):
        trip_cost.load_bookings(spark, tmp_path / "missing.csv")


def test_dataset_paths_keep_usa_layout(tmp_path):
    import datasets

    usa, india = datasets.paths("usa", tmp_path), datasets.paths("india", tmp_path)
    assert usa["input"] == tmp_path / "data" / "raw" / "flights.csv"
    assert usa["summaries"] == tmp_path / "output" / "powerbi"
    assert india["input"] == tmp_path / "data" / "raw" / "india" / "flights.csv"
    assert india["summaries"] == tmp_path / "output" / "india" / "powerbi"
    assert india["website_data"].name == "site-data-india.js"
    with pytest.raises(ValueError):
        datasets.paths("mars", tmp_path)


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

    data = read_registered_js(dash / "data-usa.js", "DASHBOARD_DATASETS", "usa")
    assert data["kpi"]["cleaned_records"] == 1500
    assert len(data["monthly"]) == 12 and data["routes"]
    assert data["meta"]["option"] == "International"

    site_data = read_registered_js(tmp_path / "website" / "data" / "site-data-usa.js", "SITE_DATASETS", "usa")
    assert site_data["overall"]["flights"] == 1500 and site_data["airline_names"]["DL"] == "Delta Air Lines"
    assert site_data["dataset"]["country"] == "USA"
