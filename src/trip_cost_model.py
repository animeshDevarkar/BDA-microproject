"""Trip cost - Spark job that learns how the cost of a trip is built up from ticket bookings.

Input: data/raw/bookings.csv (millions of simulated ticket bookings, see make_booking_data.py)
and the cleaned flight Parquet written by the main pipeline.

Steps:
1. read and clean the bookings (bad dates, missing airports, impossible amounts, duplicates)
   and store them as Parquet, partitioned by travel month;
2. fare index: each ticket fare divided by the average fare of the same airport pair and airline
   (a Spark window), then averaged by cabin and by days before departure, and by weekday -
   this is how much a cabin, a late booking or a Friday changes the price;
3. taxes: a least-squares line per cabin (taxes = fixed + rate x fare) from covariance aggregates;
4. extras: baggage and seat fees per airline, and the share of bookings that pay them;
5. routes: average cost per traveller and direction, joined with the late-arrival rate and the
   typical delay of the same route in the flight records, to price the expected delay time.

The trip cost calculator on the website multiplies the fare forecast of fare_price_model.py by
these factors and adds the extras. ``quote`` below is the same calculation in Python.

Outputs:
    data/processed/bookings_clean/           cleaned bookings (Parquet, partitioned by year_month)
    output/powerbi/trip_cost_*.csv           summary, fare factors, weekday, extras, routes, monthly
    website/data/cost-data-<dataset>.js      factors for the OnTime Check trip cost calculator

    python src/trip_cost_model.py                    # International (USA)
    python src/trip_cost_model.py --dataset india    # Domestic (India, simulated)
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

import datasets
import spark_env

spark_env.configure()

from pyspark.sql import DataFrame, SparkSession, Window  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql.types import DoubleType, IntegerType, LongType  # noqa: E402

from flight_delay_pipeline import AIRLINE_NAMES, build_spark, lookup, plain_numbers, round_doubles, write_csv  # noqa: E402
from make_booking_data import CABINS, COLUMNS, RULES  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]

ALL = "ALL"                    # pseudo-airline: every airline together
MIN_GROUP_BOOKINGS = 30        # pair x airline bookings needed for a fare index
MIN_CELL_BOOKINGS = 30         # bookings needed to quote a factor or a fee
MIN_ROUTE_BOOKINGS = 50
TRIP_TYPES = {"ONE_WAY": 1, "ROUND_TRIP": 2}
# (key, first day, last day, label) - days between booking and departure
LEAD_BUCKETS = [("0-2", 0, 2, "0–2 days"), ("3-6", 3, 6, "3–6 days"), ("7-13", 7, 13, "1–2 weeks"),
                ("14-20", 14, 20, "2–3 weeks"), ("21-59", 21, 59, "3–8 weeks"), ("60+", 60, None, "2 months or more")]
# Value of one hour of a traveller's time, used to price the expected delay. An assumption the
# website lets the visitor change; not taken from the data.
VALUE_OF_TIME = {"usa": 30.0, "india": 400.0}

log = logging.getLogger("trip_cost_model")


# ---------------------------------------------------------------- load and clean
def load_bookings(spark: SparkSession, path: Path) -> tuple[DataFrame, dict[str, int]]:
    """Typed, cleaned bookings with derived fields, and a count of the rows removed by each rule."""
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Booking data not found: {path}. Run python src/make_booking_data.py first.")
    raw = spark.read.option("header", "true").option("mode", "PERMISSIVE").csv(str(path))
    missing = [c for c in COLUMNS if c not in raw.columns]
    if missing:
        raise ValueError("Booking CSV is missing columns: " + ", ".join(missing))

    def code(name):
        return F.upper(F.trim(F.col(name)))

    def amount(name):
        return F.col(name).cast(DoubleType())

    typed = raw.select(
        F.col("booking_id").cast(LongType()).alias("booking_id"),
        F.to_date("booking_date").alias("booking_date"), F.to_date("travel_date").alias("travel_date"),
        code("origin").alias("origin"), code("destination").alias("destination"), code("airline").alias("airline"),
        F.trim("cabin").alias("cabin"), code("trip_type").alias("trip_type"),
        F.col("passengers").cast(IntegerType()).alias("passengers"),
        F.col("bags_each").cast(IntegerType()).alias("bags_each"),
        F.col("paid_seats").cast(IntegerType()).alias("paid_seats"),
        amount("distance_miles").alias("distance_miles"), amount("ticket_fare").alias("ticket_fare"),
        amount("taxes_fees").alias("taxes_fees"), amount("baggage_fee").alias("baggage_fee"),
        amount("seat_fee").alias("seat_fee"), amount("total_paid").alias("total_paid"),
    )

    def blank(name):
        return F.col(name).isNull() | (F.col(name) == "")

    def bad(condition):  # a null comparison counts as bad
        return F.coalesce(~condition, F.lit(True))

    reason = (
        F.when(blank("origin") | blank("destination") | blank("airline") | (F.col("origin") == F.col("destination")),
               "missing_route_or_airline")
        .when(bad(F.col("travel_date") >= F.col("booking_date")), "invalid_dates")
        .when(bad((F.col("ticket_fare") > 0) & (F.col("total_paid") > 0) & (F.col("taxes_fees") >= 0)
                  & (F.col("taxes_fees") < F.col("ticket_fare")) & (F.col("baggage_fee") >= 0)
                  & (F.col("seat_fee") >= 0)), "invalid_amounts")
        .when(bad(F.col("passengers").between(1, 9) & F.col("cabin").isin(CABINS)
                  & F.col("trip_type").isin(list(TRIP_TYPES)) & F.col("bags_each").between(0, 2)
                  & F.col("paid_seats").between(0, 9) & F.col("booking_id").isNotNull()), "invalid_booking")
    )
    flagged = typed.withColumn("reject_reason", reason).cache()
    quality = {"raw_rows": flagged.count()}
    for row in flagged.filter(F.col("reject_reason").isNotNull()).groupBy("reject_reason").count().collect():
        quality[row["reject_reason"]] = row["count"]
    valid = flagged.filter(F.col("reject_reason").isNull()).drop("reject_reason")

    first = F.col("origin") < F.col("destination")
    lead = F.datediff("travel_date", "booking_date")
    bucket = F.when(lead <= LEAD_BUCKETS[0][2], LEAD_BUCKETS[0][0])
    for key, _, last, _ in LEAD_BUCKETS[1:-1]:
        bucket = bucket.when(lead <= last, key)
    directions = F.when(F.col("trip_type") == "ROUND_TRIP", 2).otherwise(1)
    clean = (
        valid.dropDuplicates(["booking_id"])
        .withColumn("lead_days", lead)
        .withColumn("lead_bucket", bucket.otherwise(LEAD_BUCKETS[-1][0]))
        .withColumn("directions", directions)
        .withColumn("legs", F.col("passengers") * directions)          # traveller-directions paid for
        .withColumn("weekday", (F.dayofweek("travel_date") + 5) % 7 + 1)  # 1 = Monday ... 7 = Sunday
        .withColumn("year_month", F.date_format("travel_date", "yyyy-MM"))
        .withColumn("airport_a", F.when(first, F.col("origin")).otherwise(F.col("destination")))
        .withColumn("airport_b", F.when(first, F.col("destination")).otherwise(F.col("origin")))
        .withColumn("cost_per_leg", F.col("total_paid") / F.col("legs"))
        .withColumn("extras_per_leg", (F.col("baggage_fee") + F.col("seat_fee")) / F.col("legs"))
    ).cache()
    quality["clean_rows"] = clean.count()
    quality["duplicate_rows"] = (quality["raw_rows"] - quality["clean_rows"]
                                 - sum(v for k, v in quality.items() if k not in ("raw_rows", "clean_rows")))
    flagged.unpersist()
    return clean, quality


def with_fare_index(bookings: DataFrame, min_group: int = MIN_GROUP_BOOKINGS) -> DataFrame:
    """fare_index = ticket fare / average fare of the same airport pair and airline (null for small groups)."""
    group = Window.partitionBy("airport_a", "airport_b", "airline")
    return (bookings.withColumn("_group_fare", F.avg("ticket_fare").over(group))
            .withColumn("_group_size", F.count("*").over(group))
            .withColumn("fare_index", F.when(F.col("_group_size") >= min_group,
                                             F.col("ticket_fare") / F.col("_group_fare")))
            .drop("_group_fare", "_group_size"))


def route_delays(flights: DataFrame) -> DataFrame:
    """Late-arrival rate and typical delay per directional route, from the flight records."""
    operated = (F.col("cancelled") == 0) & (F.col("diverted") == 0) & F.col("arrival_delay").isNotNull()
    late = operated & (F.col("arrival_delayed_15") == 1)
    return flights.groupBy("origin", "destination").agg(
        F.count("*").alias("flights"),
        (F.avg(F.when(operated, F.col("arrival_delayed_15"))) * 100.0).alias("late_pct"),
        F.avg(F.when(late, F.col("arrival_delay"))).alias("late_avg_min"),
    )


# ---------------------------------------------------------------- aggregates
def _line(y: str, x: str) -> tuple:
    """Least-squares slope and intercept of y on x as Spark aggregates (slope 0 when x does not vary)."""
    paired = F.when(F.col(y).isNotNull(), F.col(x))  # only the rows that have a y
    slope = F.coalesce(F.when(F.var_samp(paired) > 0, F.covar_samp(x, y) / F.var_samp(paired)), F.lit(0.0))
    return slope, F.avg(y) - slope * F.avg(paired)


def cost_tables(bookings: DataFrame, flights: Optional[DataFrame], dataset: str, quality: dict[str, int],
                min_group: int = MIN_GROUP_BOOKINGS, min_cell: int = MIN_CELL_BOOKINGS,
                min_route: int = MIN_ROUTE_BOOKINGS) -> dict[str, DataFrame]:
    spark = bookings.sparkSession
    indexed = with_fare_index(bookings, min_group).cache()

    factors = (
        indexed.groupBy("cabin", "lead_bucket")
        .agg(F.count("*").alias("bookings"), F.avg("fare_index").alias("fare_index"),
             F.avg("ticket_fare").alias("average_ticket_fare"),
             F.expr("percentile_approx(ticket_fare, 0.5)").alias("median_ticket_fare"),
             F.avg("lead_days").alias("average_days_before_departure"))
        .filter(F.col("bookings") >= min_cell)
        .withColumn("lead_order", lookup({k: i for i, (k, *_) in enumerate(LEAD_BUCKETS)}, "lead_bucket"))
        .withColumn("cabin_order", lookup({c: i for i, c in enumerate(CABINS)}, "cabin"))
        .orderBy("cabin_order", "lead_order").drop("cabin_order", "lead_order")
    )

    weekday = (
        indexed.groupBy("weekday")
        .agg(F.count("*").alias("bookings"), F.avg("fare_index").alias("fare_index"),
             F.avg("ticket_fare").alias("average_ticket_fare"))
        .withColumn("day", lookup({1: "Mon", 2: "Tue", 3: "Wed", 4: "Thu", 5: "Fri", 6: "Sat", 7: "Sun"}, "weekday"))
        .orderBy("weekday")
    )

    tax_rate, tax_fixed = _line("taxes_fees", "ticket_fare")
    taxes = (
        bookings.groupBy("cabin")
        .agg(F.count("*").alias("bookings"), tax_fixed.alias("tax_fixed"), tax_rate.alias("tax_rate"),
             (F.sum("taxes_fees") / F.sum("ticket_fare") * 100.0).alias("tax_share_pct"))
        .filter(F.col("bookings") >= min_cell)
    )

    # Extras are priced on economy and premium economy; business fares include bags and seats.
    per_leg_bags = F.col("baggage_fee") / F.col("legs")
    per_seat = F.when(F.col("paid_seats") > 0, F.col("seat_fee") / (F.col("paid_seats") * F.col("directions")))
    seat_rate, seat_fixed = _line("_seat", "distance_miles")
    paying = bookings.filter(F.col("cabin") != "Business").withColumn("_seat", per_seat)
    extras = (
        paying.rollup("airline")
        .agg(F.count("*").alias("bookings"),
             (F.avg((F.col("bags_each") > 0).cast("double")) * 100.0).alias("bag_share_pct"),
             F.avg(F.when(F.col("bags_each") == 1, per_leg_bags)).alias("one_bag_fee"),
             F.avg(F.when(F.col("bags_each") == 2, per_leg_bags)).alias("two_bags_fee"),
             (F.avg((F.col("paid_seats") > 0).cast("double")) * 100.0).alias("seat_share_pct"),
             F.avg("_seat").alias("seat_fee"),
             seat_fixed.alias("seat_fee_fixed"), seat_rate.alias("seat_fee_per_mile"))
        .filter(F.col("bookings") >= min_cell)
        .withColumn("airline", F.coalesce("airline", F.lit(ALL)))
        .withColumn("airline_name", F.coalesce(lookup({**AIRLINE_NAMES, ALL: "All airlines"}, "airline"),
                                               F.col("airline")))
        .orderBy(F.desc("bookings"))
    )

    airline_cabins = (bookings.groupBy("airline", "cabin").agg(F.count("*").alias("bookings"))
                      .filter(F.col("bookings") >= min_cell))

    routes = (
        bookings.groupBy("origin", "destination")
        .agg(F.count("*").alias("bookings"), F.sum("passengers").alias("travellers"),
             F.max("distance_miles").alias("distance_miles"),
             F.avg("ticket_fare").alias("average_ticket_fare"), F.avg("extras_per_leg").alias("average_extras"),
             (F.sum("total_paid") / F.sum("legs")).alias("cost_per_traveller_direction"),
             (F.sum("taxes_fees") / F.sum("ticket_fare") * 100.0).alias("tax_share_pct"),
             (F.avg((F.col("trip_type") == "ROUND_TRIP").cast("double")) * 100.0).alias("round_trip_pct"))
        .filter(F.col("bookings") >= min_route)
    )
    value = VALUE_OF_TIME[dataset]
    if flights is not None:
        routes = (
            routes.join(route_delays(flights), ["origin", "destination"], "left")
            .withColumn("expected_delay_min", F.col("late_pct") / 100.0 * F.col("late_avg_min"))
            .withColumn("delay_time_cost", F.col("expected_delay_min") / 60.0 * F.lit(value))
            .withColumn("cost_with_delay", F.col("cost_per_traveller_direction")
                        + F.coalesce(F.col("delay_time_cost"), F.lit(0.0)))
        )
    routes = routes.withColumn("route", F.concat_ws("-", "origin", "destination")).orderBy(F.desc("bookings"))

    monthly = (
        bookings.groupBy("year_month")
        .agg(F.count("*").alias("bookings"), F.avg("ticket_fare").alias("average_ticket_fare"),
             (F.sum("total_paid") / F.sum("legs")).alias("cost_per_traveller_direction"),
             F.avg("lead_days").alias("average_days_before_departure"))
        .orderBy("year_month")
    )

    totals = bookings.agg(
        F.sum("passengers").alias("travellers"), F.sum("total_paid").alias("total_paid"),
        F.avg("total_paid").alias("average_booking_total"),
        (F.sum("total_paid") / F.sum("legs")).alias("cost_per_traveller_direction"),
        (F.sum("taxes_fees" ) / F.sum("ticket_fare") * 100.0).alias("tax_share_of_ticket_pct"),
        (F.sum(F.col("baggage_fee") + F.col("seat_fee")) / F.sum("total_paid") * 100.0).alias("extras_share_pct"),
        (F.avg((F.col("trip_type") == "ROUND_TRIP").cast("double")) * 100.0).alias("round_trip_pct"),
        F.expr("percentile_approx(lead_days, 0.5)").alias("median_days_before_departure"),
        F.countDistinct("origin", "destination").alias("routes"),
        F.min("travel_date").cast("string").alias("first_travel_date"),
        F.max("travel_date").cast("string").alias("last_travel_date"),
    ).first().asDict()
    dropped = {k: v for k, v in quality.items() if k not in ("raw_rows", "clean_rows")}
    metrics = ([("raw_rows", str(quality["raw_rows"])), ("clean_bookings", str(quality["clean_rows"]))]
               + [(f"removed_{k}", str(v)) for k, v in sorted(dropped.items())]
               + [(k, f"{v:.2f}" if isinstance(v, float) else str(v)) for k, v in totals.items()]
               + [("currency", RULES[dataset]["currency"]), ("value_of_time_per_hour", f"{value:.2f}")])
    summary = spark.createDataFrame(metrics, "metric string, value string")

    return {"summary": summary, "factors": factors, "weekday": weekday, "taxes": taxes, "extras": extras,
            "airline_cabins": airline_cabins, "routes": routes, "monthly": monthly}


CSV_TABLES = {"summary": "trip_cost_summary.csv", "factors": "trip_cost_fare_factors.csv",
              "weekday": "trip_cost_weekday.csv", "extras": "trip_cost_extras.csv",
              "routes": "trip_cost_routes.csv", "monthly": "trip_cost_monthly.csv"}


# ---------------------------------------------------------------- website data
def _num(value, digits: int = 2):
    return None if value is None else round(float(value), digits)


def build_cost_data(tables: dict[str, DataFrame], dataset: str) -> dict:
    """Small lookup tables for the website calculator (collected from the Spark aggregates)."""
    info = datasets.DATASETS[dataset]
    summary = {r["metric"]: r["value"] for r in tables["summary"].collect()}
    index = {c: [None] * len(LEAD_BUCKETS) for c in CABINS}
    position = {k: i for i, (k, *_) in enumerate(LEAD_BUCKETS)}
    for r in tables["factors"].collect():
        index[r["cabin"]][position[r["lead_bucket"]]] = _num(r["fare_index"], 4)
    weekday = [None] * 7
    for r in tables["weekday"].collect():
        weekday[r["weekday"] - 1] = _num(r["fare_index"], 4)
    cabins: dict[str, list[str]] = {}
    for r in tables["airline_cabins"].collect():
        cabins.setdefault(r["airline"], []).append(r["cabin"])
    pairs: dict[tuple, list] = {}
    for r in tables["routes"].collect():
        key = tuple(sorted((r["origin"], r["destination"])))
        cell = pairs.setdefault(key, [0.0, 0, 0.0])
        cell[0] = max(cell[0], r["distance_miles"] or 0.0)
        cell[1] += r["bookings"]
        cell[2] += r["cost_per_traveller_direction"] * r["bookings"]
    return {
        "dataset": {"key": info.key, "currency": info.currency, "simulated": True,
                    "source": "Simulated ticket bookings on the project's flight network",
                    "bag_unit": RULES[dataset]["bag_unit"]},
        "bookings": int(summary["clean_bookings"]), "raw_rows": int(summary["raw_rows"]),
        "lead_buckets": [{"key": k, "label": label} for k, _, _, label in LEAD_BUCKETS],
        "cabins": [c for c in CABINS if any(v is not None for v in index[c])],
        "fare_index": index, "weekday_index": weekday,
        "taxes": {r["cabin"]: [_num(r["tax_fixed"]), _num(r["tax_rate"], 4)] for r in tables["taxes"].collect()},
        "extras": {r["airline"]: {"bag_share_pct": _num(r["bag_share_pct"], 1), "one_bag": _num(r["one_bag_fee"]),
                                  "two_bags": _num(r["two_bags_fee"]), "seat_share_pct": _num(r["seat_share_pct"], 1),
                                  "seat": _num(r["seat_fee"]), "seat_fixed": _num(r["seat_fee_fixed"]),
                                  "seat_per_mile": _num(r["seat_fee_per_mile"], 5)}
                   for r in tables["extras"].collect()},
        "airline_cabins": {a: [c for c in CABINS if c in v] for a, v in sorted(cabins.items())},
        "pairs": {"cols": ["a", "b", "miles", "bookings", "cost_per_leg"],
                  "rows": [[a, b, round(m), n, round(total / n, 2)] for (a, b), (m, n, total) in sorted(pairs.items())]},
        "value_of_time": VALUE_OF_TIME[dataset],
        "summary": {k: summary[k] for k in ("cost_per_traveller_direction", "average_booking_total",
                                            "tax_share_of_ticket_pct", "extras_share_pct", "round_trip_pct",
                                            "median_days_before_departure", "first_travel_date", "last_travel_date")},
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M"),
    }


def write_cost_data(data: dict, target: Path) -> Path:
    target = Path(target)
    target.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(data, separators=(",", ":"), allow_nan=False)
    target.write_text("window.COST_DATASETS = window.COST_DATASETS || {};\n"
                      f"window.COST_DATASETS[{json.dumps(data['dataset']['key'])}] = {payload};\n", encoding="utf-8")
    log.info("Website cost data written: %s (%.0f KB)", target, len(payload) / 1024)
    return target


# ---------------------------------------------------------------- the calculation
def quote(cost: dict, fare: float, *, airline: Optional[str] = None, cabin: str = "Economy",
          lead: str = "21-59", weekday: Optional[int] = None, travellers: int = 1, round_trip: bool = False,
          bags: int = 0, seat: bool = False, miles: Optional[float] = None, late_pct: Optional[float] = None,
          late_avg_min: Optional[float] = None, value_of_time: Optional[float] = None) -> dict:
    """Cost of a trip from the average one-way ``fare`` of the route (the fare model's forecast).

    ticket   = fare x cabin-and-booking-date index x weekday index, per traveller and direction
    extras   = baggage and seat fees of the airline (or of all airlines), per traveller and direction
    total    = (ticket + extras) x travellers x directions; taxes are the part of the ticket that is tax
    delay    = late rate x typical delay x value of time, per traveller and direction (time, not cash)
    The website (website/app.js, tripCost) does the same sum.
    """
    if cabin not in cost["cabins"]:
        raise ValueError(f"Unknown cabin {cabin!r}; choose from {cost['cabins']}")
    position = next(i for i, b in enumerate(cost["lead_buckets"]) if b["key"] == lead)
    index = cost["fare_index"][cabin][position]
    if index is None:
        raise ValueError(f"Too few {cabin} bookings {lead} days ahead to price")
    day = cost["weekday_index"][weekday - 1] if weekday else 1.0
    legs = travellers * (2 if round_trip else 1)
    ticket = fare * index * (day or 1.0)
    fixed, rate = cost["taxes"].get(cabin, [0.0, 0.0])
    taxes = max(0.0, min(ticket, fixed + rate * ticket))
    extras = cost["extras"].get(airline) or cost["extras"].get(ALL) or {}
    included = cabin == "Business"   # business fares include bags and seats
    bag_fee = 0.0 if included or not bags else (extras.get("one_bag" if bags == 1 else "two_bags") or 0.0)
    seat_fee = 0.0
    if seat and not included:
        if miles is not None and extras.get("seat_fixed") is not None:
            seat_fee = max(0.0, extras["seat_fixed"] + (extras.get("seat_per_mile") or 0.0) * miles)
        else:
            seat_fee = extras.get("seat") or 0.0
    hours = (late_pct or 0.0) / 100.0 * (late_avg_min or 0.0) / 60.0
    hourly = cost["value_of_time"] if value_of_time is None else value_of_time
    parts = {"ticket": ticket * legs, "taxes_included": taxes * legs, "baggage": bag_fee * legs,
             "seats": seat_fee * legs}
    parts["total"] = parts["ticket"] + parts["baggage"] + parts["seats"]
    parts["per_traveller"] = parts["total"] / travellers
    parts["expected_delay_hours"] = hours * legs
    parts["delay_time_cost"] = hours * legs * hourly
    return {k: round(v, 2) for k, v in parts.items()}


# ---------------------------------------------------------------- run
def run(spark: SparkSession, dataset: str, bookings_path: Path, flights: Optional[DataFrame], locations: dict,
        min_group: int = MIN_GROUP_BOOKINGS, min_cell: int = MIN_CELL_BOOKINGS,
        min_route: int = MIN_ROUTE_BOOKINGS) -> dict:
    bookings, quality = load_bookings(spark, bookings_path)
    removed = quality["raw_rows"] - quality["clean_rows"]
    log.info("Bookings read: %s, removed by cleaning: %s (%s), clean: %s", f"{quality['raw_rows']:,}",
             f"{removed:,}", ", ".join(f"{k} {v:,}" for k, v in sorted(quality.items())
                                       if k not in ("raw_rows", "clean_rows")), f"{quality['clean_rows']:,}")
    processed = locations.get("bookings_processed")
    if processed:
        bookings.write.mode("overwrite").partitionBy("year_month").parquet(str(processed))
        log.info("Created Parquet dataset (partitioned by year_month): %s", processed)
    tables = cost_tables(bookings, flights, dataset, quality, min_group, min_cell, min_route)
    for key, filename in CSV_TABLES.items():
        if key == "summary":
            table = tables[key]
        elif key in ("factors", "weekday", "extras"):   # small ratios: keep 4 decimals
            table = round_doubles(tables[key], 4)
        else:
            table = plain_numbers(round_doubles(tables[key]))
        write_csv(table, locations["summaries"] / filename)
        log.info("Created: %s", locations["summaries"] / filename)
    data = build_cost_data(tables, dataset)
    write_cost_data(data, locations["cost_data"])
    return {"quality": quality, "data": data, "tables": tables}


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Learn trip cost factors from ticket bookings with Spark")
    parser.add_argument("--dataset", choices=list(datasets.DATASETS), default=datasets.DEFAULT)
    parser.add_argument("--bookings", help="Bookings CSV (default: the dataset's bookings.csv)")
    parser.add_argument("--processed", help="Cleaned flight Parquet (default: the dataset's)")
    parser.add_argument("--driver-memory", default="6g", help="Spark driver memory (default 6g)")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
    locations = datasets.paths(args.dataset, ROOT)
    processed = Path(args.processed or locations["processed"])
    started = time.time()
    spark = build_spark(args.driver_memory)
    spark.sparkContext.setLogLevel("ERROR")
    try:
        flights = spark.read.parquet(str(processed)) if processed.exists() else None
        if flights is None:
            log.warning("%s not found: route costs will have no delay columns.", processed)
        result = run(spark, args.dataset, Path(args.bookings or locations["bookings_input"]), flights, locations)
    finally:
        spark.stop()
    s = result["data"]["summary"]
    log.info("Average cost per traveller and direction: %s %s (taxes %s%% of the ticket, extras %s%% of the total)",
             result["data"]["dataset"]["currency"], s["cost_per_traveller_direction"],
             s["tax_share_of_ticket_pct"], s["extras_share_pct"])
    log.info("Trip cost model finished in %.0f s", time.time() - started)
    return 0


if __name__ == "__main__":
    sys.exit(main())
