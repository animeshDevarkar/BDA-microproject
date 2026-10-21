"""Build the data file for the customer website (website/index.html).

The website answers traveller questions ("how often is my flight late?"), so it
needs finer-grained aggregates than the Power BI summaries: route x airline,
departure hour per airport and month per airport. They are computed with Spark
and written as one compact JavaScript file per dataset, website/data/site-data-<dataset>.js,
which the static site loads (works from disk and on any static host).

The pipeline calls this automatically. To rebuild from the saved Parquet only:

    python src/build_website_data.py                  # International (USA)
    python src/build_website_data.py --dataset india  # Domestic (India)
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

import datasets
import spark_env

spark_env.configure()

from pyspark.sql import DataFrame  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]

# Smallest groups the site will show. Below these, a rate is too noisy to quote to a customer.
MIN_AIRPORT_FLIGHTS = 100
MIN_ROUTE_FLIGHTS = 30
MIN_CELL_FLIGHTS = 20  # route x airline, airport x hour, airport x month


def _rate(numerator: str, denominator: str):
    return F.round(F.when(F.col(denominator) > 0, F.col(numerator) * 100.0 / F.col(denominator)), 1)


def _outcomes(*extra):
    """Aggregations shared by every table. Rates use the same definitions as the pipeline."""
    operated = (F.col("cancelled") == 0) & (F.col("diverted") == 0) & F.col("arrival_delay").isNotNull()
    late = operated & (F.col("arrival_delayed_15") == 1)
    return [
        F.count("*").alias("flights"),
        F.sum("cancelled").alias("cancelled"),
        F.sum(F.when(operated, 1).otherwise(0)).alias("operated"),
        F.sum(F.when(late, 1).otherwise(0)).alias("late"),
        F.avg(F.when(late, F.col("arrival_delay"))).alias("late_avg"),
        *extra,
    ]


def _finish(df: DataFrame, keys: list[str], min_flights: int, extra: Optional[list[str]] = None) -> DataFrame:
    return (
        df.filter(F.col("flights") >= min_flights)
        .select(
            *keys,
            "flights",
            _rate("late", "operated").alias("late_pct"),
            _rate("cancelled", "flights").alias("cancel_pct"),
            F.round("late_avg", 0).cast("int").alias("late_avg_min"),
            *(extra or []),
        )
        .orderBy(*keys)
    )


def _table(df: DataFrame) -> dict:
    """Columnar layout ({"cols": [...], "rows": [[...], ...]}) keeps the file small."""
    return {"cols": df.columns, "rows": [list(row) for row in df.collect()]}


def build_tables(flights: DataFrame) -> dict:
    df = flights.withColumn("month", F.date_format("flight_date", "yyyy-MM"))
    departures = df.groupBy("origin").agg(*_outcomes(
        F.first("origin_city", ignorenulls=True).alias("city"),
        F.sum("departure_delayed_15").alias("dep_late"),
        F.sum(F.when(F.col("cancelled") == 0, 1).otherwise(0)).alias("departed"),
    ))
    arrivals = df.groupBy(F.col("destination").alias("origin")).agg(
        F.first("destination_city", ignorenulls=True).alias("arr_city"))
    airports = (
        departures.join(arrivals, "origin", "left")
        .withColumn("city", F.coalesce("city", "arr_city"))
        .withColumn("dep_late_pct", _rate("dep_late", "departed"))
        .withColumnRenamed("origin", "code")
    )
    airports = _finish(airports, ["code"], MIN_AIRPORT_FLIGHTS, ["city", "dep_late_pct"])

    tables = {
        "airports": airports,
        "airlines": _finish(df.groupBy("airline").agg(*_outcomes()), ["airline"], MIN_AIRPORT_FLIGHTS),
        "routes": _finish(df.groupBy("origin", "destination").agg(*_outcomes()),
                          ["origin", "destination"], MIN_ROUTE_FLIGHTS),
        "route_airlines": _finish(df.groupBy("origin", "destination", "airline").agg(*_outcomes()),
                                  ["origin", "destination", "airline"], MIN_CELL_FLIGHTS),
        "airport_hours": _finish(
            df.filter(F.col("scheduled_departure_hour").between(0, 23))
            .groupBy("origin", F.col("scheduled_departure_hour").alias("hour")).agg(*_outcomes()),
            ["origin", "hour"], MIN_CELL_FLIGHTS),
        "airport_months": _finish(df.groupBy("origin", "month").agg(*_outcomes()),
                                  ["origin", "month"], MIN_CELL_FLIGHTS),
        "airport_weekdays": _finish(
            df.filter(F.col("day_of_week").between(1, 7))
            .groupBy("origin", F.col("day_of_week").alias("weekday")).agg(*_outcomes()),
            ["origin", "weekday"], MIN_CELL_FLIGHTS),
        "months": _finish(df.groupBy("month").agg(*_outcomes()), ["month"], 1),
        "hours": _finish(df.filter(F.col("scheduled_departure_hour").between(0, 23))
                         .groupBy(F.col("scheduled_departure_hour").alias("hour")).agg(*_outcomes()),
                         ["hour"], 1),
    }
    data = {name: _table(table) for name, table in tables.items()}

    overall = df.agg(*_outcomes(F.min("flight_date").alias("first"), F.max("flight_date").alias("last"))).first()
    data["overall"] = {
        "flights": overall["flights"],
        "late_pct": round(overall["late"] * 100.0 / overall["operated"], 1) if overall["operated"] else None,
        "cancel_pct": round(overall["cancelled"] * 100.0 / overall["flights"], 1) if overall["flights"] else None,
        "late_avg_min": round(overall["late_avg"]) if overall["late_avg"] is not None else None,
        "first_date": overall["first"].isoformat() if overall["first"] else None,
        "last_date": overall["last"].isoformat() if overall["last"] else None,
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "min_flights": {"airport": MIN_AIRPORT_FLIGHTS, "route": MIN_ROUTE_FLIGHTS, "cell": MIN_CELL_FLIGHTS},
    }
    return data


def write_site_data(flights: DataFrame, target: Path, dataset: str = datasets.DEFAULT) -> Path:
    from flight_delay_pipeline import AIRLINE_NAMES

    info = datasets.DATASETS[dataset]
    data = build_tables(flights)
    data["airline_names"] = AIRLINE_NAMES
    data["dataset"] = {"key": info.key, "option": info.option, "country": info.country,
                       "region_name": info.region_name, "source": info.source_name,
                       "simulated": info.simulated}
    target = Path(target)
    target.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(data, separators=(",", ":"), allow_nan=False)
    # Each dataset registers itself, so the site can load both files and switch between them.
    target.write_text("window.SITE_DATASETS = window.SITE_DATASETS || {};\n"
                      f"window.SITE_DATASETS[{json.dumps(info.key)}] = {payload};\n", encoding="utf-8")
    print(f"Website data written: {target} ({len(payload) / 1024:.0f} KB)")
    return target


def main(argv: Optional[list[str]] = None) -> int:
    from flight_delay_pipeline import build_spark

    parser = argparse.ArgumentParser(description="Build website/data/site-data-<dataset>.js from the cleaned Parquet")
    parser.add_argument("--dataset", choices=list(datasets.DATASETS), default=datasets.DEFAULT)
    parser.add_argument("--processed", help="Cleaned Parquet directory (default: the dataset's)")
    parser.add_argument("--output", help="Target .js file (default: the dataset's)")
    parser.add_argument("--driver-memory", default="4g", help="Spark driver memory (default 4g)")
    args = parser.parse_args(argv)

    locations = datasets.paths(args.dataset, ROOT)
    processed = Path(args.processed or locations["processed"])
    if not processed.exists():
        raise FileNotFoundError(f"{processed} not found. Run python src/flight_delay_pipeline.py "
                                f"--dataset {args.dataset} first.")
    spark = build_spark(args.driver_memory)
    spark.sparkContext.setLogLevel("ERROR")
    try:
        write_site_data(spark.read.parquet(str(processed)), Path(args.output or locations["website_data"]),
                        args.dataset)
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
