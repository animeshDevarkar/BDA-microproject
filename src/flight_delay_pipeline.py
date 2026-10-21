"""Flight Delay Analysis & Airport Network Bottlenecks - PySpark pipeline.

Reads BTS on-time performance records, standardizes and cleans them, derives
features (flight date, route, scheduled hour, 15-minute delay flags), writes the
cleaned records to Parquet and exports small summary CSVs for Power BI and the
web dashboard.

Usage:
    python src/flight_delay_pipeline.py                    # International (USA) data
    python src/flight_delay_pipeline.py --dataset india    # Domestic (India) data
    python src/flight_delay_pipeline.py --input data/raw/flights.csv --output output/powerbi
"""
from __future__ import annotations

import argparse
import logging
import shutil
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

import datasets
import spark_env

spark_env.configure()

from pyspark.sql import DataFrame, SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql.types import DecimalType, DoubleType, IntegerType, StringType  # noqa: E402
from pyspark.sql.window import Window  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "logs"

DELAY_THRESHOLD_MIN = 15

ALIASES = {
    "year": ["Year", "FlightYear"],
    "month": ["Month", "FlightMonth"],
    "day": ["DayofMonth", "DayOfMonth", "Day"],
    "day_of_week": ["DayOfWeek", "Day_of_Week"],
    "airline": ["Reporting_Airline", "OP_UNIQUE_CARRIER", "UniqueCarrier", "Airline", "Carrier"],
    "origin": ["Origin"],
    "destination": ["Dest", "Destination"],
    "scheduled_departure": ["CRSDepTime", "CRS_Dep_Time", "ScheduledDepTime"],
    "departure_delay": ["DepDelay", "Dep_Delay", "DepartureDelay"],
    "arrival_delay": ["ArrDelay", "Arr_Delay", "ArrivalDelay"],
    "cancelled": ["Cancelled", "Canceled"],
    "diverted": ["Diverted"],
    "carrier_delay": ["CarrierDelay", "Carrier_Delay"],
    "weather_delay": ["WeatherDelay", "Weather_Delay"],
    "nas_delay": ["NASDelay", "NAS_Delay"],
    "security_delay": ["SecurityDelay", "Security_Delay"],
    "late_aircraft_delay": ["LateAircraftDelay", "Late_Aircraft_Delay"],
    "flight_date": ["FlightDate", "Flight_Date", "FL_DATE"],
    "origin_city": ["OriginCityName", "Origin_City_Name"],
    "destination_city": ["DestCityName", "DestinationCityName", "Dest_City_Name"],
}

# Reporting carrier codes used by BTS. Unknown codes fall back to the code itself.
AIRLINE_NAMES = {
    "9E": "Endeavor Air", "AA": "American Airlines", "AS": "Alaska Airlines",
    "B6": "JetBlue Airways", "DL": "Delta Air Lines", "F9": "Frontier Airlines",
    "G4": "Allegiant Air", "HA": "Hawaiian Airlines", "MQ": "Envoy Air",
    "NK": "Spirit Airlines", "OH": "PSA Airlines", "OO": "SkyWest Airlines",
    "QX": "Horizon Air", "UA": "United Airlines", "WN": "Southwest Airlines",
    "YV": "Mesa Airlines", "YX": "Republic Airways", "EV": "ExpressJet Airlines",
    "VX": "Virgin America", "ZW": "Air Wisconsin", "C5": "CommutAir",
    "G7": "GoJet Airlines", "PT": "Piedmont Airlines", "CP": "Compass Airlines",
    # Indian domestic carriers (Domestic option)
    "6E": "IndiGo", "AI": "Air India", "IX": "Air India Express", "QP": "Akasa Air",
    "SG": "SpiceJet", "9I": "Alliance Air", "S5": "Star Air", "IC": "Fly91",
}

DAY_NAMES = {1: "Mon", 2: "Tue", 3: "Wed", 4: "Thu", 5: "Fri", 6: "Sat", 7: "Sun"}

log = logging.getLogger("flight_delay_pipeline")


def setup_logging() -> Path:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_file = LOG_DIR / f"pipeline_{datetime.now():%Y%m%d_%H%M%S}.log"
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s", "%H:%M:%S")
    log.setLevel(logging.INFO)
    log.handlers.clear()
    for handler in (logging.StreamHandler(sys.stdout), logging.FileHandler(log_file, encoding="utf-8")):
        handler.setFormatter(formatter)
        log.addHandler(handler)
    return log_file


def build_spark(driver_memory: str = "4g", master: str = "local[*]") -> SparkSession:
    return (
        SparkSession.builder
        .appName("FlightDelayAirportBottleneckAnalysis")
        .master(master)
        .config("spark.driver.memory", driver_memory)
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )


def resolve_columns(df: DataFrame) -> dict[str, Optional[str]]:
    actual = {name.strip().lower(): name for name in df.columns}
    return {
        key: next(
            (actual[candidate.lower()] for candidate in candidates
             if candidate.lower() in actual),
            None,
        )
        for key, candidates in ALIASES.items()
    }


def safe_col(name: Optional[str], data_type, default=None):
    if name is None:
        return F.lit(default).cast(data_type)
    return F.col(f"`{name}`").cast(data_type)


def load_and_standardize(spark: SparkSession, input_path: Path) -> DataFrame:
    """Read the raw CSV and return cleaned records with standard column names."""
    input_path = Path(input_path)
    if not input_path.exists():
        raise FileNotFoundError(
            f"Input CSV not found: {input_path}. Run `python src/download_bts_data.py "
            "--start 2026-06 --end 2026-08` or `python src/make_sample_data.py`."
        )

    raw = (
        spark.read.option("header", "true")
        .option("inferSchema", "false")
        .option("mode", "PERMISSIVE")
        .csv(str(input_path))
    )
    if not raw.columns:
        raise ValueError("CSV has no columns. Check the downloaded file and header.")

    resolved = resolve_columns(raw)
    required = ["airline", "origin", "destination"]
    if resolved["flight_date"] is None:
        required = ["year", "month", "day"] + required
    missing = [name for name in required if resolved[name] is None]
    if missing:
        raise ValueError(
            "Missing required fields: " + ", ".join(missing)
            + f". Found columns: {raw.columns[:25]}. Update ALIASES if names differ."
        )

    if resolved["flight_date"] is not None:
        date_text = safe_col(resolved["flight_date"], StringType())
        date_expr = F.coalesce(
            F.to_date(date_text, "yyyy-MM-dd"),
            F.to_date(date_text, "M/d/yyyy h:mm:ss a"),
            F.to_date(date_text, "M/d/yyyy"),
        )
        year_expr = F.coalesce(safe_col(resolved["year"], IntegerType()), F.year(date_expr))
        month_expr = F.coalesce(safe_col(resolved["month"], IntegerType()), F.month(date_expr))
        day_expr = F.coalesce(safe_col(resolved["day"], IntegerType()), F.dayofmonth(date_expr))
    else:
        year_expr = safe_col(resolved["year"], IntegerType())
        month_expr = safe_col(resolved["month"], IntegerType())
        day_expr = safe_col(resolved["day"], IntegerType())

    standard = raw.select(
        year_expr.cast(IntegerType()).alias("year"),
        month_expr.cast(IntegerType()).alias("month"),
        day_expr.cast(IntegerType()).alias("day"),
        safe_col(resolved["day_of_week"], IntegerType()).alias("day_of_week"),
        safe_col(resolved["airline"], StringType()).alias("airline"),
        safe_col(resolved["origin"], StringType()).alias("origin"),
        safe_col(resolved["destination"], StringType()).alias("destination"),
        safe_col(resolved["scheduled_departure"], StringType()).alias("scheduled_departure_raw"),
        safe_col(resolved["departure_delay"], DoubleType()).alias("departure_delay"),
        safe_col(resolved["arrival_delay"], DoubleType()).alias("arrival_delay"),
        safe_col(resolved["cancelled"], DoubleType(), 0.0).alias("cancelled"),
        safe_col(resolved["diverted"], DoubleType(), 0.0).alias("diverted"),
        safe_col(resolved["carrier_delay"], DoubleType()).alias("carrier_delay"),
        safe_col(resolved["weather_delay"], DoubleType()).alias("weather_delay"),
        safe_col(resolved["nas_delay"], DoubleType()).alias("nas_delay"),
        safe_col(resolved["security_delay"], DoubleType()).alias("security_delay"),
        safe_col(resolved["late_aircraft_delay"], DoubleType()).alias("late_aircraft_delay"),
        safe_col(resolved["origin_city"], StringType()).alias("origin_city"),
        safe_col(resolved["destination_city"], StringType()).alias("destination_city"),
    )

    # With ANSI mode off (Spark default), make_date returns NULL for impossible dates.
    standard = (
        standard
        .withColumn("scheduled_departure_num", F.trim(F.col("scheduled_departure_raw")).cast(IntegerType()))
        .withColumn(
            "scheduled_departure_hour",
            F.when(
                F.col("scheduled_departure_num").between(0, 2359)
                & ((F.col("scheduled_departure_num") % 100) < 60),
                F.floor(F.col("scheduled_departure_num") / 100).cast(IntegerType()),
            ).when(F.col("scheduled_departure_num") == 2400, F.lit(0)),
        )
        .withColumn("flight_date", F.make_date(F.col("year"), F.col("month"), F.col("day")))
    )

    for name in ["airline", "origin", "destination"]:
        standard = standard.withColumn(name, F.upper(F.trim(F.col(name))))

    operated = (F.col("cancelled") == 0) & (F.col("diverted") == 0)
    standard = (
        standard
        .withColumn("cancelled", F.when(F.coalesce(F.col("cancelled"), F.lit(0.0)) > 0, 1).otherwise(0))
        .withColumn("diverted", F.when(F.coalesce(F.col("diverted"), F.lit(0.0)) > 0, 1).otherwise(0))
        # BTS uses Monday=1..Sunday=7; Spark's dayofweek uses Sunday=1..Saturday=7.
        .withColumn("day_of_week", F.coalesce(F.col("day_of_week"),
                                              (F.dayofweek("flight_date") + 5) % 7 + 1))
        .filter(F.col("flight_date").isNotNull())
        .filter(F.col("airline").isNotNull() & (F.col("airline") != ""))
        .filter(F.col("origin").isNotNull() & (F.col("origin") != ""))
        .filter(F.col("destination").isNotNull() & (F.col("destination") != ""))
        .filter(F.col("origin") != F.col("destination"))
        .dropDuplicates()
        .withColumn("route", F.concat_ws("-", "origin", "destination"))
        .withColumn(
            "arrival_delayed_15",
            F.when(
                F.col("arrival_delay").isNotNull()
                & (F.col("arrival_delay") >= DELAY_THRESHOLD_MIN)
                & operated, 1,
            ).otherwise(0),
        )
        .withColumn(
            "departure_delayed_15",
            F.when(
                F.col("departure_delay").isNotNull()
                & (F.col("departure_delay") >= DELAY_THRESHOLD_MIN)
                & (F.col("cancelled") == 0), 1,
            ).otherwise(0),
        )
        .withColumn("year_month", F.date_format("flight_date", "yyyy-MM"))
    )
    return standard


def safe_percent(numerator, denominator):
    return F.when(denominator > 0, numerator * F.lit(100.0) / denominator)


def lookup(mapping: dict, column: str):
    """Translate codes with a Spark map literal (no Python workers or joins needed)."""
    pairs = [F.lit(item) for pair in mapping.items() for item in pair]
    return F.create_map(*pairs)[F.col(column)]


def priority_score(rate_col: str, volume_col: str):
    """Project-defined hotspot score: delay rate (%) x ln(1 + flights). Not an official metric."""
    return F.round(F.col(rate_col) * F.log1p(F.col(volume_col)), 2)


def write_csv(df: DataFrame, destination: Path) -> None:
    """Write a small summary as one CSV file for convenient Power BI import."""
    destination.parent.mkdir(parents=True, exist_ok=True)
    temp_dir = Path(str(destination) + "_spark_tmp")
    if temp_dir.exists():
        shutil.rmtree(temp_dir)
    (
        df.coalesce(1).write.mode("overwrite")
        .option("header", "true").option("nullValue", "")
        .csv(str(temp_dir))
    )
    parts = list(temp_dir.glob("part-*.csv"))
    if not parts:
        raise RuntimeError(f"Spark did not create a CSV part file in {temp_dir}")
    if destination.exists():
        destination.unlink()
    parts[0].replace(destination)
    shutil.rmtree(temp_dir, ignore_errors=True)


def round_doubles(df: DataFrame, digits: int = 2) -> DataFrame:
    return df.select([
        F.round(F.col(name), digits).alias(name) if dtype == "double" else F.col(name)
        for name, dtype in df.dtypes
    ])


def plain_numbers(df: DataFrame) -> DataFrame:
    """Cast doubles to decimals so CSVs show 45617724.00 rather than 4.5617724E7."""
    return df.select([
        F.col(name).cast(DecimalType(38, 2)).alias(name) if dtype == "double" else F.col(name)
        for name, dtype in df.dtypes
    ])


def build_summaries(df: DataFrame, min_group_flights: int = 100,
                    min_route_flights: int = 50) -> dict[str, DataFrame]:
    """Return {filename: DataFrame} for every summary table."""
    # Main arrival-delay denominator: operated flights with a known arrival delay.
    arrival_base = df.filter(
        (F.col("cancelled") == 0) & (F.col("diverted") == 0)
        & F.col("arrival_delay").isNotNull()
    )

    monthly_status = df.groupBy("year_month").agg(
        F.count("*").alias("scheduled_flights"),
        F.sum("cancelled").alias("cancelled_flights"),
        F.sum("diverted").alias("diverted_flights"),
    )
    monthly = (
        arrival_base.groupBy("year_month")
        .agg(
            F.count("*").alias("operated_flights_with_arrival_data"),
            F.sum("arrival_delayed_15").alias("arrival_delayed_flights"),
            F.avg("arrival_delay").alias("average_arrival_delay_min"),
            F.expr("percentile_approx(arrival_delay, 0.5)").alias("median_arrival_delay_min"),
            F.avg("departure_delay").alias("average_departure_delay_min"),
        )
        .withColumn("arrival_delay_rate_pct",
                    safe_percent(F.col("arrival_delayed_flights"),
                                 F.col("operated_flights_with_arrival_data")))
        .join(monthly_status, "year_month", "left")
        .withColumn("cancellation_rate_pct",
                    safe_percent(F.col("cancelled_flights"), F.col("scheduled_flights")))
        .orderBy("year_month")
    )

    airline_status = df.groupBy("airline").agg(
        F.count("*").alias("scheduled_flights"),
        F.sum("cancelled").alias("cancelled_flights"),
    )
    airline = (
        arrival_base.groupBy("airline")
        .agg(
            F.count("*").alias("flights"),
            F.sum("arrival_delayed_15").alias("arrival_delayed_flights"),
            F.avg("arrival_delay").alias("average_arrival_delay_min"),
            F.avg("departure_delay").alias("average_departure_delay_min"),
        )
        .withColumn("arrival_delay_rate_pct",
                    safe_percent(F.col("arrival_delayed_flights"), F.col("flights")))
        .join(airline_status, "airline", "left")
        .withColumn("cancellation_rate_pct",
                    safe_percent(F.col("cancelled_flights"), F.col("scheduled_flights")))
        .withColumn("airline_name", F.coalesce(lookup(AIRLINE_NAMES, "airline"), F.col("airline")))
        .filter(F.col("flights") >= min_group_flights)
        .orderBy(F.desc("arrival_delay_rate_pct"))
    )

    origin = (
        df.filter((F.col("cancelled") == 0) & F.col("departure_delay").isNotNull())
        .groupBy("origin")
        .agg(
            F.first("origin_city", ignorenulls=True).alias("origin_city"),
            F.count("*").alias("flights"),
            F.sum("departure_delayed_15").alias("departure_delayed_flights"),
            F.avg("departure_delay").alias("average_departure_delay_min"),
        )
        .withColumn("departure_delay_rate_pct",
                    safe_percent(F.col("departure_delayed_flights"), F.col("flights")))
        .filter(F.col("flights") >= min_group_flights)
        .withColumn("priority_score", priority_score("departure_delay_rate_pct", "flights"))
        .orderBy(F.desc("departure_delay_rate_pct"))
    )

    destination = (
        arrival_base.groupBy("destination")
        .agg(
            F.first("destination_city", ignorenulls=True).alias("destination_city"),
            F.count("*").alias("flights"),
            F.sum("arrival_delayed_15").alias("arrival_delayed_flights"),
            F.avg("arrival_delay").alias("average_arrival_delay_min"),
        )
        .withColumn("arrival_delay_rate_pct",
                    safe_percent(F.col("arrival_delayed_flights"), F.col("flights")))
        .filter(F.col("flights") >= min_group_flights)
        .withColumn("priority_score", priority_score("arrival_delay_rate_pct", "flights"))
        .orderBy(F.desc("arrival_delay_rate_pct"))
    )

    routes = (
        arrival_base.groupBy("origin", "destination", "route")
        .agg(
            F.first("origin_city", ignorenulls=True).alias("origin_city"),
            F.first("destination_city", ignorenulls=True).alias("destination_city"),
            F.count("*").alias("flights"),
            F.sum("arrival_delayed_15").alias("arrival_delayed_flights"),
            F.avg("arrival_delay").alias("average_arrival_delay_min"),
        )
        .withColumn("arrival_delay_rate_pct",
                    safe_percent(F.col("arrival_delayed_flights"), F.col("flights")))
        .filter(F.col("flights") >= min_route_flights)
        .withColumn("priority_score", priority_score("arrival_delay_rate_pct", "flights"))
        .orderBy(F.desc("arrival_delay_rate_pct"), F.desc("flights"))
    )

    hourly = (
        arrival_base.filter(F.col("scheduled_departure_hour").between(0, 23))
        .groupBy("scheduled_departure_hour")
        .agg(
            F.count("*").alias("flights"),
            F.sum("arrival_delayed_15").alias("arrival_delayed_flights"),
            F.avg("arrival_delay").alias("average_arrival_delay_min"),
        )
        .withColumn("arrival_delay_rate_pct",
                    safe_percent(F.col("arrival_delayed_flights"), F.col("flights")))
        .orderBy("scheduled_departure_hour")
    )

    weekday = (
        arrival_base.filter(F.col("day_of_week").between(1, 7))
        .groupBy("day_of_week")
        .agg(
            F.count("*").alias("flights"),
            F.sum("arrival_delayed_15").alias("arrival_delayed_flights"),
            F.avg("arrival_delay").alias("average_arrival_delay_min"),
        )
        .withColumn("arrival_delay_rate_pct",
                    safe_percent(F.col("arrival_delayed_flights"), F.col("flights")))
        .withColumn("day_name", lookup(DAY_NAMES, "day_of_week"))
        .orderBy("day_of_week")
    )

    cause_columns = [
        ("Carrier", "carrier_delay"),
        ("Weather", "weather_delay"),
        ("National Air System", "nas_delay"),
        ("Security", "security_delay"),
        ("Late Aircraft", "late_aircraft_delay"),
    ]
    # One pass over the data, then unpivot the five sums into rows.
    cause_totals = df.agg(*[
        F.coalesce(F.sum(column), F.lit(0.0)).alias(column) for _, column in cause_columns
    ])
    stack_expr = "stack({n}, {pairs}) as (delay_cause, total_reported_delay_minutes)".format(
        n=len(cause_columns),
        pairs=", ".join(f"'{label}', `{column}`" for label, column in cause_columns),
    )
    causes = cause_totals.selectExpr(stack_expr)
    grand_total = F.sum("total_reported_delay_minutes").over(Window.partitionBy())
    causes = (
        causes.withColumn("share_of_reported_minutes_pct",
                          safe_percent(F.col("total_reported_delay_minutes"), grand_total))
        .orderBy(F.desc("total_reported_delay_minutes"))
    )

    totals = df.agg(
        F.count("*").alias("cleaned_records"),
        F.sum("cancelled").alias("cancelled_flights"),
        F.sum("diverted").alias("diverted_flights"),
        F.min("flight_date").alias("first_date"),
        F.max("flight_date").alias("last_date"),
        F.countDistinct("airline").alias("airlines"),
        F.countDistinct("origin").alias("origin_airports"),
        F.countDistinct("route").alias("routes"),
    ).first()
    arrival_totals = arrival_base.agg(
        F.count("*").alias("operated"),
        F.sum("arrival_delayed_15").alias("delayed"),
        F.avg("arrival_delay").alias("avg_delay"),
    ).first()

    operated_count = arrival_totals["operated"] or 0
    delayed_count = arrival_totals["delayed"] or 0
    cleaned = totals["cleaned_records"] or 0
    delay_rate = delayed_count / operated_count * 100.0 if operated_count else 0.0

    def date_number(value) -> float:
        return float(value.strftime("%Y%m%d")) if value else 0.0

    kpi_rows = [
        ("cleaned_records", float(cleaned)),
        ("operated_flights_with_arrival_data", float(operated_count)),
        ("arrival_delayed_flights_15_min_or_more", float(delayed_count)),
        ("arrival_delay_rate_pct", float(delay_rate)),
        ("cancelled_flights", float(totals["cancelled_flights"] or 0)),
        ("diverted_flights", float(totals["diverted_flights"] or 0)),
        ("cancellation_rate_pct",
         float((totals["cancelled_flights"] or 0) / cleaned * 100.0) if cleaned else 0.0),
        ("average_arrival_delay_min", float(arrival_totals["avg_delay"] or 0.0)),
        ("airlines", float(totals["airlines"] or 0)),
        ("origin_airports", float(totals["origin_airports"] or 0)),
        ("routes", float(totals["routes"] or 0)),
        ("first_flight_date_yyyymmdd", date_number(totals["first_date"])),
        ("last_flight_date_yyyymmdd", date_number(totals["last_date"])),
    ]
    kpis = df.sparkSession.createDataFrame(kpi_rows, ["metric", "value"])

    tables = {
        "monthly_summary.csv": monthly,
        "airline_summary.csv": airline,
        "origin_airport_summary.csv": origin,
        "destination_airport_summary.csv": destination,
        "route_summary.csv": routes,
        "hourly_summary.csv": hourly,
        "delay_cause_summary.csv": causes,
        "kpi_summary.csv": kpis,
        "weekday_summary.csv": weekday,
    }
    return {name: round_doubles(table) for name, table in tables.items()}


def make_summaries(df: DataFrame, output_dir: Path, min_group_flights: int = 100,
                   min_route_flights: int = 50) -> list[Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    written = []
    for filename, table in build_summaries(df, min_group_flights, min_route_flights).items():
        write_csv(plain_numbers(table), output_dir / filename)
        log.info("Created: %s", output_dir / filename)
        written.append(output_dir / filename)
    return written


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Flight delay analysis using PySpark")
    parser.add_argument("--dataset", choices=list(datasets.DATASETS), default=datasets.DEFAULT,
                        help="usa = International (USA, BTS), india = Domestic (India, simulated)")
    parser.add_argument("--input", help="Input CSV path (default: the dataset's flights.csv)")
    parser.add_argument("--processed", help="Parquet output directory")
    parser.add_argument("--output", help="Summary CSV output directory")
    parser.add_argument("--min-group-flights", type=int, default=100,
                        help="Minimum flights for airline/airport rankings (default 100)")
    parser.add_argument("--min-route-flights", type=int, default=50,
                        help="Minimum flights for route rankings (default 50)")
    parser.add_argument("--driver-memory", default="4g", help="Spark driver memory (default 4g)")
    parser.add_argument("--skip-dashboard", action="store_true",
                        help="Do not refresh the web dashboard data file")
    parser.add_argument("--skip-website", action="store_true",
                        help="Do not refresh the customer website data file")
    args = parser.parse_args(argv)

    locations = datasets.paths(args.dataset, ROOT)
    input_path = Path(args.input or locations["input"]).resolve()
    processed_path = Path(args.processed or locations["processed"]).resolve()
    output_path = Path(args.output or locations["summaries"]).resolve()

    log_file = setup_logging()
    started = time.time()
    spark = build_spark(args.driver_memory)
    spark.sparkContext.setLogLevel("ERROR")
    try:
        log.info("Dataset: %s (%s)", args.dataset, datasets.DATASETS[args.dataset].option)
        log.info("Reading input: %s", input_path)
        flights = load_and_standardize(spark, input_path).cache()
        count = flights.count()
        log.info("Cleaned records: %s", f"{count:,}")
        if count == 0:
            raise ValueError("All records were filtered out. See the Troubleshooting section of the README.")
        flights.printSchema()
        flights.select("flight_date", "airline", "route", "scheduled_departure_hour",
                       "departure_delay", "arrival_delay", "cancelled", "diverted",
                       "arrival_delayed_15").show(10, truncate=False)

        processed_path.parent.mkdir(parents=True, exist_ok=True)
        flights.write.mode("overwrite").partitionBy("year_month").parquet(str(processed_path))
        log.info("Created Parquet dataset (partitioned by year_month): %s", processed_path)

        make_summaries(flights, output_path, args.min_group_flights, args.min_route_flights)
        if not args.skip_website:
            import build_website_data
            build_website_data.write_site_data(flights, locations["website_data"], args.dataset)

        elapsed = time.time() - started
        summary = locations["run_summary"]
        summary.parent.mkdir(parents=True, exist_ok=True)
        summary.write_text(
            "Flight Delay Analysis completed.\n"
            f"Dataset: {args.dataset} ({datasets.DATASETS[args.dataset].option})\n"
            f"Run at: {datetime.now():%Y-%m-%d %H:%M:%S}\n"
            f"Input: {input_path}\n"
            f"Cleaned records: {count:,}\n"
            f"Delay threshold: arrival delay >= {DELAY_THRESHOLD_MIN} minutes\n"
            f"Minimum flights: airline/airport {args.min_group_flights}, route {args.min_route_flights}\n"
            f"Parquet: {processed_path}\n"
            f"Summary CSVs: {output_path}\n"
            f"Elapsed: {elapsed:.1f} s\n"
            f"Log: {log_file}\n",
            encoding="utf-8",
        )
        log.info("Run completed in %.1f s. Summary: %s", elapsed, summary)
    finally:
        spark.stop()

    if not args.skip_dashboard:
        import build_dashboard
        build_dashboard.build(output_path, args.dataset)
    return 0


if __name__ == "__main__":
    sys.exit(main())
