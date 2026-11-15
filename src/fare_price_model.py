"""Journey price prediction - a Spark MLlib model of airfares, trained on fares + flight big data.

Training data (one row per airport pair, quarter and airline):
- fares: data/raw/fares.csv (USA: real U.S. DOT Consumer Airfare Report Table 1a; India:
  simulated by make_india_fares.py in the same layout). Each pair-quarter gives three labels:
  the market average fare (airline "ALL"), the largest airline's fare and the low-fare airline's fare;
- network features computed with Spark from the cleaned flight Parquet (millions of flights):
  nonstop flights on the pair, airlines competing on it, its late-arrival and cancellation rates,
  the airline's flights and reliability on the pair, the size of both airports and of the airline.

Three regressors are trained on log(fare) and compared on a time-based hold-out (the latest
4 quarters, never seen in training): linear regression (baseline), random forest and
gradient-boosted trees. The best one is refitted on all quarters and forecasts the next 4
quarters for every route on the customer website, by airline, with a typical range taken
from the hold-out errors.

Outputs:
    output/models/fare_model/                Spark PipelineModel (reload with PipelineModel.load)
    output/powerbi/fare_*.csv                metrics, feature importance, forecast, fare trend
    website/data/price-data-<dataset>.js     fare forecasts for the OnTime Check website

    python src/fare_price_model.py                    # International (USA)
    python src/fare_price_model.py --dataset india    # Domestic (India, simulated)
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

from pyspark.ml import PipelineModel  # noqa: E402
from pyspark.ml.evaluation import RegressionEvaluator  # noqa: E402
from pyspark.ml.feature import OneHotEncoder, StringIndexer, StringIndexerModel, VectorAssembler  # noqa: E402
from pyspark.ml.regression import GBTRegressor, LinearRegression, RandomForestRegressor  # noqa: E402
from pyspark.sql import DataFrame, SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql.types import DoubleType, IntegerType  # noqa: E402

from flight_delay_pipeline import AIRLINE_NAMES, build_spark, plain_numbers, round_doubles, write_csv  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]

MARKET = "ALL"          # pseudo-airline for the market average fare of a pair
TEST_QUARTERS = 4       # latest quarters held out for testing
FORECAST_QUARTERS = 4
MIN_ROUTE_FLIGHTS = 30  # same minimums as the website (build_website_data.py)
MIN_CELL_FLIGHTS = 20
SEED = 42

NUMERIC_FEATURES = {
    "log_distance": "Distance",
    "log_passengers": "Passengers per day",
    "year": "Year",
    "quarter": "Quarter (season)",
    "log_pair_flights": "Nonstop flights on the pair",
    "pair_airlines": "Airlines competing on the pair",
    "pair_late_pct": "Pair late-arrival rate",
    "pair_cancel_pct": "Pair cancellation rate",
    "log_carrier_pair_flights": "Airline's flights on the pair",
    "carrier_late_pct": "Airline's late rate on the pair",
    "log_hub_large": "Size of the bigger airport",
    "log_hub_small": "Size of the smaller airport",
    "log_carrier_flights": "Airline network size",
}
CATEGORICAL_FEATURES = {"carrier": "Airline", "airport_a": "Airport A", "airport_b": "Airport B"}

log = logging.getLogger("fare_price_model")


# ---------------------------------------------------------------- data
def load_fares(spark: SparkSession, path: Path) -> DataFrame:
    """Table 1a rows -> one row per (pair, quarter, airline) with a fare label."""
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Fare data not found: {path}. Run python src/download_fare_data.py "
                                "(USA) or python src/make_india_fares.py (India).")
    raw = spark.read.option("header", "true").csv(str(path))
    ap1, ap2 = F.upper(F.trim("airport_1")), F.upper(F.trim("airport_2"))
    base = raw.select(
        F.col("year").cast(IntegerType()).alias("year"),
        F.col("quarter").cast(IntegerType()).alias("quarter"),
        F.least(ap1, ap2).alias("airport_a"),
        F.greatest(ap1, ap2).alias("airport_b"),
        F.col("nsmiles").cast(DoubleType()).alias("distance_miles"),
        F.col("passengers").cast(DoubleType()).alias("passengers"),
        F.col("fare").cast(DoubleType()).alias("fare"),
        F.upper(F.trim("carrier_lg")).alias("carrier_lg"),
        F.col("fare_lg").cast(DoubleType()).alias("fare_lg"),
        F.upper(F.trim("carrier_low")).alias("carrier_low"),
        F.col("fare_low").cast(DoubleType()).alias("fare_low"),
    ).filter(F.col("year").isNotNull() & F.col("quarter").between(1, 4)
             & (F.col("airport_a") != F.col("airport_b"))
             & (F.col("distance_miles") > 0) & (F.col("passengers") > 0))
    keys = ["year", "quarter", "airport_a", "airport_b", "distance_miles", "passengers"]
    labels = (
        base.select(*keys, F.lit(MARKET).alias("carrier"), "fare")
        .unionByName(base.select(*keys, F.col("carrier_lg").alias("carrier"), F.col("fare_lg").alias("fare")))
        .unionByName(base.filter(F.col("carrier_low") != F.col("carrier_lg"))
                     .select(*keys, F.col("carrier_low").alias("carrier"), F.col("fare_low").alias("fare")))
        .filter((F.col("fare") > 0) & F.col("carrier").isNotNull() & (F.col("carrier") != ""))
    )
    return (
        labels.groupBy("year", "quarter", "airport_a", "airport_b", "carrier")
        .agg(F.max("distance_miles").alias("distance_miles"), F.avg("passengers").alias("passengers"),
             F.avg("fare").alias("fare"))
        .withColumn("period", F.col("year") * 4 + F.col("quarter") - 1)
    )


def network_features(flights: DataFrame) -> dict[str, DataFrame]:
    """Route, airline and airport features aggregated from the cleaned flight records."""
    first = F.col("origin") < F.col("destination")
    f = (flights.withColumn("airport_a", F.when(first, F.col("origin")).otherwise(F.col("destination")))
         .withColumn("airport_b", F.when(first, F.col("destination")).otherwise(F.col("origin"))))
    operated = (F.col("cancelled") == 0) & (F.col("diverted") == 0) & F.col("arrival_delay").isNotNull()
    late_pct = F.avg(F.when(operated, F.col("arrival_delayed_15"))) * 100.0
    pair = f.groupBy("airport_a", "airport_b").agg(
        F.count("*").alias("pair_flights"),
        F.countDistinct("airline").alias("pair_airlines"),
        late_pct.alias("pair_late_pct"),
        (F.avg("cancelled") * 100.0).alias("pair_cancel_pct"),
    )
    carrier_pair = f.groupBy("airport_a", "airport_b", F.col("airline").alias("carrier")).agg(
        F.count("*").alias("carrier_pair_flights"),
        late_pct.alias("carrier_late_pct"),
    )
    airport = (f.select(F.col("origin").alias("airport")).unionByName(f.select(F.col("destination").alias("airport")))
               .groupBy("airport").agg((F.count("*") / 2).alias("airport_flights")))
    carrier = f.groupBy(F.col("airline").alias("carrier")).agg(F.count("*").alias("carrier_flights"))
    return {"pair": pair, "carrier_pair": carrier_pair, "airport": airport, "carrier": carrier}


def add_features(rows: DataFrame, network: dict[str, DataFrame]) -> DataFrame:
    """Join the network features onto (pair, quarter, airline) rows and derive model inputs."""
    hub_a = network["airport"].select(F.col("airport").alias("airport_a"), F.col("airport_flights").alias("hub_a"))
    hub_b = network["airport"].select(F.col("airport").alias("airport_b"), F.col("airport_flights").alias("hub_b"))
    df = (rows.join(network["pair"], ["airport_a", "airport_b"], "left")
          .join(network["carrier_pair"], ["airport_a", "airport_b", "carrier"], "left")
          .join(hub_a, "airport_a", "left").join(hub_b, "airport_b", "left")
          .join(network["carrier"], "carrier", "left"))
    market = F.col("carrier") == MARKET
    # The market row takes the pair's own figures; missing counts mean "no nonstop service" (0).
    return (
        df.withColumn("carrier_pair_flights", F.when(market, F.col("pair_flights")).otherwise(F.col("carrier_pair_flights")))
        .withColumn("carrier_late_pct", F.when(market, F.col("pair_late_pct")).otherwise(F.col("carrier_late_pct")))
        .fillna(0, ["pair_flights", "pair_airlines", "pair_late_pct", "pair_cancel_pct", "carrier_pair_flights",
                    "carrier_late_pct", "hub_a", "hub_b", "carrier_flights"])
        .withColumn("log_distance", F.log("distance_miles"))
        .withColumn("log_passengers", F.log("passengers"))
        .withColumn("log_pair_flights", F.log1p("pair_flights"))
        .withColumn("log_carrier_pair_flights", F.log1p("carrier_pair_flights"))
        .withColumn("log_hub_large", F.log1p(F.greatest("hub_a", "hub_b")))
        .withColumn("log_hub_small", F.log1p(F.least("hub_a", "hub_b")))
        .withColumn("log_carrier_flights", F.log1p("carrier_flights"))
        .withColumn("pair_airlines", F.col("pair_airlines").cast(DoubleType()))
        .withColumn("year", F.col("year").cast(DoubleType()))
        .withColumn("quarter", F.col("quarter").cast(DoubleType()))
    )


# ---------------------------------------------------------------- model
def feature_stages(df: DataFrame) -> list:
    """Indexers/encoders/assemblers fitted on ``df``: 'features' for trees, 'lr_features' for the baseline."""
    stages = []
    for name in CATEGORICAL_FEATURES:
        stages.append(StringIndexer(inputCol=name, outputCol=f"{name}_idx", handleInvalid="keep").fit(df))
    encoder = OneHotEncoder(inputCols=[f"{n}_idx" for n in CATEGORICAL_FEATURES],
                            outputCols=[f"{n}_vec" for n in CATEGORICAL_FEATURES], handleInvalid="keep")
    indexed = df
    for stage in stages:
        indexed = stage.transform(indexed)
    stages.append(encoder.fit(indexed))
    stages.append(VectorAssembler(inputCols=list(NUMERIC_FEATURES) + [f"{n}_idx" for n in CATEGORICAL_FEATURES],
                                  outputCol="features"))
    stages.append(VectorAssembler(inputCols=list(NUMERIC_FEATURES) + [f"{n}_vec" for n in CATEGORICAL_FEATURES],
                                  outputCol="lr_features"))
    return stages


def candidate_models(max_bins: int, fast: bool = False) -> dict:
    trees = 25 if fast else 60
    return {
        "Linear regression (baseline)": LinearRegression(featuresCol="lr_features", labelCol="label",
                                                         regParam=0.001, elasticNetParam=0.0, maxIter=100),
        "Random forest": RandomForestRegressor(featuresCol="features", labelCol="label", numTrees=trees,
                                               maxDepth=10 if fast else 12, maxBins=max_bins,
                                               subsamplingRate=0.8, seed=SEED),
        "Gradient-boosted trees": GBTRegressor(featuresCol="features", labelCol="label", maxIter=trees,
                                               maxDepth=6 if fast else 7, stepSize=0.1, maxBins=max_bins,
                                               subsamplingRate=0.8, seed=SEED),
    }


def with_predicted_fare(df: DataFrame) -> DataFrame:
    return df.withColumn("predicted_fare", F.exp("prediction"))


def evaluate(predictions: DataFrame) -> dict[str, float]:
    metrics = {name: RegressionEvaluator(labelCol="fare", predictionCol="predicted_fare", metricName=name)
               .evaluate(predictions) for name in ("rmse", "mae", "r2")}
    metrics["mape_pct"] = predictions.agg(
        F.avg(F.abs(F.col("predicted_fare") - F.col("fare")) / F.col("fare")) * 100.0).first()[0]
    return metrics


def importances(model, df: DataFrame) -> list[tuple[str, float]]:
    """Tree feature importances by readable name (the baseline has none)."""
    if not hasattr(model, "featureImportances"):
        return []
    names = {**NUMERIC_FEATURES, **{f"{k}_idx": v for k, v in CATEGORICAL_FEATURES.items()}}
    attrs = df.schema["features"].metadata["ml_attr"]["attrs"]
    slots = sorted((a["idx"], a["name"]) for group in attrs.values() for a in group)
    vector = model.featureImportances.toArray()
    return sorted(((names.get(name, name), float(vector[i])) for i, name in slots), key=lambda x: -x[1])


def period_label(period: int) -> str:
    return f"{period // 4}Q{period % 4 + 1}"


def train(fares: DataFrame, network: dict[str, DataFrame], fast: bool = False):
    """Fit, compare on the hold-out, refit the winner on everything. Returns a result dict."""
    data = add_features(fares, network).withColumn("label", F.log("fare")).cache()
    last = data.agg(F.max("period")).first()[0]
    cutoff = last - TEST_QUARTERS + 1
    stages = feature_stages(data)
    prepared = PipelineModel(stages=stages).transform(data).cache()
    train_df = prepared.filter(F.col("period") < cutoff).cache()
    test_df = prepared.filter(F.col("period") >= cutoff).cache()
    counts = {"train_rows": train_df.count(), "test_rows": test_df.count()}
    if not counts["train_rows"] or not counts["test_rows"]:
        raise ValueError("Not enough quarters of fare data for a time-based hold-out.")
    labels = {s.getInputCol(): s.labelsArray[0] for s in stages if isinstance(s, StringIndexerModel)}
    max_bins = max(32, *(len(values) + 1 for values in labels.values()))  # trees need a bin per category
    log.info("Training rows %s, test rows %s (test quarters %s - %s)", f"{counts['train_rows']:,}",
             f"{counts['test_rows']:,}", period_label(cutoff), period_label(last))

    results = []
    for name, estimator in candidate_models(max_bins, fast).items():
        started = time.time()
        model = estimator.fit(train_df)
        predictions = with_predicted_fare(model.transform(test_df))
        metrics = evaluate(predictions)
        metrics["train_seconds"] = time.time() - started
        log.info("%-30s RMSE %.2f  MAE %.2f  MAPE %.1f%%  R2 %.3f  (%.0f s)", name, metrics["rmse"],
                 metrics["mae"], metrics["mape_pct"], metrics["r2"], metrics["train_seconds"])
        results.append({"name": name, "estimator": estimator, "model": model, "predictions": predictions, **metrics})
    best = min(results, key=lambda r: r["rmse"])
    ratio = best["predictions"].select((F.col("fare") / F.col("predicted_fare")).alias("ratio"))
    low, high = ratio.approxQuantile("ratio", [0.1, 0.9], 0.001)
    log.info("Selected: %s. 80%% of hold-out fares fall within x%.2f - x%.2f of the prediction",
             best["name"], low, high)

    final = best["estimator"].fit(prepared)
    return {
        "pipeline": PipelineModel(stages=stages + [final]),
        "results": results, "best": best, "interval": (low, high),
        "importance": importances(final, prepared),
        "test_period": f"{period_label(cutoff)}-{period_label(last)}",
        "last_period": last, "data": data, **counts,
        "carriers": set(labels["carrier"]),
    }


# ---------------------------------------------------------------- forecast
def forecast(spark: SparkSession, result: dict, flights: DataFrame, network: dict[str, DataFrame]) -> DataFrame:
    """Predicted fares for the next quarters, for every website route and its airlines."""
    data, last = result["data"], result["last_period"]
    periods = [last + i for i in range(1, FORECAST_QUARTERS + 1)]
    quarters = spark.createDataFrame([(p // 4, p % 4 + 1) for p in periods], ["year", "quarter"])

    first = F.col("origin") < F.col("destination")
    directional = flights.groupBy("origin", "destination").count().filter(F.col("count") >= MIN_ROUTE_FLIGHTS)
    pairs = (directional.select(F.when(first, F.col("origin")).otherwise(F.col("destination")).alias("airport_a"),
                                F.when(first, F.col("destination")).otherwise(F.col("origin")).alias("airport_b"))
             .distinct())
    # Distance and traffic come from the pair's latest year of fare data.
    recent = (data.filter((F.col("period") > last - 4) & (F.col("carrier") == MARKET))
              .groupBy("airport_a", "airport_b")
              .agg(F.max("distance_miles").alias("distance_miles"), F.avg("passengers").alias("passengers"),
                   F.avg("fare").alias("last_fare")))
    carriers = (network["carrier_pair"].filter(F.col("carrier_pair_flights") >= MIN_CELL_FLIGHTS)
                .filter(F.col("carrier").isin(sorted(result["carriers"] - {MARKET})))
                .select("airport_a", "airport_b", "carrier"))
    pair_carriers = pairs.withColumn("carrier", F.lit(MARKET)).unionByName(pairs.join(carriers, ["airport_a", "airport_b"]))
    last_by_carrier = (data.filter(F.col("period") > last - 4).groupBy("airport_a", "airport_b", "carrier")
                       .agg(F.avg("fare").alias("last_carrier_fare")))
    rows = (pair_carriers.join(recent, ["airport_a", "airport_b"]).crossJoin(quarters)
            .join(last_by_carrier, ["airport_a", "airport_b", "carrier"], "left"))
    low, high = result["interval"]
    predicted = with_predicted_fare(result["pipeline"].transform(add_features(rows, network)))
    return predicted.select(
        "airport_a", "airport_b", "carrier", F.col("year").cast(IntegerType()).alias("year"),
        F.col("quarter").cast(IntegerType()).alias("quarter"), "distance_miles",
        F.round("predicted_fare", 0).alias("predicted_fare"),
        F.round(F.col("predicted_fare") * low, 0).alias("typical_low"),
        F.round(F.col("predicted_fare") * high, 0).alias("typical_high"),
        F.round("last_carrier_fare", 0).alias("last_year_fare"),
    )


# ---------------------------------------------------------------- outputs
def summary_tables(spark: SparkSession, result: dict, predicted: DataFrame) -> dict[str, DataFrame]:
    best = result["best"]["name"]
    metrics = spark.createDataFrame(
        [(r["name"], float(r["rmse"]), float(r["mae"]), float(r["mape_pct"]), float(r["r2"]),
          result["train_rows"], result["test_rows"], result["test_period"], r["name"] == best)
         for r in result["results"]],
        "model string, rmse double, mae double, mape_pct double, r2 double, train_rows long, test_rows long, "
        "test_period string, selected boolean")
    importance = spark.createDataFrame([(name, value * 100.0) for name, value in result["importance"]]
                                       or [("(linear model)", 0.0)], "feature string, importance_pct double")
    names = F.create_map(*[F.lit(x) for pair in {**AIRLINE_NAMES, MARKET: "All airlines"}.items() for x in pair])
    forecast_table = (predicted.withColumn("airline_name", F.coalesce(names[F.col("carrier")], F.col("carrier")))
                      .orderBy("airport_a", "airport_b", "carrier", "year", "quarter"))
    market = result["data"].filter(F.col("carrier") == MARKET)
    trend = (market.groupBy("year", "quarter")
             .agg(F.count("*").alias("airport_pairs"),
                  (F.sum(F.col("fare") * F.col("passengers")) / F.sum("passengers")).alias("passenger_weighted_fare"),
                  F.avg("fare").alias("average_pair_fare"))
             .withColumn("year", F.col("year").cast(IntegerType()))
             .withColumn("quarter", F.col("quarter").cast(IntegerType()))
             .orderBy("year", "quarter"))
    tables = {"fare_model_metrics.csv": metrics, "fare_feature_importance.csv": importance,
              "fare_forecast.csv": forecast_table, "fare_trend.csv": trend}
    return {name: round_doubles(table) for name, table in tables.items()}


def write_price_data(result: dict, predicted: DataFrame, target: Path, dataset: str) -> Path:
    """Compact forecast file for the website: one row per pair and airline, one fare per quarter."""
    info = datasets.DATASETS[dataset]
    periods = [result["last_period"] + i for i in range(1, FORECAST_QUARTERS + 1)]
    cells: dict[tuple, dict] = {}
    for r in predicted.collect():
        cell = cells.setdefault((r.airport_a, r.airport_b, r.carrier),
                                {"fares": [None] * len(periods), "last": r.last_year_fare})
        cell["fares"][periods.index(r.year * 4 + r.quarter - 1)] = int(r.predicted_fare)
    rows = [[a, b, c, *v["fares"], int(v["last"]) if v["last"] is not None else None]
            for (a, b, c), v in sorted(cells.items())]
    best = result["best"]
    data = {
        "dataset": {"key": info.key, "currency": info.currency, "source": info.fare_source,
                    "simulated": info.simulated},
        "model": {
            "name": best["name"],
            "metrics": [{"model": r["name"], "rmse": round(r["rmse"], 2), "mae": round(r["mae"], 2),
                         "mape_pct": round(r["mape_pct"], 1), "r2": round(r["r2"], 3)} for r in result["results"]],
            "train_rows": result["train_rows"], "test_rows": result["test_rows"],
            "test_period": result["test_period"], "last_period": period_label(result["last_period"]),
            "interval": [round(x, 3) for x in result["interval"]],
            "importance": [[name, round(value, 4)] for name, value in result["importance"][:8]],
        },
        "quarters": [[p // 4, p % 4 + 1] for p in periods],
        "pairs": {"cols": ["a", "b", "carrier", *[period_label(p) for p in periods], "last_year"], "rows": rows},
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M"),
    }
    target = Path(target)
    target.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(data, separators=(",", ":"), allow_nan=False)
    target.write_text("window.PRICE_DATASETS = window.PRICE_DATASETS || {};\n"
                      f"window.PRICE_DATASETS[{json.dumps(info.key)}] = {payload};\n", encoding="utf-8")
    log.info("Website price data written: %s (%.0f KB, %s pair-airline rows)", target, len(payload) / 1024, f"{len(rows):,}")
    return target


def run(spark: SparkSession, dataset: str, fares_path: Path, flights: DataFrame, locations: dict,
        fast: bool = False) -> dict:
    network = {name: df.cache() for name, df in network_features(flights).items()}
    fares = load_fares(spark, fares_path)
    result = train(fares, network, fast)
    model_path = locations["fare_model"]
    result["pipeline"].write().overwrite().save(str(model_path))
    log.info("Saved model: %s", model_path)
    predicted = forecast(spark, result, flights, network).cache()
    # Materialise once, so the single-file CSV writes below do not rerun the model in one task.
    log.info("Forecast rows: %s", f"{predicted.count():,}")
    for name, table in summary_tables(spark, result, predicted).items():
        write_csv(plain_numbers(table), locations["summaries"] / name)
        log.info("Created: %s", locations["summaries"] / name)
    write_price_data(result, predicted, locations["price_data"], dataset)
    return result


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Train the fare (journey price) prediction model with Spark MLlib")
    parser.add_argument("--dataset", choices=list(datasets.DATASETS), default=datasets.DEFAULT)
    parser.add_argument("--fares", help="Fare CSV (default: the dataset's fares.csv)")
    parser.add_argument("--processed", help="Cleaned flight Parquet (default: the dataset's)")
    parser.add_argument("--fast", action="store_true", help="Smaller forests for a quick run")
    parser.add_argument("--driver-memory", default="6g", help="Spark driver memory (default 6g)")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
    locations = datasets.paths(args.dataset, ROOT)
    processed = Path(args.processed or locations["processed"])
    if not processed.exists():
        raise FileNotFoundError(f"{processed} not found. Run python src/flight_delay_pipeline.py "
                                f"--dataset {args.dataset} first.")
    started = time.time()
    spark = build_spark(args.driver_memory)
    spark.sparkContext.setLogLevel("ERROR")
    try:
        run(spark, args.dataset, Path(args.fares or locations["fares_input"]), spark.read.parquet(str(processed)),
            locations, args.fast)
    finally:
        spark.stop()
    log.info("Fare model finished in %.0f s", time.time() - started)
    return 0


if __name__ == "__main__":
    sys.exit(main())
