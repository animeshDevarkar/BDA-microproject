"""Bundle the summary CSVs into dashboard/data-<dataset>.js for the offline web dashboard.

The dashboard (dashboard/index.html) is a static page that opens directly from
disk - no server or internet connection needed. It loads every dataset file and
offers a switch between them. Run this after the pipeline (the pipeline calls it
automatically):

    python src/build_dashboard.py                 # International (USA)
    python src/build_dashboard.py --dataset india # Domestic (India)
"""
from __future__ import annotations

import json
import math
from datetime import datetime
from pathlib import Path

import pandas as pd

import datasets

ROOT = Path(__file__).resolve().parents[1]
DASHBOARD_DIR = ROOT / "dashboard"

TABLES = {
    "kpi": "kpi_summary.csv",
    "monthly": "monthly_summary.csv",
    "airlines": "airline_summary.csv",
    "origins": "origin_airport_summary.csv",
    "destinations": "destination_airport_summary.csv",
    "routes": "route_summary.csv",
    "hourly": "hourly_summary.csv",
    "weekday": "weekday_summary.csv",
    "causes": "delay_cause_summary.csv",
}
OPTIONAL = {"weekday"}


def _clean(value):
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    return value


def read_table(path: Path) -> list[dict]:
    try:
        frame = pd.read_csv(path, keep_default_na=True)
    except pd.errors.EmptyDataError:
        return []
    records = frame.to_dict(orient="records")
    return [{key: _clean(value) for key, value in row.items()} for row in records]


def read_key_values(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    result = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if ":" in line:
            key, value = line.split(":", 1)
            result[key.strip()] = value.strip()
    return result


def build(summary_dir: Path | None = None, dataset: str = datasets.DEFAULT) -> Path:
    locations = datasets.paths(dataset, ROOT)
    info = datasets.DATASETS[dataset]
    summary_dir = Path(summary_dir or locations["summaries"])
    data: dict = {}
    for key, filename in TABLES.items():
        path = summary_dir / filename
        if not path.exists():
            if key in OPTIONAL:
                data[key] = []
                continue
            raise FileNotFoundError(f"{path} not found. Run the pipeline first.")
        data[key] = read_table(path)

    data["kpi"] = {row["metric"]: row["value"] for row in data["kpi"]}
    source = read_key_values(locations["source"])
    run = read_key_values(locations["run_summary"])
    data["meta"] = {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "source": source.get("Source", "Unknown source - see data/raw/DATA_SOURCE.txt"),
        "period": source.get("Period"),
        "downloaded": source.get("Downloaded"),
        "synthetic": "SYNTHETIC" in source.get("Source", "").upper(),
        "simulated": info.simulated or "SIMULATED" in source.get("Source", "").upper(),
        "dataset": info.key,
        "option": info.option,
        "country": info.country,
        "region_name": info.region_name,
        "source_name": info.source_name,
        "threshold": run.get("Delay threshold", "arrival delay >= 15 minutes"),
        "min_flights": run.get("Minimum flights", "airline/airport 100, route 50"),
        "run_at": run.get("Run at"),
    }

    DASHBOARD_DIR.mkdir(parents=True, exist_ok=True)
    target = DASHBOARD_DIR / f"data-{dataset}.js"
    payload = json.dumps(data, separators=(",", ":"), allow_nan=False)
    target.write_text("window.DASHBOARD_DATASETS = window.DASHBOARD_DATASETS || {};\n"
                      f"window.DASHBOARD_DATASETS[{json.dumps(dataset)}] = {payload};\n", encoding="utf-8")
    print(f"Dashboard data written: {target} ({len(payload) / 1024:.0f} KB)")
    print(f"Open the dashboard: {DASHBOARD_DIR / 'index.html'}")
    return target


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Bundle summary CSVs for the dashboard")
    parser.add_argument("summary_dir", nargs="?", help="Summary CSV folder (default: the dataset's)")
    parser.add_argument("--dataset", choices=list(datasets.DATASETS), default=datasets.DEFAULT)
    args = parser.parse_args()
    build(Path(args.summary_dir) if args.summary_dir else None, args.dataset)
