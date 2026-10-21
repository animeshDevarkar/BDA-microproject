"""Print the main summary tables with Pandas (small files only, never the raw data).

    python src/inspect_outputs.py            # International (USA)
    python src/inspect_outputs.py india      # Domestic (India)
"""
import sys
from pathlib import Path

import pandas as pd

import datasets

DATASET = sys.argv[1] if len(sys.argv) > 1 else datasets.DEFAULT
OUT = datasets.paths(DATASET, Path(__file__).resolve().parents[1])["summaries"]

pd.set_option("display.width", 160)
pd.set_option("display.max_columns", 12)

print(f"Dataset: {DATASET} ({datasets.DATASETS[DATASET].option}) - {OUT}")
for title, name, rows in [
    ("KPIs", "kpi_summary.csv", 20),
    ("Monthly summary", "monthly_summary.csv", 24),
    ("Airline summary", "airline_summary.csv", 10),
    ("Origin airports (by priority score)", "origin_airport_summary.csv", 10),
    ("Route summary (by priority score)", "route_summary.csv", 10),
    ("Delay causes", "delay_cause_summary.csv", 5),
]:
    frame = pd.read_csv(OUT / name)
    if "priority_score" in frame:
        frame = frame.sort_values("priority_score", ascending=False)
    print(f"\n=== {title} ({len(frame)} rows) ===")
    print(frame.head(rows).to_string(index=False))
