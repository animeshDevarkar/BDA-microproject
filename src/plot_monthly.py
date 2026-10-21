"""Optional quick chart (needs `pip install matplotlib`). Saves monthly_delay_rate.png next to the summaries.

    python src/plot_monthly.py           # International (USA)
    python src/plot_monthly.py india     # Domestic (India)
"""
import sys
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

import datasets

ROOT = Path(__file__).resolve().parents[1]
DATASET = sys.argv[1] if len(sys.argv) > 1 else datasets.DEFAULT
SUMMARIES = datasets.paths(DATASET, ROOT)["summaries"]
df = pd.read_csv(SUMMARIES / "monthly_summary.csv").sort_values("year_month")
plt.figure(figsize=(10, 5))
plt.plot(df["year_month"], df["arrival_delay_rate_pct"], marker="o", color="#2a78d6", linewidth=2)
plt.title(f"Monthly Arrival Delay Rate - {datasets.DATASETS[DATASET].option} ({datasets.DATASETS[DATASET].country})")
plt.xlabel("Month")
plt.ylabel("Flights arriving 15+ minutes late (%)")
plt.xticks(rotation=45)
plt.grid(axis="y", color="#e1e0d9")
plt.tight_layout()
out = SUMMARIES.parent / "monthly_delay_rate.png"
plt.savefig(out, dpi=150)
print(f"Saved {out}")
