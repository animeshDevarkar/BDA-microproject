"""Optional quick chart (needs `pip install matplotlib`). Saves output/monthly_delay_rate.png."""
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
df = pd.read_csv(ROOT / "output" / "powerbi" / "monthly_summary.csv").sort_values("year_month")
plt.figure(figsize=(10, 5))
plt.plot(df["year_month"], df["arrival_delay_rate_pct"], marker="o", color="#2a78d6", linewidth=2)
plt.title("Monthly Arrival Delay Rate")
plt.xlabel("Month")
plt.ylabel("Flights arriving 15+ minutes late (%)")
plt.xticks(rotation=45)
plt.grid(axis="y", color="#e1e0d9")
plt.tight_layout()
out = ROOT / "output" / "monthly_delay_rate.png"
plt.savefig(out, dpi=150)
print(f"Saved {out}")
