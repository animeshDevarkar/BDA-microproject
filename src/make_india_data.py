"""Create a SIMULATED year of Indian domestic flights (the "Domestic (India)" option).

India's regulator (DGCA) publishes only monthly on-time-performance (OTP) percentages per
airline at a few metro airports; there is no public flight-by-flight file like the U.S.
BTS data. So this script generates flight-level records with real airports, airlines and a
realistic route network, and calibrates the delay rates to published DGCA OTP levels:

- airline late-arrival rates near DGCA OTP (IndiGo/Akasa most punctual, SpiceJet least);
- winter fog at north Indian airports (Dec-Feb), south-west monsoon (Jun-Sep) on the
  west coast, north-east monsoon (Oct-Dec) in Tamil Nadu;
- congestion that builds through the day and at Delhi, Mumbai and Bengaluru;
- reactionary (late-aircraft) delay as the largest cause, as DGCA reports.

The output uses the same columns as the BTS file, so the PySpark pipeline runs unchanged.
It is clearly labelled SIMULATED and must not be quoted as real-world evidence.

    python src/make_india_data.py                       # Sep 2025 - Aug 2026, ~1.15M flights
    python src/make_india_data.py --flights-per-day 500 --output data/raw/india/small.csv
"""
from __future__ import annotations

import argparse
from datetime import date, datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUT = ROOT / "data" / "raw" / "india" / "flights.csv"

# code: (city, state, relative traffic weight, climate flags, region)
# flags: F = winter fog, M = south-west monsoon, N = north-east monsoon, C = congested hub
AIRPORTS = {
    "DEL": ("Delhi", "Delhi", 100, "FC", "N"), "BOM": ("Mumbai", "Maharashtra", 80, "MC", "W"),
    "BLR": ("Bengaluru", "Karnataka", 70, "C", "S"), "HYD": ("Hyderabad", "Telangana", 45, "", "S"),
    "MAA": ("Chennai", "Tamil Nadu", 35, "N", "S"), "CCU": ("Kolkata", "West Bengal", 30, "", "E"),
    "AMD": ("Ahmedabad", "Gujarat", 20, "", "W"), "COK": ("Kochi", "Kerala", 18, "M", "S"),
    "PNQ": ("Pune", "Maharashtra", 16, "M", "W"), "GOX": ("Goa (Mopa)", "Goa", 8, "M", "W"),
    "GOI": ("Goa (Dabolim)", "Goa", 8, "M", "W"), "JAI": ("Jaipur", "Rajasthan", 10, "F", "N"),
    "LKO": ("Lucknow", "Uttar Pradesh", 10, "F", "N"), "GAU": ("Guwahati", "Assam", 10, "", "E"),
    "TRV": ("Thiruvananthapuram", "Kerala", 6, "M", "S"), "PAT": ("Patna", "Bihar", 8, "F", "E"),
    "BBI": ("Bhubaneswar", "Odisha", 8, "", "E"), "IXC": ("Chandigarh", "Chandigarh", 7, "F", "N"),
    "SXR": ("Srinagar", "Jammu and Kashmir", 7, "F", "N"), "NAG": ("Nagpur", "Maharashtra", 6, "", "W"),
    "VNS": ("Varanasi", "Uttar Pradesh", 6, "F", "N"), "IXB": ("Bagdogra", "West Bengal", 6, "", "E"),
    "IDR": ("Indore", "Madhya Pradesh", 7, "", "W"), "CCJ": ("Kozhikode", "Kerala", 4, "M", "S"),
    "IXE": ("Mangaluru", "Karnataka", 3, "M", "S"), "CJB": ("Coimbatore", "Tamil Nadu", 4, "N", "S"),
    "VTZ": ("Visakhapatnam", "Andhra Pradesh", 5, "", "S"), "IXR": ("Ranchi", "Jharkhand", 4, "F", "E"),
    "RPR": ("Raipur", "Chhattisgarh", 4, "", "E"), "BDQ": ("Vadodara", "Gujarat", 3, "", "W"),
    "ATQ": ("Amritsar", "Punjab", 4, "F", "N"), "DED": ("Dehradun", "Uttarakhand", 3, "F", "N"),
    "IXZ": ("Port Blair", "Andaman and Nicobar", 3, "M", "E"), "IMF": ("Imphal", "Manipur", 3, "", "E"),
    "IXJ": ("Jammu", "Jammu and Kashmir", 3, "F", "N"), "UDR": ("Udaipur", "Rajasthan", 3, "", "N"),
    "TRZ": ("Tiruchirappalli", "Tamil Nadu", 2, "N", "S"), "IXM": ("Madurai", "Tamil Nadu", 2, "N", "S"),
    "STV": ("Surat", "Gujarat", 2, "", "W"), "BHO": ("Bhopal", "Madhya Pradesh", 3, "", "W"),
    "HBX": ("Hubballi", "Karnataka", 1.5, "", "S"), "IXL": ("Leh", "Ladakh", 1.5, "F", "N"),
    "DIB": ("Dibrugarh", "Assam", 1.5, "", "E"),
}

# code: (market share, target late-arrival %, base cancellation %, network)
# Late % approximates 100 - DGCA OTP for 2025-26 (e.g. July 2026: IndiGo 91.2% on time,
# Akasa 90.8%, SpiceJet 34.5%); yearly targets include the fog and monsoon months.
# network: "all" = any route, "trunk" = routes touching a metro, "regional" = small airports.
AIRLINES = {
    "6E": (0.640, 14.0, 0.6, "all"),       # IndiGo
    "AI": (0.170, 27.0, 1.0, "trunk"),     # Air India (incl. merged Vistara)
    "IX": (0.090, 25.0, 1.2, "all"),       # Air India Express
    "QP": (0.050, 15.0, 0.7, "trunk"),     # Akasa Air
    "SG": (0.030, 50.0, 2.5, "trunk"),     # SpiceJet
    "9I": (0.012, 32.0, 3.0, "regional"),  # Alliance Air
    "S5": (0.005, 20.0, 1.5, "regional"),  # Star Air
    "IC": (0.003, 18.0, 1.5, "regional"),  # Fly91
}
METROS = {"DEL", "BOM", "BLR", "HYD", "MAA", "CCU", "AMD", "COK", "PNQ"}

# Scheduled departures by hour (early-morning and evening banks, a few red-eyes).
HOUR_WEIGHTS = np.array([2, 1.5, 1, 0.5, 1, 4, 7, 8, 7, 6, 5, 5, 5, 5, 5, 5, 6, 7, 8, 8, 7, 6, 4, 3], float)
CAUSES = ["CarrierDelay", "WeatherDelay", "NASDelay", "SecurityDelay", "LateAircraftDelay"]
CAUSE_WEIGHTS = np.array([0.27, 0.05, 0.22, 0.01, 0.45])  # reactionary delay dominates in DGCA data

COLUMNS = ["Year", "Month", "DayofMonth", "DayOfWeek", "FlightDate", "Reporting_Airline",
           "Origin", "OriginCityName", "OriginState", "Dest", "DestCityName", "DestState",
           "CRSDepTime", "DepDelay", "ArrDelay", "Cancelled", "Diverted", *CAUSES]


def season_factor(flags: str, month: int) -> float:
    factor = 1.0
    if "F" in flags:
        factor = max(factor, {12: 1.9, 1: 2.1, 2: 1.3}.get(month, 1.0))
    if "M" in flags:
        factor = max(factor, {6: 1.4, 7: 1.7, 8: 1.5, 9: 1.2}.get(month, 1.0))
    if "N" in flags:
        factor = max(factor, {10: 1.2, 11: 1.6, 12: 1.4}.get(month, 1.0))
    return factor


def build_routes(rng: np.random.Generator) -> tuple[list[tuple[str, str]], np.ndarray, np.ndarray]:
    """Gravity-model network: busy pairs get more flights; airlines fly the routes that fit them."""
    codes = list(AIRPORTS)
    pairs, weights = [], []
    for o in codes:
        for d in codes:
            if o == d:
                continue
            wo, wd = AIRPORTS[o][2], AIRPORTS[d][2]
            if o not in METROS and d not in METROS and min(wo, wd) < 5:
                continue  # small-to-small pairs rarely have nonstop service
            bonus = 1.5 if AIRPORTS[o][4] == AIRPORTS[d][4] else 1.0
            pairs.append((o, d))
            weights.append((wo * wd) ** 0.85 * bonus)
    weights = np.array(weights)
    keep = weights >= np.quantile(weights, 0.25)  # thin out the very smallest pairs
    pairs = [p for p, k in zip(pairs, keep) if k]
    weights = weights[keep]

    names = list(AIRLINES)
    mix = np.zeros((len(pairs), len(names)))
    for i, (o, d) in enumerate(pairs):
        small = min(AIRPORTS[o][2], AIRPORTS[d][2])
        metro = o in METROS or d in METROS
        for j, code in enumerate(names):
            share, _, _, network = AIRLINES[code]
            if network == "trunk" and not metro:
                continue
            if network == "regional" and small >= 8:
                continue
            if code != "6E" and not (o in METROS and d in METROS) and rng.random() < 0.35:
                continue  # not every airline flies every route (metro-to-metro trunks get all of them)
            mix[i, j] = share
        if mix[i].sum() == 0:
            mix[i, 0] = 1.0
    # Rebalance so each airline's overall share matches its market share (iterative fitting).
    route_p = weights / weights.sum()
    target = np.array([AIRLINES[c][0] for c in names])
    for _ in range(50):
        mix /= mix.sum(axis=1, keepdims=True)
        realised = route_p @ mix
        mix *= np.where(realised > 0, target / np.maximum(realised, 1e-12), 0)
    mix /= mix.sum(axis=1, keepdims=True)
    return pairs, route_p, np.cumsum(mix, axis=1)


def generate(start: date, end: date, flights_per_day: int, seed: int) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    pairs, route_p, airline_cum = build_routes(rng)
    names = list(AIRLINES)

    days = [start + timedelta(days=i) for i in range((end - start).days + 1)]
    # Weekly rhythm plus gentle growth over the year.
    day_weight = np.array([(1.06 if d.weekday() in (4, 6) else 0.94 if d.weekday() == 5 else 1.0)
                           * (1 + 0.06 * i / len(days)) for i, d in enumerate(days)])
    total = int(flights_per_day * len(days))
    day_idx = rng.choice(len(days), size=total, p=day_weight / day_weight.sum())
    day_idx.sort()
    route_idx = rng.choice(len(pairs), size=total, p=route_p)
    u = rng.random(total)
    airline_idx = (u[:, None] > airline_cum[route_idx]).sum(axis=1).clip(0, len(names) - 1)
    hour = rng.choice(24, size=total, p=HOUR_WEIGHTS / HOUR_WEIGHTS.sum())
    minute = rng.choice(np.arange(0, 60, 5), size=total)

    dates = np.array(days, dtype="datetime64[D]")[day_idx]
    month = (dates.astype("datetime64[M]").astype(int) % 12) + 1
    weekday = (dates.astype("datetime64[D]").view("int64") - 4) % 7 + 1  # 1970-01-01 was a Thursday; Monday=1

    origin = np.array([pairs[i][0] for i in range(len(pairs))])[route_idx]
    dest = np.array([pairs[i][1] for i in range(len(pairs))])[route_idx]
    flags = {c: v[3] for c, v in AIRPORTS.items()}

    # Seasonal factor = worse of the two ends (fog at either airport holds the flight).
    season_table = {(c, m): season_factor(flags[c], m) for c in AIRPORTS for m in range(1, 13)}
    lookup = pd.Series(season_table)
    s_origin = lookup.reindex(pd.MultiIndex.from_arrays([origin, month])).to_numpy()
    s_dest = lookup.reindex(pd.MultiIndex.from_arrays([dest, month])).to_numpy()
    season = np.maximum(s_origin, s_dest)
    hour_f = np.interp(hour, [0, 5, 7, 12, 16, 19, 21, 23], [1.1, 0.6, 0.65, 0.9, 1.05, 1.3, 1.3, 1.15])
    weekday_f = np.where(np.isin(weekday, [5, 7]), 1.08, np.where(weekday == 6, 0.94, 1.0))
    hub_f = np.where(np.isin(origin, ["DEL", "BOM", "BLR"]), 1.12, 1.0) * np.where(np.isin(dest, ["DEL", "BOM"]), 1.06, 1.0)
    noise = rng.lognormal(0, 0.15, size=total)
    raw = season * hour_f * weekday_f * hub_f * noise

    # Scale each airline so its yearly late rate lands on its DGCA-based target.
    p_late = np.zeros(total)
    p_cancel = np.zeros(total)
    for j, code in enumerate(names):
        mask = airline_idx == j
        if not mask.any():
            continue
        _, late_pct, cancel_pct, _ = AIRLINES[code]
        p_late[mask] = late_pct / 100 * raw[mask] / raw[mask].mean()
        p_cancel[mask] = cancel_pct / 100 * (1 + 1.5 * (season[mask] - 1))
    p_late = p_late.clip(0, 0.92)

    cancelled = rng.random(total) < p_cancel.clip(0, 0.3)
    diverted = ~cancelled & (rng.random(total) < 0.0012 * season ** 2)
    late = ~cancelled & ~diverted & (rng.random(total) < p_late)

    arr = np.where(late, 15 + rng.gamma(1.3, 28 * np.sqrt(season), size=total),
                   np.clip(rng.normal(-5, 7, size=total), -40, 14))
    arr = np.round(arr)
    dep = np.round(arr + rng.normal(4, 6, size=total))
    dep = np.where(late, np.maximum(dep, arr - 10), dep)

    # Delay causes (minutes) for late arrivals only, summing to the arrival delay like BTS.
    weather_boost = np.where(season > 1, season ** 3, 1.0)
    w = np.tile(CAUSE_WEIGHTS, (total, 1))
    w[:, 1] *= weather_boost
    w[:, 2] *= hub_f
    split = rng.gamma(w * 4)
    split = split / split.sum(axis=1, keepdims=True)
    cause_minutes = np.floor(split * arr[:, None])
    cause_minutes[:, 4] += arr - cause_minutes.sum(axis=1)

    city_names = {c: f"{v[0]}, {v[1]}" for c, v in AIRPORTS.items()}
    states = {c: v[1] for c, v in AIRPORTS.items()}
    frame = pd.DataFrame({
        "Year": dates.astype("datetime64[Y]").astype(int) + 1970,
        "Month": month,
        "DayofMonth": (dates - dates.astype("datetime64[M]")).astype(int) + 1,
        "DayOfWeek": weekday,
        "FlightDate": pd.to_datetime(dates).strftime("%Y-%m-%d"),
        "Reporting_Airline": np.array(names)[airline_idx],
        "Origin": origin,
        "OriginCityName": pd.Series(origin).map(city_names).to_numpy(),
        "OriginState": pd.Series(origin).map(states).to_numpy(),
        "Dest": dest,
        "DestCityName": pd.Series(dest).map(city_names).to_numpy(),
        "DestState": pd.Series(dest).map(states).to_numpy(),
        "CRSDepTime": hour * 100 + minute,
        "DepDelay": np.where(cancelled, np.nan, dep),
        "ArrDelay": np.where(cancelled | diverted, np.nan, arr),
        "Cancelled": cancelled.astype(float),
        "Diverted": diverted.astype(float),
    })
    for k, cause in enumerate(CAUSES):
        frame[cause] = np.where(late, cause_minutes[:, k], np.nan)
    return frame[COLUMNS]


def month_start(text: str) -> date:
    return datetime.strptime(text, "%Y-%m").date().replace(day=1)


def month_end(text: str) -> date:
    first = month_start(text)
    return (first.replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Generate SIMULATED Indian domestic flight records")
    parser.add_argument("--start", default="2025-09", help="First month, YYYY-MM (default 2025-09)")
    parser.add_argument("--end", default="2026-08", help="Last month, YYYY-MM (default 2026-08)")
    parser.add_argument("--flights-per-day", type=int, default=3150,
                        help="Average scheduled domestic departures per day (default 3150)")
    parser.add_argument("--seed", type=int, default=2026)
    parser.add_argument("--output", default=str(DEFAULT_OUT))
    args = parser.parse_args(argv)

    start, end = month_start(args.start), month_end(args.end)
    frame = generate(start, end, args.flights_per_day, args.seed)
    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(out, index=False, float_format="%.0f")
    (out.parent / "DATA_SOURCE.txt").write_text(
        "Source: SIMULATED Indian domestic flights, calibrated to DGCA on-time performance levels "
        "(not real flight records)\n"
        "Method: src/make_india_data.py - real airports, airlines and route network; delay rates "
        "matched to DGCA monthly OTP by airline, with fog, monsoon and congestion effects\n"
        "References: DGCA monthly domestic traffic & OTP releases; data.gov.in 'Airline-wise "
        "Details of Monthly On-Time Performance Data in Respect of Scheduled Domestic Airlines'\n"
        f"Period: {start:%Y-%m} to {end:%Y-%m}\n"
        f"Downloaded: {date.today().isoformat()}\n"
        f"Rows: {len(frame):,}\n"
        f"Seed: {args.seed}\n",
        encoding="utf-8",
    )
    late = (frame["ArrDelay"] >= 15).sum() / frame["ArrDelay"].notna().sum() * 100
    print(f"Wrote {len(frame):,} simulated flights to {out} (late arrivals {late:.1f}%)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
