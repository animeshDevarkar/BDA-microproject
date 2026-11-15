"""Create SIMULATED quarterly airfares for Indian domestic airport pairs (Domestic option).

India publishes no airport-pair fare file like the U.S. DOT Consumer Airfare Report, so
this script generates one in the same layout (Table 1a columns), which lets the fare
model train on both datasets unchanged. The fares use the simulated flight network
(data/raw/india/flights.csv): the airlines on each pair, their shares and the pair's traffic.
They are shaped by published patterns of the Indian domestic market:

- distance: about Rs 2,700 for 300 km, Rs 5,700 for Delhi-Mumbai (1,150 km), Rs 7,300 for 1,700 km;
- airline: Air India (full service) highest, Air India Express / SpiceJet / Akasa below IndiGo,
  Alliance Air lowest (regional UDAN fares);
- competition: single-airline pairs cost more, pairs with four or more airlines less;
- seasons: summer holidays (Apr-Jun) and the festive season (Oct-Dec) are dearer, the monsoon
  quarter cheaper; Goa peaks in winter, Srinagar/Leh/Dehradun in summer;
- history: about 3.5% fare growth a year, the 2020-21 COVID slump, the 2023 Go First grounding;
  Akasa flies from 2022Q3 and Fly91 from 2024Q1.

The output is clearly labelled SIMULATED and must not be quoted as real fares.

    python src/make_india_fares.py                   # 2015Q1 - 2026Q2
"""
from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd

import datasets
from download_fare_data import COLUMNS
from make_india_data import AIRPORTS

ROOT = Path(__file__).resolve().parents[1]
LOCATIONS = datasets.paths("india", ROOT)

# Airport reference points (degrees), for great-circle distances.
COORDS = {
    "DEL": (28.567, 77.103), "BOM": (19.090, 72.866), "BLR": (13.199, 77.707), "HYD": (17.240, 78.429),
    "MAA": (12.994, 80.171), "CCU": (22.655, 88.447), "AMD": (23.077, 72.635), "COK": (10.152, 76.402),
    "PNQ": (18.582, 73.920), "GOX": (15.730, 73.862), "GOI": (15.381, 73.831), "JAI": (26.824, 75.812),
    "LKO": (26.761, 80.889), "GAU": (26.106, 91.586), "TRV": (8.482, 76.920), "PAT": (25.591, 85.088),
    "BBI": (20.244, 85.818), "IXC": (30.674, 76.789), "SXR": (33.987, 74.774), "NAG": (21.092, 79.047),
    "VNS": (25.452, 82.859), "IXB": (26.681, 88.329), "IDR": (22.722, 75.801), "CCJ": (11.137, 75.955),
    "IXE": (12.961, 74.890), "CJB": (11.030, 77.043), "VTZ": (17.721, 83.225), "IXR": (23.314, 85.322),
    "RPR": (21.180, 81.739), "BDQ": (22.336, 73.226), "ATQ": (31.710, 74.797), "DED": (30.190, 78.180),
    "IXZ": (11.641, 92.730), "IMF": (24.760, 93.897), "IXJ": (32.689, 74.837), "UDR": (24.618, 73.896),
    "TRZ": (10.765, 78.710), "IXM": (9.835, 78.093), "STV": (21.114, 72.742), "BHO": (23.288, 77.337),
    "HBX": (15.362, 75.085), "IXL": (34.136, 77.547), "DIB": (27.484, 95.017),
}

AIRLINE_FARE = {"6E": 1.00, "AI": 1.14, "IX": 0.92, "QP": 0.97, "SG": 0.94, "9I": 0.82, "S5": 1.05, "IC": 1.00}
AIRLINE_START = {"QP": (2022, 3), "IC": (2024, 1)}  # first quarter each airline flies
QUARTER_FARE = {1: 0.96, 2: 1.06, 3: 0.90, 4: 1.10}
COMPETITION = {1: 1.12, 2: 1.04, 3: 1.00}           # 4+ airlines: 0.95
BEACH, HILLS = {"GOI", "GOX"}, {"SXR", "IXL", "DED"}
SEATS_PER_FLIGHT = 180 * 0.85                        # A320-family seats x load factor


def great_circle_miles(a: str, b: str) -> float:
    (lat1, lon1), (lat2, lon2) = (np.radians(COORDS[a]), np.radians(COORDS[b]))
    h = np.sin((lat2 - lat1) / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin((lon2 - lon1) / 2) ** 2
    return float(2 * 3958.8 * np.arcsin(np.sqrt(h)))


def history_factors(year: int, quarter: int) -> tuple[float, float]:
    """(fare factor, traffic factor) for one quarter relative to 2026."""
    fare = 1.035 ** (year - 2026)
    traffic = 1.08 ** (year - 2026)
    if (year, quarter) in {(2020, 3), (2020, 4), (2021, 1), (2021, 2)}:
        fare, traffic = fare * 0.86, traffic * 0.45
    elif (year, quarter) in {(2021, 3), (2021, 4)}:
        fare, traffic = fare * 0.95, traffic * 0.70
    elif (year, quarter) in {(2023, 2), (2023, 3), (2023, 4)}:
        fare *= 1.12  # capacity shortage after Go First stopped flying
    return fare, traffic


def pair_mix(flights_csv: Path) -> pd.DataFrame:
    """Flights per unordered airport pair and airline in the simulated year."""
    raw = pd.read_csv(flights_csv, usecols=["Origin", "Dest", "Reporting_Airline"])
    first = raw["Origin"] < raw["Dest"]
    return (raw.assign(a=raw["Origin"].where(first, raw["Dest"]), b=raw["Dest"].where(first, raw["Origin"])).groupby(["a", "b", "Reporting_Airline"]).size()
            .rename("flights").reset_index())


def generate(mix: pd.DataFrame, start: tuple[int, int], end: tuple[int, int], seed: int) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    quarters = [(y, q) for y in range(start[0], end[0] + 1) for q in range(1, 5)
                if start <= (y, q) <= end and (y, q) != (2020, 2)]  # no scheduled flights in 2020Q2
    rows = []
    for (a, b), group in mix.groupby(["a", "b"]):
        miles = great_circle_miles(a, b)
        km = miles * 1.609
        base = 900 + 30 * km ** 0.72
        pair_level = rng.lognormal(0, 0.07)  # demand that distance alone does not explain
        pair_flights = group["flights"].sum()
        for year, quarter in quarters:
            live = group[[AIRLINE_START.get(c, (0, 0)) <= (year, quarter) for c in group["Reporting_Airline"]]]
            if live.empty:
                continue
            share = live["flights"].to_numpy() / live["flights"].sum()
            fare_f, traffic_f = history_factors(year, quarter)
            season = QUARTER_FARE[quarter]
            if {a, b} & BEACH:
                season *= {4: 1.15, 1: 1.05, 3: 0.92}.get(quarter, 1.0)
            if {a, b} & HILLS:
                season *= {2: 1.15, 1: 0.92}.get(quarter, 1.0)
            competition = COMPETITION.get(len(live), 0.95)
            carrier_fares = np.array([
                base * pair_level * fare_f * season * competition * AIRLINE_FARE.get(c, 1.0)
                * rng.lognormal(0, 0.06) for c in live["Reporting_Airline"]
            ])
            carriers = live["Reporting_Airline"].to_numpy()
            lg = int(np.argmax(share))
            eligible = np.where(share >= 0.1)[0]
            low = int(eligible[np.argmin(carrier_fares[eligible])])
            passengers = pair_flights / 365 * SEATS_PER_FLIGHT * traffic_f * rng.lognormal(0, 0.05)
            rows.append({
                "year": year, "quarter": quarter, "airport_1": a, "airport_2": b,
                "city1": f"{AIRPORTS[a][0]}, {AIRPORTS[a][1]}", "city2": f"{AIRPORTS[b][0]}, {AIRPORTS[b][1]}",
                "nsmiles": round(miles), "passengers": round(passengers, 2),
                "fare": round(float(share @ carrier_fares), 2),
                "carrier_lg": carriers[lg], "large_ms": round(float(share[lg]), 4),
                "fare_lg": round(float(carrier_fares[lg]), 2),
                "carrier_low": carriers[low], "lf_ms": round(float(share[low]), 4),
                "fare_low": round(float(carrier_fares[low]), 2),
            })
    return pd.DataFrame(rows, columns=COLUMNS)


def parse_quarter(text: str) -> tuple[int, int]:
    year, quarter = text.upper().split("Q")
    return int(year), int(quarter)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Generate SIMULATED Indian domestic airport-pair fares")
    parser.add_argument("--start", default="2015Q1", help="First quarter, e.g. 2015Q1")
    parser.add_argument("--end", default="2026Q2", help="Last quarter, e.g. 2026Q2")
    parser.add_argument("--flights", default=str(LOCATIONS["input"]), help="Simulated India flights CSV")
    parser.add_argument("--seed", type=int, default=2026)
    parser.add_argument("--output", default=str(LOCATIONS["fares_input"]))
    args = parser.parse_args(argv)

    flights = Path(args.flights)
    if not flights.exists():
        raise FileNotFoundError(f"{flights} not found. Run python src/make_india_data.py first.")
    start, end = parse_quarter(args.start), parse_quarter(args.end)
    frame = generate(pair_mix(flights), start, end, args.seed)
    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(out, index=False)
    (out.parent / "FARES_SOURCE.txt").write_text(
        "Source: SIMULATED Indian domestic airport-pair fares (not real ticket prices)\n"
        "Method: src/make_india_fares.py - distance-based fare curve, airline, competition, season and "
        "history effects on the simulated flight network; same columns as U.S. DOT Table 1a\n"
        f"Period: {start[0]}Q{start[1]} to {end[0]}Q{end[1]}\n"
        f"Downloaded: {date.today().isoformat()}\n"
        f"Rows: {len(frame):,}\n"
        f"Seed: {args.seed}\n"
        "Fares: average one-way fare in Indian rupees\n",
        encoding="utf-8",
    )
    print(f"Wrote {len(frame):,} simulated pair-quarter fares to {out} "
          f"(average fare Rs {frame['fare'].mean():,.0f})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
