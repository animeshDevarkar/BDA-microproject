"""Create SIMULATED ticket bookings - the big data behind the trip cost calculator.

No public file lists individual bookings with the cabin, the booking date and the baggage
and seat fees paid, for the USA or for India. This script generates one booking per row on
the project's own flight network, so that Spark can learn how the cost of a trip is built up:

- routes and airlines: sampled in proportion to the flights in the cleaned flight Parquet
  (data/processed/.../flights_clean), so busy routes get more bookings;
- ticket fare: anchored on the pair's average fare for the travel quarter in data/raw/fares.csv
  (USA: real U.S. DOT fares; India: simulated), then moved by cabin, by how many days before
  departure the ticket is bought, by the day of the week and by random variation;
- taxes and airport fees: the U.S. domestic ticket taxes (7.5% excise, segment fee, security
  fee, passenger facility charge) or India's GST plus airport fees, at approximate published levels;
- extras: checked-bag fees (USA) or extra-baggage fees beyond the free 15 kg (India) and paid
  seat selection, at approximate published levels per airline.

About 0.3% of the rows are deliberately broken (duplicates, missing airports, negative fares,
travel before booking) so that the Spark cleaning step has real work to do.

The output is clearly labelled SIMULATED and must not be quoted as real bookings.

    python src/make_booking_data.py                    # International (USA), 3,000,000 bookings
    python src/make_booking_data.py --dataset india    # Domestic (India), 1,500,000 bookings
"""
from __future__ import annotations

import argparse
import time
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd

import datasets

ROOT = Path(__file__).resolve().parents[1]

COLUMNS = ["booking_id", "booking_date", "travel_date", "days_before_departure", "origin", "destination",
           "airline", "cabin", "trip_type", "passengers", "bags_each", "paid_seats", "distance_miles",
           "ticket_fare", "taxes_fees", "baggage_fee", "seat_fee", "total_paid"]
CABINS = ["Economy", "Premium Economy", "Business"]
DEFAULT_ROWS = {"usa": 3_000_000, "india": 1_500_000}
CHUNK = 500_000
DIRTY_SHARE = 0.003

# Fare multiplier by travel weekday (Mon..Sun): mid-week is cheapest, Friday and Sunday dearest.
WEEKDAY_FARE = np.array([1.01, 0.94, 0.95, 1.00, 1.06, 0.96, 1.08])

# Per dataset: who sells which cabin, what the extras cost and how the ticket is taxed.
# cabin_mix: share of bookings in Economy / Premium Economy / Business.
# bag_fee: (first unit, second unit) per traveller and direction; bag_noise: day-to-day price variation.
RULES = {
    "usa": {
        "currency": "USD",
        "bag_unit": "checked bag",
        "cabin_mix": {
            "legacy": (0.80, 0.11, 0.09), "jetblue": (0.87, 0.10, 0.03), "economy_only": (1.0, 0.0, 0.0),
            "regional": (0.88, 0.05, 0.07),
        },
        "airline_group": {"AA": "legacy", "DL": "legacy", "UA": "legacy", "AS": "legacy", "HA": "legacy",
                          "B6": "jetblue", "WN": "economy_only", "NK": "economy_only", "F9": "economy_only",
                          "G4": "economy_only"},
        "default_group": "regional",
        "bag_fee": {"AA": (38, 45), "DL": (35, 45), "UA": (38, 48), "AS": (35, 45), "HA": (35, 45),
                    "B6": (38, 55), "WN": (35, 45), "NK": (48, 68), "F9": (55, 72), "G4": (42, 50)},
        "default_bag_fee": (37, 45),
        "bag_noise": {"NK": 0.20, "F9": 0.20, "G4": 0.15},
        "default_bag_noise": 0.04,
        "bag_mix": (0.58, 0.36, 0.06),            # travellers checking 0 / 1 / 2 bags
        "low_cost": {"NK", "F9", "G4"},
        "low_cost_bag_mix": (0.68, 0.29, 0.03),
        "seat_share": 0.30, "low_cost_seat_share": 0.45,
        "seat_fee": lambda miles: 12 + 0.012 * miles,
        "cabin_fare": lambda miles: (1.0, 1.40, 2.3 + 0.25 * np.minimum(miles, 2500) / 1000),
        "round_trip_share": 0.62,
        # 7.5% excise tax on the base fare + $5.20 segment fee + $5.60 security fee + $4.50 facility charge.
        "tax_rate": (0.075, 0.075, 0.075), "tax_fixed": 15.30,
        "min_fare": 39.0,
    },
    "india": {
        "currency": "INR",
        "bag_unit": "extra 5 kg of baggage",
        "cabin_mix": {"full_service": (0.84, 0.08, 0.08), "economy_only": (1.0, 0.0, 0.0)},
        "airline_group": {"AI": "full_service"},
        "default_group": "economy_only",
        "bag_fee": {"6E": (2525, 2525), "AI": (2500, 2500), "IX": (2100, 2100), "QP": (2500, 2500),
                    "SG": (2400, 2400), "9I": (2000, 2000), "S5": (2200, 2200), "IC": (2000, 2000)},
        "default_bag_fee": (2400, 2400),
        "bag_noise": {},
        "default_bag_noise": 0.05,
        "bag_mix": (0.90, 0.08, 0.02),            # travellers buying 0 / 1 / 2 extra 5 kg blocks
        "low_cost": set(),
        "low_cost_bag_mix": (0.90, 0.08, 0.02),
        "seat_share": 0.35, "low_cost_seat_share": 0.35,
        "seat_fee": lambda miles: 250 + 0.25 * miles,
        "cabin_fare": lambda miles: (1.0, 1.55, 3.2 + 0.0 * miles),
        "round_trip_share": 0.35,
        # GST of 5% (economy) or 12% (premium cabins) on the base fare + airport fees (see airport_fees).
        "tax_rate": (0.05, 0.12, 0.12), "tax_fixed": None,
        "min_fare": 1500.0,
    },
}
INDIA_METROS = {"DEL", "BOM", "BLR", "HYD", "MAA", "CCU", "AMD", "COK", "PNQ"}
INDIA_SECURITY_FEE = 236.0  # aviation security fee incl. GST


def airport_fees(airports: list[str]) -> dict[str, float]:
    """Simulated user development fee per Indian departure airport + the aviation security fee."""
    rng = np.random.default_rng(91)
    fees = {}
    for code in sorted(airports):
        low, high = (350, 700) if code in INDIA_METROS else (150, 400)
        fees[code] = float(round(rng.uniform(low, high))) + INDIA_SECURITY_FEE
    return fees


def lead_fare(days: np.ndarray, cabin: np.ndarray) -> np.ndarray:
    """Fare multiplier by days before departure: last-minute tickets cost most, 3-8 weeks ahead least.
    Premium cabins move less with the booking date than economy."""
    curve = 0.88 + 0.72 * np.exp(-days / 6.0) + 0.0009 * np.maximum(days - 45, 0)
    curve = np.minimum(curve, 1.65)
    return np.where(cabin == 2, 1.0 + (curve - 1.0) * 0.45, curve)


# ---------------------------------------------------------------- inputs
def route_mix(processed: Path) -> pd.DataFrame:
    """Flights per origin, destination and airline in the cleaned flight Parquet."""
    import pyarrow.dataset as ds

    table = ds.dataset(str(processed), format="parquet", partitioning="hive").to_table(
        columns=["origin", "destination", "airline"])
    counts = table.group_by(["origin", "destination", "airline"]).aggregate([([], "count_all")])
    return counts.to_pandas().rename(columns={"count_all": "flights"})


def fare_anchors(fares_csv: Path) -> pd.DataFrame:
    """Latest reported fare per unordered airport pair and calendar quarter, with the pair's distance."""
    raw = pd.read_csv(fares_csv, usecols=["year", "quarter", "airport_1", "airport_2", "nsmiles", "fare",
                                          "carrier_lg", "fare_lg", "carrier_low", "fare_low"])
    first = raw["airport_1"] < raw["airport_2"]
    raw = raw.assign(a=raw["airport_1"].where(first, raw["airport_2"]),
                     b=raw["airport_2"].where(first, raw["airport_1"]))
    latest = raw.sort_values(["year", "quarter"]).groupby(["a", "b", "quarter"], as_index=False).last()
    return latest[["a", "b", "quarter", "nsmiles", "fare", "carrier_lg", "fare_lg", "carrier_low", "fare_low"]]


def priced_routes(mix: pd.DataFrame, anchors: pd.DataFrame, min_flights: int = 20) -> tuple[pd.DataFrame, np.ndarray]:
    """Route x airline rows that have fare data, and their anchor fare for quarters 1-4 (rows x 4)."""
    routes = mix[mix["flights"] >= min_flights].reset_index(drop=True).copy()
    first = routes["origin"] < routes["destination"]
    routes["a"] = routes["origin"].where(first, routes["destination"])
    routes["b"] = routes["destination"].where(first, routes["origin"])
    routes["row"] = np.arange(len(routes))
    joined = routes.merge(anchors, on=["a", "b"], how="inner")
    # The airline's own fare when the fare report names it, else the market average of the pair.
    own = np.where(joined["airline"] == joined["carrier_lg"], joined["fare_lg"],
                   np.where(joined["airline"] == joined["carrier_low"], joined["fare_low"], joined["fare"]))
    joined["anchor"] = np.where(np.isfinite(own) & (own > 0), own, joined["fare"])
    table = joined.pivot_table(index="row", columns="quarter", values="anchor", aggfunc="mean")
    table = table.reindex(columns=[1, 2, 3, 4])
    table = table.apply(lambda col: col.fillna(table.mean(axis=1)))  # quarters with no report: the pair's mean
    miles = joined.groupby("row")["nsmiles"].max()
    routes = routes[routes["row"].isin(table.index)].copy()
    routes["distance_miles"] = routes["row"].map(miles).astype(float)
    anchor = table.loc[routes["row"]].to_numpy(float)
    return routes.drop(columns=["a", "b", "row"]).reset_index(drop=True), anchor


# ---------------------------------------------------------------- generation
def generate_chunk(rng: np.random.Generator, n: int, first_id: int, routes: pd.DataFrame, anchor: np.ndarray,
                   dataset: str, start: date, end: date, fees: dict[str, float]) -> pd.DataFrame:
    rules = RULES[dataset]
    weights = routes["flights"].to_numpy(float)
    pick = rng.choice(len(routes), size=n, p=weights / weights.sum())
    airline = routes["airline"].to_numpy()[pick]
    origin = routes["origin"].to_numpy()[pick]
    destination = routes["destination"].to_numpy()[pick]
    miles = routes["distance_miles"].to_numpy()[pick]

    groups = pd.Series(airline).map(rules["airline_group"]).fillna(rules["default_group"]).to_numpy()
    mix = np.array([rules["cabin_mix"][g] for g in groups])
    u = rng.random(n)
    cabin = (u > mix[:, 0]).astype(int) + (u > mix[:, 0] + mix[:, 1]).astype(int)

    days_total = (end - start).days + 1
    travel = np.datetime64(start) + rng.integers(0, days_total, n).astype("timedelta64[D]")
    # Premium cabins are booked closer to departure (business travel).
    lead = np.minimum(rng.lognormal(np.where(cabin == 2, np.log(11), np.log(26)), 1.0), 330).astype(int)
    booked = travel - lead.astype("timedelta64[D]")
    weekday = (travel.astype("datetime64[D]").astype("int64") + 3) % 7  # 0 = Monday (1970-01-01 was a Thursday)
    quarter = (travel.astype("datetime64[M]").astype(int) % 12) // 3

    cabin_fares = np.stack(np.broadcast_arrays(*rules["cabin_fare"](miles), miles)[:3], axis=1)
    fare = (anchor[pick, quarter] * cabin_fares[np.arange(n), cabin] * lead_fare(lead, cabin)
            * WEEKDAY_FARE[weekday] * rng.lognormal(0, 0.16, n))
    fare = np.maximum(fare, rules["min_fare"]).round(2)

    rate = np.array(rules["tax_rate"])[cabin]
    fixed = (np.full(n, rules["tax_fixed"]) if rules["tax_fixed"] is not None
             else pd.Series(origin).map(fees).to_numpy(float))
    fixed = np.minimum(fixed, fare * 0.5)
    taxes = (fixed + (fare - fixed) * rate / (1 + rate)).round(2)

    round_trip = rng.random(n) < rules["round_trip_share"]
    directions = np.where(round_trip, 2, 1)
    passengers = rng.choice([1, 2, 3, 4, 5, 6], size=n, p=[0.55, 0.28, 0.08, 0.06, 0.02, 0.01])
    legs = passengers * directions

    low_cost = np.isin(airline, sorted(rules["low_cost"]))
    bag_mix = np.where(low_cost[:, None], rules["low_cost_bag_mix"], rules["bag_mix"])
    u = rng.random(n)
    bags = (u > bag_mix[:, 0]).astype(int) + (u > bag_mix[:, 0] + bag_mix[:, 1]).astype(int)
    fee_table = np.array([rules["bag_fee"].get(c, rules["default_bag_fee"]) for c in airline], float)
    noise = np.array([rules["bag_noise"].get(c, rules["default_bag_noise"]) for c in airline])
    level = np.maximum(rng.normal(1.0, noise), 0.5)
    per_leg = np.where(bags >= 1, fee_table[:, 0], 0) + np.where(bags == 2, fee_table[:, 1], 0)
    # Business tickets include the bags, so no fee is paid.
    baggage_fee = np.where(cabin == 2, 0.0, (per_leg * level).round() * legs)

    seat_share = np.where(low_cost, rules["low_cost_seat_share"], rules["seat_share"])
    paid_seats = np.where((cabin < 2) & (rng.random(n) < seat_share), passengers, 0)
    seat_each = (rules["seat_fee"](miles) * rng.lognormal(0, 0.3, n)).round()
    seat_fee = paid_seats * directions * seat_each

    total = (fare * legs + baggage_fee + seat_fee).round(2)
    return pd.DataFrame({
        "booking_id": np.arange(first_id, first_id + n),
        "booking_date": booked.astype("datetime64[D]").astype(str), "travel_date": travel.astype(str),
        "days_before_departure": lead, "origin": origin, "destination": destination, "airline": airline,
        "cabin": np.array(CABINS)[cabin], "trip_type": np.where(round_trip, "ROUND_TRIP", "ONE_WAY"),
        "passengers": passengers, "bags_each": bags, "paid_seats": paid_seats,
        "distance_miles": miles.round().astype(int), "ticket_fare": fare, "taxes_fees": taxes,
        "baggage_fee": baggage_fee, "seat_fee": seat_fee, "total_paid": total,
    }, columns=COLUMNS)


def add_dirty_rows(rng: np.random.Generator, frame: pd.DataFrame, share: float = DIRTY_SHARE) -> pd.DataFrame:
    """Break a small share of rows the way real exports break: duplicates, blanks, impossible values."""
    n = int(len(frame) * share)
    if n < 4:
        return frame
    frame = frame.copy()
    rows = rng.choice(len(frame), size=n, replace=False)
    parts = np.array_split(rows, 4)
    frame.iloc[parts[0], frame.columns.get_loc("ticket_fare")] = -1.0
    frame.iloc[parts[1], frame.columns.get_loc("origin")] = ""
    frame.iloc[parts[2], frame.columns.get_loc("travel_date")] = frame.iloc[parts[2]]["booking_date"].map(
        lambda d: str(np.datetime64(d) - np.timedelta64(3, "D")))
    return pd.concat([frame, frame.iloc[parts[3]]], ignore_index=True)  # exact duplicates


def generate(routes: pd.DataFrame, anchor: np.ndarray, dataset: str, rows: int, start: date, end: date,
             seed: int, dirty: bool = True, chunk: int = CHUNK):
    """Yield booking frames of up to ``chunk`` rows; the same seed always gives the same bookings."""
    rng = np.random.default_rng(seed)
    fees = airport_fees(sorted(set(routes["origin"]))) if dataset == "india" else {}
    done = 0
    while done < rows:
        n = min(chunk, rows - done)
        frame = generate_chunk(rng, n, done + 1, routes, anchor, dataset, start, end, fees)
        yield add_dirty_rows(rng, frame) if dirty else frame
        done += n


def travel_period(processed: Path) -> tuple[date, date]:
    """First and last day of the months in the flight Parquet (year_month=YYYY-MM folders)."""
    months = sorted(p.name.split("=")[1] for p in Path(processed).glob("year_month=*"))
    if not months:
        raise FileNotFoundError(f"No year_month partitions in {processed}")
    start = date(int(months[0][:4]), int(months[0][5:7]), 1)
    last = pd.Timestamp(f"{months[-1]}-01") + pd.offsets.MonthEnd(0)
    return start, last.date()


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Generate SIMULATED ticket bookings for the trip cost model")
    parser.add_argument("--dataset", choices=list(datasets.DATASETS), default=datasets.DEFAULT)
    parser.add_argument("--rows", type=int, help="Bookings to generate (default 3,000,000 USA / 1,500,000 India)")
    parser.add_argument("--seed", type=int, default=2026)
    parser.add_argument("--output", help="Output CSV (default: the dataset's bookings.csv)")
    args = parser.parse_args(argv)

    locations = datasets.paths(args.dataset, ROOT)
    for needed, hint in ((locations["processed"], f"python src/flight_delay_pipeline.py --dataset {args.dataset}"),
                         (locations["fares_input"], "python src/download_fare_data.py (USA) or "
                                                    "python src/make_india_fares.py (India)")):
        if not needed.exists():
            raise FileNotFoundError(f"{needed} not found. Run {hint} first.")
    started = time.time()
    rows = args.rows or DEFAULT_ROWS[args.dataset]
    routes, anchor = priced_routes(route_mix(locations["processed"]), fare_anchors(locations["fares_input"]))
    start, end = travel_period(locations["processed"])
    out = Path(args.output or locations["bookings_input"])
    out.parent.mkdir(parents=True, exist_ok=True)
    written = 0
    for i, frame in enumerate(generate(routes, anchor, args.dataset, rows, start, end, args.seed)):
        frame.to_csv(out, index=False, mode="w" if i == 0 else "a", header=i == 0)
        written += len(frame)
        print(f"  {written:,} rows written", flush=True)
    info = datasets.DATASETS[args.dataset]
    (out.parent / "BOOKINGS_SOURCE.txt").write_text(
        f"Source: SIMULATED ticket bookings for {info.country} (not real bookings)\n"
        "Method: src/make_booking_data.py - routes and airlines in proportion to the flight records; ticket "
        "fares anchored on the pair's average fare in fares.csv and moved by cabin, days before departure, "
        "weekday and random variation; taxes, baggage and seat fees at approximate published levels\n"
        f"Travel period: {start.isoformat()} to {end.isoformat()}\n"
        f"Generated: {date.today().isoformat()}\n"
        f"Rows: {written:,} (including about {DIRTY_SHARE:.1%} deliberately broken rows for the cleaning step)\n"
        f"Routes: {len(routes):,} route-airline combinations with fare data\n"
        f"Seed: {args.seed}\n"
        f"Amounts: {rules_currency(args.dataset)}\n",
        encoding="utf-8",
    )
    print(f"Wrote {written:,} simulated bookings to {out} ({out.stat().st_size / 1e6:.0f} MB, "
          f"{time.time() - started:.0f} s)")
    return 0


def rules_currency(dataset: str) -> str:
    return "U.S. dollars" if RULES[dataset]["currency"] == "USD" else "Indian rupees"


if __name__ == "__main__":
    raise SystemExit(main())
