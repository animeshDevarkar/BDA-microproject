"""Download real U.S. airfares and build data/raw/fares.csv (training data for the fare model).

Source: U.S. DOT Consumer Airfare Report, Table 1a - All U.S. Airport Pair Markets
(data.transportation.gov dataset tfrh-tu9e). Each row is one airport pair in one quarter:
average one-way fare, nonstop distance, average daily passengers, plus the fare of the
largest carrier and of the lowest-fare carrier on that pair. It is built from DOT's 10%
ticket sample (DB1B), so it covers every domestic market, not only nonstop routes.

Examples:
    python src/download_fare_data.py                  # 2015 to the latest published quarter
    python src/download_fare_data.py --since 2019
"""
from __future__ import annotations

import argparse
import csv
import io
import sys
import time
import urllib.parse
import urllib.request
from datetime import date
from pathlib import Path

import datasets

ROOT = Path(__file__).resolve().parents[1]
LOCATIONS = datasets.paths("usa", ROOT)

ENDPOINT = "https://data.transportation.gov/resource/tfrh-tu9e.csv"
DATASET_PAGE = "https://data.transportation.gov/d/tfrh-tu9e"
PAGE_SIZE = 50000

# Table 1a columns, in the order the fare model and the India simulation use.
COLUMNS = ["year", "quarter", "airport_1", "airport_2", "city1", "city2", "nsmiles", "passengers",
           "fare", "carrier_lg", "large_ms", "fare_lg", "carrier_low", "lf_ms", "fare_low"]


def fetch_page(since: int, offset: int, retries: int = 3) -> list[dict]:
    query = urllib.parse.urlencode({
        "$select": ",".join(COLUMNS),
        "$where": f"year >= {since}",
        "$order": "year, quarter, airport_1, airport_2",
        "$limit": PAGE_SIZE,
        "$offset": offset,
    })
    for attempt in range(1, retries + 1):
        try:
            request = urllib.request.Request(f"{ENDPOINT}?{query}", headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(request, timeout=300) as response:
                text = response.read().decode("utf-8")
            return list(csv.DictReader(io.StringIO(text)))
        except OSError as exc:
            if attempt == retries:
                raise RuntimeError(f"Could not download fares (offset {offset}): {exc}") from exc
            print(f"  retrying after error: {exc}")
            time.sleep(5 * attempt)
    return []


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Download DOT airport-pair airfares (Table 1a)")
    parser.add_argument("--since", type=int, default=2015, help="First year to download (default 2015)")
    parser.add_argument("--output", default=str(LOCATIONS["fares_input"]))
    args = parser.parse_args(argv)

    rows: list[dict] = []
    while True:
        print(f"  fetching rows {len(rows):,}+ ...", flush=True)
        page = fetch_page(args.since, len(rows))
        rows.extend(page)
        if len(page) < PAGE_SIZE:
            break
    if not rows:
        print("No fare rows returned. Check the internet connection or the --since year.")
        return 1

    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=COLUMNS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)

    quarters = sorted({(int(r["year"]), int(r["quarter"])) for r in rows})
    first, last = quarters[0], quarters[-1]
    (out.parent / "FARES_SOURCE.txt").write_text(
        "Source: U.S. DOT Consumer Airfare Report, Table 1a - All U.S. Airport Pair Markets\n"
        f"URL: {DATASET_PAGE}\n"
        f"Period: {first[0]}Q{first[1]} to {last[0]}Q{last[1]}\n"
        f"Downloaded: {date.today().isoformat()}\n"
        f"Rows: {len(rows):,}\n"
        "Fares: average one-way fare in U.S. dollars (not inflation-adjusted)\n",
        encoding="utf-8",
    )
    print(f"Wrote {len(rows):,} airport-pair fares ({first[0]}Q{first[1]} - {last[0]}Q{last[1]}) to {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
