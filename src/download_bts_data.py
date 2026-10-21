"""Download official BTS Airline On-Time Performance data and build data/raw/flights.csv.

Uses the monthly pre-zipped files published by the U.S. Bureau of Transportation
Statistics (TranStats). Only the columns the pipeline needs are kept, which makes
the raw CSV roughly 4x smaller than the original extract.

Examples:
    python src/download_bts_data.py --start 2026-06 --end 2026-08
    python src/download_bts_data.py --start 2025-09 --end 2026-08
"""
from __future__ import annotations

import argparse
import csv
import io
import sys
import time
import urllib.request
import zipfile
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = ROOT / "data" / "raw"
ZIP_CACHE = RAW_DIR / "bts_zips"
DEFAULT_OUTPUT = RAW_DIR / "flights.csv"

URL_TEMPLATE = (
    "https://transtats.bts.gov/PREZIP/"
    "On_Time_Reporting_Carrier_On_Time_Performance_1987_present_{year}_{month}.zip"
)

KEEP_COLUMNS = [
    "Year", "Month", "DayofMonth", "DayOfWeek", "FlightDate", "Reporting_Airline",
    "Origin", "OriginCityName", "OriginState", "Dest", "DestCityName", "DestState",
    "CRSDepTime", "DepDelay", "ArrDelay", "Cancelled", "Diverted",
    "CarrierDelay", "WeatherDelay", "NASDelay", "SecurityDelay", "LateAircraftDelay",
]


def parse_month(text: str) -> date:
    try:
        year, month = text.split("-")
        return date(int(year), int(month), 1)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"Expected YYYY-MM, got {text!r}") from exc


def month_range(start: date, end: date):
    current = start
    while current <= end:
        yield current.year, current.month
        current = date(current.year + (current.month // 12), current.month % 12 + 1, 1)


def download(year: int, month: int, retries: int = 3) -> Path:
    ZIP_CACHE.mkdir(parents=True, exist_ok=True)
    target = ZIP_CACHE / f"bts_{year}_{month:02d}.zip"
    if target.exists() and zipfile.is_zipfile(target):
        print(f"  cached  {target.name}")
        return target

    url = URL_TEMPLATE.format(year=year, month=month)
    for attempt in range(1, retries + 1):
        try:
            print(f"  fetching {year}-{month:02d} (attempt {attempt}) ...", flush=True)
            request = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(request, timeout=300) as response:
                payload = response.read()
            if not payload.startswith(b"PK"):
                raise RuntimeError(
                    f"BTS did not return a ZIP for {year}-{month:02d}; "
                    "the month may not be published yet."
                )
            partial = target.with_suffix(".part")
            partial.write_bytes(payload)
            partial.replace(target)
            print(f"  saved   {target.name} ({len(payload) / 1e6:.1f} MB)")
            return target
        except RuntimeError:
            raise
        except Exception as exc:  # network hiccups: retry with backoff
            if attempt == retries:
                raise RuntimeError(f"Download failed for {url}: {exc}") from exc
            time.sleep(5 * attempt)
    raise AssertionError("unreachable")


def append_month(zip_path: Path, writer: csv.writer) -> int:
    with zipfile.ZipFile(zip_path) as archive:
        csv_name = next(n for n in archive.namelist() if n.lower().endswith(".csv"))
        with archive.open(csv_name) as raw:
            reader = csv.reader(io.TextIOWrapper(raw, encoding="utf-8", newline=""))
            header = next(reader)
            index = {name: i for i, name in enumerate(header)}
            missing = [c for c in KEEP_COLUMNS if c not in index]
            if missing:
                raise ValueError(f"{zip_path.name} is missing columns: {missing}")
            positions = [index[c] for c in KEEP_COLUMNS]
            rows = 0
            for row in reader:
                if row:
                    writer.writerow([row[p] for p in positions])
                    rows += 1
            return rows


def main() -> int:
    parser = argparse.ArgumentParser(description="Download BTS on-time performance data")
    parser.add_argument("--start", type=parse_month, required=True, help="First month, YYYY-MM")
    parser.add_argument("--end", type=parse_month, required=True, help="Last month, YYYY-MM")
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT), help="Output CSV path")
    parser.add_argument("--keep-zips", action="store_true", help="Keep downloaded ZIPs")
    args = parser.parse_args()

    if args.start > args.end:
        parser.error("--start must not be after --end")

    output = Path(args.output).resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    temp_output = output.with_suffix(".csv.part")

    months = list(month_range(args.start, args.end))
    print(f"Downloading {len(months)} month(s) of BTS data -> {output}")
    total = 0
    with temp_output.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(KEEP_COLUMNS)
        for year, month in months:
            zip_path = download(year, month)
            rows = append_month(zip_path, writer)
            total += rows
            print(f"  added   {rows:,} rows from {year}-{month:02d}")
            if not args.keep_zips:
                zip_path.unlink(missing_ok=True)
    temp_output.replace(output)

    note = RAW_DIR / "DATA_SOURCE.txt"
    note.write_text(
        "Source: U.S. Bureau of Transportation Statistics (TranStats), "
        "Airline On-Time Performance (Reporting Carrier On-Time Performance, 1987-present)\n"
        f"URL pattern: {URL_TEMPLATE}\n"
        f"Period: {args.start:%Y-%m} to {args.end:%Y-%m}\n"
        f"Downloaded: {date.today().isoformat()}\n"
        f"Rows: {total:,}\n"
        f"Columns kept: {', '.join(KEEP_COLUMNS)}\n",
        encoding="utf-8",
    )
    print(f"Done. {total:,} rows written to {output}")
    print(f"Source details recorded in {note}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
