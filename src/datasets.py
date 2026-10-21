"""The two data options of the project and where each one's files live.

- "usa"   -> International (USA): real U.S. BTS on-time records.
- "india" -> Domestic (India): simulated Indian domestic flights calibrated to DGCA OTP.

The U.S. paths are the project's original ones, so older commands keep working.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Dataset:
    key: str
    option: str        # the switch label: "International" / "Domestic"
    country: str       # "USA" / "India"
    region_name: str   # used in sentences: "U.S. average", "India average"
    source_name: str   # short source label for the UI
    simulated: bool


DATASETS = {
    "usa": Dataset("usa", "International", "USA", "U.S.", "U.S. BTS On-Time Performance", False),
    "india": Dataset("india", "Domestic", "India", "India", "Simulated, calibrated to DGCA on-time data", True),
}
DEFAULT = "usa"


def paths(key: str, root: Path) -> dict[str, Path]:
    """Input, Parquet, summary and metadata locations for one dataset under ``root``."""
    if key not in DATASETS:
        raise ValueError(f"Unknown dataset {key!r}; choose from {', '.join(DATASETS)}")
    if key == "usa":
        raw, processed, output = root / "data" / "raw", root / "data" / "processed" / "flights_clean", root / "output"
    else:
        raw, processed, output = (root / "data" / "raw" / key, root / "data" / "processed" / key / "flights_clean",
                                  root / "output" / key)
    return {
        "input": raw / "flights.csv",
        "source": raw / "DATA_SOURCE.txt",
        "processed": processed,
        "summaries": output / "powerbi",
        "run_summary": output / "run_summary.txt",
        "dashboard_data": root / "dashboard" / f"data-{key}.js",
        "website_data": root / "website" / "data" / f"site-data-{key}.js",
    }
