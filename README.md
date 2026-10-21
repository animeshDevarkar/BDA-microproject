# Flight Delay Analysis & Airport Network Bottlenecks

Big Data Analytics micro-project: a PySpark pipeline that cleans U.S. BTS on-time
performance records, finds delay patterns by month, hour, weekday, airline, airport,
route and cause, and presents them in an offline web dashboard and Power BI.

```
BTS ZIPs ──► download_bts_data.py ──► data/raw/flights.csv
                                          │
                     flight_delay_pipeline.py (PySpark, local mode)
                       read → normalise columns → cast → clean → features
                          │                                   │
     data/processed/flights_clean/ (Parquet,          output/powerbi/*.csv (9 summaries)
         partitioned by year_month)                           │
                                         ┌────────────────────┴───────────────┐
                               build_dashboard.py                      Power BI Desktop
                               dashboard/index.html                    (import the CSVs)
```

## Quick start (Windows)

```powershell
cd flight-delay-bda
powershell -ExecutionPolicy Bypass -File setup_windows.ps1   # once: JDK 17, venv, packages, winutils
.\run_all.ps1                                                # pipeline + tests + opens dashboard
```

`run_all.ps1` options:

| Command | What it does |
|---|---|
| `.\run_all.ps1` | Uses existing `data\raw\flights.csv` (downloads Jun–Aug 2026 if missing) |
| `.\run_all.ps1 -Start 2025-09 -End 2026-08` | Downloads those BTS months first, then runs |
| `.\run_all.ps1 -Sample` | 20,000-row **synthetic** sample, no internet needed |
| `.\run_all.ps1 -SkipTests` | Skips pytest |

### Step by step (what `run_all.ps1` does)

```powershell
.\.venv\Scripts\Activate.ps1
python src\download_bts_data.py --start 2025-09 --end 2026-08   # real data → data\raw\flights.csv
python src\flight_delay_pipeline.py                             # Spark pipeline + dashboard data
python -m pytest -q                                             # automated checks
start dashboard\index.html                                      # open the dashboard
python src\inspect_outputs.py                                   # print summaries with Pandas
```

Pipeline options: `--input`, `--processed`, `--output`, `--min-group-flights` (default 100),
`--min-route-flights` (default 50), `--driver-memory` (default 4g), `--skip-dashboard`.

## Project layout

```
flight-delay-bda/
├── data/raw/flights.csv            downloaded BTS data (+ DATA_SOURCE.txt: period, date, rows)
├── data/processed/flights_clean/   cleaned Parquet, partitioned by year_month
├── output/powerbi/                 9 summary CSVs for Power BI and the dashboard
├── output/run_summary.txt          run metadata
├── dashboard/index.html            offline dashboard (reads dashboard/data.js)
├── powerbi/flight_delay_theme.json Power BI colour theme matching the dashboard
├── src/
│   ├── flight_delay_pipeline.py    main PySpark pipeline
│   ├── spark_env.py                finds Java, sets HADOOP_HOME/short paths for Windows
│   ├── download_bts_data.py        fetches official BTS monthly files
│   ├── make_sample_data.py         synthetic sample for setup testing
│   ├── build_dashboard.py          bundles CSVs into dashboard/data.js
│   ├── inspect_outputs.py          Pandas inspection of summaries
│   └── plot_monthly.py             optional Matplotlib chart
├── tests/test_pipeline.py          pytest suite (self-contained, uses temp files)
├── hadoop/bin/                     winutils.exe + hadoop.dll (Spark on Windows)
├── logs/                           one log file per pipeline run
├── setup_windows.ps1, run_all.ps1, requirements.txt
```

## Outputs (`output/powerbi/`)

| File | Grain | Key columns |
|---|---|---|
| `kpi_summary.csv` | metric/value | records, delay rate, cancelled, diverted, period |
| `monthly_summary.csv` | year_month | delay rate, avg/median delay, cancellation rate |
| `airline_summary.csv` | airline (≥100 flights) | delay rate, avg delay, cancel rate, airline name |
| `origin_airport_summary.csv` | origin (≥100) | departure delay rate, priority score |
| `destination_airport_summary.csv` | destination (≥100) | arrival delay rate, priority score |
| `route_summary.csv` | route (≥50) | delay rate, avg delay, priority score |
| `hourly_summary.csv` | scheduled hour | delay rate, avg delay |
| `delay_cause_summary.csv` | cause | reported minutes, share % |
| `weekday_summary.csv` | day of week | delay rate (extra, beyond the 8 required files) |

## Definitions

- **Delayed arrival:** operated flight with `ArrDelay >= 15` minutes.
- **Arrival delay rate (%)** = delayed operated flights ÷ operated flights with a known
  arrival delay × 100. Cancelled and diverted flights are excluded from both and reported separately.
- **Departure delay rate (origin airports)** = non-cancelled flights with `DepDelay >= 15`
  ÷ non-cancelled flights with known departure delay.
- **Priority score** = delay rate × ln(1 + flights). Project-defined (guide §14 A), not an
  official metric; it balances rate with volume.
- **Cleaning:** drop rows with invalid date (e.g. Feb 30), missing airline/origin/destination,
  origin = destination, exact duplicates. Codes are trimmed and upper-cased.
  `CRSDepTime` outside 0000–2359 gives a null hour (2400 is treated as 00).

## Dashboard

`dashboard/index.html` opens directly from disk: no server, no internet.
Sections: Overview (KPIs + auto-generated answers to the 8 project questions), Time patterns,
Airlines, Airport hotspots, Route bottlenecks (volume vs rate scatter + searchable table),
Delay causes, Methodology. Every chart has a Table view and hover tooltips; light/dark mode
follows the system and can be toggled. The data is refreshed every time the pipeline runs.

## Customer website (OnTime Check)

`website/` is a public-facing site for travellers, built from the same Spark output:

- **Check a flight:** pick From/To (type a code or a city), plus an optional airline, departure hour and month.
  The site shows:
  - the chance of a 15+ minute late arrival, with a clear rating, the cancellation rate and the typical delay;
  - a step-by-step explanation of how the estimate was made;
  - airlines on the route ranked by reliability;
  - the best departure hours, a month-by-month line and the weekday pattern at the origin.
- **Airports:** search and sort 354 airports. Opening one shows its hour and month charts and its nonstop destinations.
- **Airlines:** compare carriers by late %, cancelled % or minutes late, and see each carrier's busiest routes.
- **My trips:** save trips, add notes and compare them. Trips are stored in the visitor's browser.
- Every result has a **Copy link** button: `?from=JFK&to=LAX&airline=DL&hour=8#check`.
- Light and dark mode, keyboard support, and works on phones.

How it is built: the pipeline (or `python src/build_website_data.py`, which reads the saved Parquet)
aggregates the data with Spark into `website/data/site-data.js`, about 0.9 MB. The aggregates are:
- route;
- route × airline;
- airport × hour, airport × month and airport × weekday.

The site is plain HTML/CSS/JS with no build step and no server code. To try it locally, double-click `website/index.html`.

### Make it live (pick one)

The site is static, so any static host can serve the `website/` folder for free.

| Host | Steps |
|---|---|
| **Netlify Drop** (fastest, no Git) | Sign in at https://app.netlify.com/drop and drag the `website` folder onto the page. You get a `https://<name>.netlify.app` URL; rename it under Site settings. |
| **GitHub Pages** | Push this project to a GitHub repo (`main` branch). Then Settings → Pages → Source: **GitHub Actions**. `.github/workflows/deploy-website.yml` publishes `website/` on every push, to `https://<user>.github.io/<repo>/`. |
| **Netlify / Vercel from Git** | Import the repo. `netlify.toml` / `vercel.json` already set the publish folder to `website` with no build command. |

To publish new data, re-run the pipeline, then upload or commit the new `website/data/site-data.js`.
Raw and Parquet data are git-ignored, so the repository stays small.

## Power BI

A `.pbix` file can only be authored in Power BI Desktop, so build it from the CSVs (guide §11):

1. Power BI Desktop → **Get data → Text/CSV** → load each file in `output/powerbi/`.
2. **View → Themes → Browse for themes** → `powerbi/flight_delay_theme.json`.
3. Build the pages from guide §11.2–11.6 (Overview cards filtered on `kpi_summary[metric]`,
   monthly line, hourly columns, airline bars, airport bars, route table with Top N, cause bars).
   `*_pct` columns are already 0–100: format them as decimals, not as Percentage.
4. Save as `output/Flight_Delay_Analysis.pbix`.

## Windows notes (handled automatically)

- Spark needs `winutils.exe`/`hadoop.dll` to write files; they live in `hadoop/bin` and
  `spark_env.py` points `HADOOP_HOME` at them.
- Paths with spaces (e.g. `BDA microproject`) break Spark's Windows scripts; `spark_env.py`
  uses 8.3 short paths for `HADOOP_HOME`, `SPARK_HOME` and the Python executable.
- PySpark **3.5.9** is pinned: 3.5.3 crashes its Python workers on Python 3.12/Windows.
- If `JAVA_HOME` is unset, `spark_env.py` finds a JDK under `C:\Program Files`.

## Troubleshooting

| Problem | Fix |
|---|---|
| `Java was not found` | `winget install EclipseAdoptium.Temurin.17.JDK`, open a new terminal |
| `winutils.exe is missing` | run `setup_windows.ps1` |
| `Input CSV not found` | run `download_bts_data.py` or `make_sample_data.py` |
| Empty airline/airport/route summaries | sample too small for the 100/50-flight minimums; use real data or lower `--min-*-flights` |
| Out of memory | download fewer months, or `--driver-memory 6g` |
| Dashboard says "No data yet" | run the pipeline; it writes `dashboard/data.js` |

## Limitations

U.S. reporting carriers only; historical and descriptive (no prediction). Delay-cause fields
are blank for on-time flights. High delay metrics flag hotspots for investigation; they do
not prove that an airport or airline caused the delays. The synthetic sample is for setup
testing only and must not be used as evidence.
