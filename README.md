# Flight Delay Analysis & Airport Network Bottlenecks

Big Data Analytics micro-project: a PySpark pipeline that cleans U.S. BTS on-time
performance records, finds delay patterns by month, hour, weekday, airline, airport,
route and cause, presents them in an offline web dashboard and Power BI, predicts journey
fares with a Spark MLlib model trained on airfares plus features from the flight data, and
works out the cost of a whole trip from millions of (simulated) ticket bookings.

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

## Data options: International and Domestic

The project runs the same pipeline on two datasets. The dashboard and the website have a switch between them.

| Option | Data | Files |
|---|---|---|
| **International · USA** | Real U.S. BTS on-time records, Sep 2025 – Aug 2026 (7.05M flights) | `data/raw/flights.csv` → `output/powerbi/` |
| **Domestic · India** | **Simulated** Indian domestic flights, Sep 2025 – Aug 2026 (1.15M flights) | `data/raw/india/flights.csv` → `output/india/powerbi/` |

**Why the India data is simulated:** India's regulator DGCA publishes only monthly on-time-performance (OTP)
percentages per airline at a few metro airports. There is no public flight-by-flight file like the BTS one.
`src/make_india_data.py` generates realistic records instead, reproducible from a seed:
- **Network:** 43 real airports, a gravity-model route network (684 routes) and 8 real airlines at their market shares (IndiGo 64%, Air India group, Akasa, SpiceJet, Alliance Air, Star Air, Fly91).
- **Delay rates:** each airline's late rate is calibrated to DGCA OTP levels: IndiGo about 14% late, Akasa 15%, Air India 27%, SpiceJet 49%.
- **Seasons and congestion:**
  - winter fog in north India (Dec–Feb);
  - south-west monsoon on the west coast (Jun–Sep);
  - north-east monsoon in Tamil Nadu (Oct–Dec);
  - evening congestion and busy hubs (DEL/BOM/BLR).
- **Causes:** reactionary (late-aircraft) delay is the largest cause.

It uses the BTS column layout, so the PySpark pipeline runs on it unchanged. The site and the dashboard label it
**Simulated** everywhere. Use it to demonstrate the pipeline, not as real-world evidence.

```powershell
python src\make_india_data.py                          # generate data\raw\india\flights.csv (~22 s)
python src\flight_delay_pipeline.py --dataset india    # Spark pipeline for India
python src\flight_delay_pipeline.py --dataset usa      # (default) Spark pipeline for the USA
python src\inspect_outputs.py india                    # print the India summaries
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
| `.\run_all.ps1` | Runs both datasets. Uses existing data; downloads Jun–Aug 2026 BTS data and generates the India data if missing |
| `.\run_all.ps1 -Start 2025-09 -End 2026-08` | Downloads those BTS months first, then runs |
| `.\run_all.ps1 -Sample` | 20,000-row **synthetic** sample, no internet needed |
| `.\run_all.ps1 -SkipTests` | Skips pytest |
| `.\run_all.ps1 -SkipFares` | Skips the fare prediction model and the trip cost model (they download DOT fares and generate India fares and bookings if missing) |

### Step by step (what `run_all.ps1` does)

```powershell
.\.venv\Scripts\Activate.ps1
python src\download_bts_data.py --start 2025-09 --end 2026-08   # real data → data\raw\flights.csv
python src\flight_delay_pipeline.py                             # Spark pipeline (USA) + dashboard/website data
python src\make_india_data.py                                   # simulated India data (once)
python src\flight_delay_pipeline.py --dataset india             # Spark pipeline (India)
python src\download_fare_data.py                                # real U.S. airfares → data\raw\fares.csv
python src\make_india_fares.py                                  # simulated India fares (once)
python src\fare_price_model.py --dataset usa                    # fare model (Spark MLlib), USA
python src\fare_price_model.py --dataset india                  # fare model, India
python src\make_booking_data.py                                 # simulated bookings, USA (once)
python src\make_booking_data.py --dataset india                 # simulated bookings, India (once)
python src\trip_cost_model.py                                   # trip cost factors (Spark), USA
python src\trip_cost_model.py --dataset india                   # trip cost factors, India
python -m pytest -q                                             # automated checks
start dashboard\index.html                                      # open the dashboard
python src\inspect_outputs.py                                   # print summaries with Pandas
```

Pipeline options: `--dataset usa|india`, `--input`, `--processed`, `--output`, `--min-group-flights` (default 100),
`--min-route-flights` (default 50), `--driver-memory` (default 4g), `--skip-dashboard`, `--skip-website`.

## Project layout

```
flight-delay-bda/
├── data/raw/flights.csv            downloaded BTS data (+ DATA_SOURCE.txt: period, date, rows)
├── data/raw/india/flights.csv      simulated Indian domestic flights (+ DATA_SOURCE.txt)
├── data/raw/fares.csv              U.S. DOT airport-pair fares (+ FARES_SOURCE.txt); india/fares.csv simulated
├── data/raw/bookings.csv           simulated ticket bookings (+ BOOKINGS_SOURCE.txt); india/bookings.csv too
├── data/processed/flights_clean/   cleaned Parquet, partitioned by year_month (india/ for India)
├── data/processed/bookings_clean/  cleaned bookings, Parquet by travel month (india/ for India)
├── output/powerbi/                 9 summary CSVs for Power BI and the dashboard (USA)
├── output/india/powerbi/           the same 9 CSVs for India
├── output/run_summary.txt          run metadata
├── output/models/fare_model/       saved Spark MLlib fare model (india/models/ for India)
├── dashboard/index.html            offline dashboard (reads data-usa.js / data-india.js)
├── website/                        OnTime Check customer site (data/site-data-*.js, price-data-*.js, cost-data-*.js)
├── powerbi/flight_delay_theme.json Power BI colour theme matching the dashboard
├── src/
│   ├── flight_delay_pipeline.py    main PySpark pipeline
│   ├── datasets.py                 the two data options and their file locations
│   ├── make_india_data.py          simulated Indian domestic flights (DGCA-calibrated)
│   ├── build_website_data.py       Spark aggregates for the customer website
│   ├── fare_price_model.py         journey price prediction: Spark MLlib training, evaluation, forecast
│   ├── download_fare_data.py       fetches U.S. DOT Consumer Airfare Report (Table 1a)
│   ├── make_india_fares.py         simulated Indian airport-pair fares (same layout)
│   ├── make_booking_data.py        simulated ticket bookings, millions of rows (both datasets)
│   ├── trip_cost_model.py          trip cost: Spark cleaning, fare index, fees, route cost + delay
│   ├── spark_env.py                finds Java, sets HADOOP_HOME/short paths for Windows
│   ├── download_bts_data.py        fetches official BTS monthly files
│   ├── make_sample_data.py         synthetic sample for setup testing
│   ├── build_dashboard.py          bundles CSVs into dashboard/data-<dataset>.js
│   ├── inspect_outputs.py          Pandas inspection of summaries
│   └── plot_monthly.py             optional Matplotlib chart
├── tests/test_pipeline.py          pytest suite (self-contained, uses temp files)
├── hadoop/bin/                     winutils.exe + hadoop.dll (Spark on Windows)
├── logs/                           one log file per pipeline run
├── setup_windows.ps1, run_all.ps1, requirements.txt
```

## Outputs (`output/powerbi/`, and `output/india/powerbi/` for India)

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
| `fare_*.csv` | model / pair × airline × quarter | fare model metrics, feature importance, forecast, trend (see Journey price prediction) |
| `trip_cost_*.csv` | cabin × booking date / airline / route / month | fare index, fees, cost per traveller, delay cost (see Trip cost) |

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
  - the best departure hours, a month-by-month line and the weekday pattern at the origin;
  - an AI **predicted fare** with a typical range, by quarter and by airline (see Journey price prediction);
  - a **trip cost calculator**: total for all travellers with a breakdown and the best time to book (see Trip cost).
- **Airports:** search and sort 354 airports. Opening one shows its hour and month charts and its nonstop destinations.
- **Airlines:** compare carriers by late %, cancelled % or minutes late, and see each carrier's busiest routes.
- **My trips:** save trips, add notes and compare them. Trips are stored in the visitor's browser.
- Every result has a **Copy link** button: `?from=JFK&to=LAX&airline=DL&hour=8#check`.
- Light and dark mode, keyboard support, and works on phones.

How it is built: the pipeline (or `python src/build_website_data.py`, which reads the saved Parquet)
aggregates the data with Spark into `website/data/site-data-usa.js` (about 0.9 MB) and `site-data-india.js` (about 0.2 MB).
A switch in the header changes between **International · USA** and **Domestic · India**. The aggregates are:
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

To publish new data, re-run the pipeline, then upload or commit the new `website/data/site-data-*.js` files.
Raw and Parquet data are git-ignored, so the repository stays small.

## Journey price prediction (AI)

`src/fare_price_model.py` trains a **Spark MLlib** model that predicts the average one-way fare of a journey
(airport pair × airline × quarter) and forecasts the next four quarters for every route on the website.

```
DOT Table 1a fares ──► download_fare_data.py ──► data/raw/fares.csv ─┐
(India: make_india_fares.py, simulated, same columns)                │
                                                                     ▼
data/processed/flights_clean/ (Parquet, millions of flights) ──► fare_price_model.py (PySpark + MLlib)
   network features: nonstop flights, competing airlines,            label: log(fare), 1 row per pair × quarter × airline
   late / cancellation rates, airline & airport size                 models: linear regression, random forest, GBT
                                                                     time-based hold-out = latest 4 quarters
                                                                     ▼
              output/models/fare_model/ · output/powerbi/fare_*.csv · website/data/price-data-<dataset>.js
```

**Training data.**
- **USA (real fares).** The U.S. DOT *Consumer Airfare Report, Table 1a*, 2015Q1 to the latest published quarter
  (about 90,000 airport-pair quarters). Each row gives:
  - the average fare;
  - the largest airline's fare and the lowest-fare airline's fare;
  - the distance and the passengers per day.

  Each row becomes up to three labelled examples: the market average (airline `ALL`), the largest airline and the low-fare airline.
- **India (simulated fares).** India publishes no airport-pair fare file. `make_india_fares.py` generates one in the
  same layout on the simulated flight network. It uses a distance fare curve, airline price levels, competition,
  seasons (summer, festive, monsoon, Goa and hill peaks) and history (COVID, Go First). Labelled **Simulated** everywhere.

**Big-data features.** Spark aggregates the cleaned flight records (7M U.S. / 1.15M India) per airport pair, pair × airline,
airport and airline. They give the model:
- the nonstop frequency;
- the number of competing airlines;
- the late-arrival and cancellation rates;
- the airline's own flights and punctuality on the pair;
- the size of both airports and of the airline's network.

These are joined to the fare rows, together with distance, passengers, year, quarter, airline and both airports.

**Models and evaluation.** Three regressors are trained on log(fare):
- linear regression (baseline);
- random forest;
- gradient-boosted trees.

They are compared on the **latest 4 quarters, held out by time**, so the test is a real forecast of unseen quarters.
The metrics are RMSE, MAE, MAPE and R². The best model is refitted on all quarters and saved as a Spark `PipelineModel`.
The "typical range" shown to travellers is the 10th–90th percentile of actual ÷ predicted fares in the hold-out.

**Results of the last run** (hold-out = the latest 4 quarters; figures from `fare_model_metrics.csv`):

| Dataset | Rows (train / test) | Test quarters | Model | RMSE | MAE | MAPE | R² |
|---|---|---|---|---|---|---|---|
| USA (real fares) | 200,484 / 20,182 | 2025Q2–2026Q1 | Linear regression | $60.60 | $39.41 | 15.2% | 0.49 |
| | | | **Random forest (selected)** | $48.51 | $30.47 | 11.8% | 0.68 |
| | | | Gradient-boosted trees | $100.82 | $31.51 | 12.3% | −0.40 |
| India (simulated fares) | 33,809 / 3,276 | 2025Q3–2026Q2 | Linear regression | ₹672 | ₹509 | 10.3% | 0.86 |
| | | | Random forest | ₹462 | ₹348 | 6.5% | 0.93 |
| | | | **Gradient-boosted trees (selected)** | ₹387 | ₹293 | 5.7% | 0.95 |

- USA: 8 in 10 hold-out fares fall within ×0.87 to ×1.25 of the prediction. The airline (32%), the distance (19%)
  and the two airports (12% each) carry most of the importance.
- USA gradient-boosted trees have a similar typical error to the forest but a few very large misses, which is why
  their RMSE is high and R² negative. The model is chosen on RMSE, so the forest wins.
- India scores higher because its fares are generated from a formula; that is a property of the simulation, not
  evidence that Indian fares are easier to predict.

```powershell
python src\download_fare_data.py                  # real U.S. fares → data\raw\fares.csv (~1 min)
python src\make_india_fares.py                    # simulated India fares → data\raw\india\fares.csv (~15 s)
python src\fare_price_model.py                    # USA: train, evaluate, forecast (~10 min)
python src\fare_price_model.py --dataset india    # India (~3 min); --fast for smaller forests
```

| Output | Contents |
|---|---|
| `fare_model_metrics.csv` | RMSE, MAE, MAPE, R² per model on the hold-out; `selected` marks the winner |
| `fare_feature_importance.csv` | importance (%) of each input in the selected tree model |
| `fare_forecast.csv` | predicted fare + typical range per pair × airline × forecast quarter, with last year's actual |
| `fare_trend.csv` | passenger-weighted and plain average fare per quarter (history) |
| `output/models/fare_model/` | the fitted Spark pipeline; reload with `PipelineModel.load(path)` |

On the website, **Check a flight** shows a **Predicted fare** panel:
- the forecast for the chosen airline and travel month, or the next quarter when no month is picked;
- the typical range, and last year's reported fare;
- a quarter-by-quarter chart and the airlines on the route ranked by predicted fare;
- an explanation of how the prediction was made.

Saved trips show the fare too.

## Trip cost (big data)

The fare forecast is an average one-way fare. The **trip cost calculator** turns it into the cost of a whole trip:
tickets for every traveller and direction, taxes, baggage and seat fees, plus the value of the expected delay.
The adjustments are learned with Spark from millions of ticket bookings.

```
flights_clean/ (Parquet) + fares.csv ──► make_booking_data.py ──► data/raw/bookings.csv
   routes and airlines by flight count,         (SIMULATED: 3,002,250 rows USA, 306 MB;
   fares anchored on the pair's fare             1,501,125 rows India, 156 MB)
                                                        │
                                          trip_cost_model.py (PySpark)
                                   clean → Parquet → window fare index → aggregates
                                   → least-squares tax line → join with route delays
                                                        ▼
        data/processed/bookings_clean/ · output/powerbi/trip_cost_*.csv · website/data/cost-data-<dataset>.js
```

**Why the bookings are simulated.** No public file lists single bookings with the cabin, the booking date and the
fees paid, for either country. `src/make_booking_data.py` generates one row per booking, reproducible from a seed:
- **Routes and airlines** are sampled in proportion to the flights in the cleaned flight records.
- **Ticket fare** starts from the pair's average fare for the travel quarter in `fares.csv` (real DOT fares for the
  USA), then moves with the cabin, the days between booking and departure, the weekday and random variation.
- **Taxes** follow the U.S. domestic ticket taxes (7.5% excise, segment fee, security fee, facility charge), or
  India's GST plus airport fees, at approximate published levels.
- **Extras** are checked-bag fees (USA) or extra baggage beyond the free 15 kg (India), and paid seat selection,
  at approximate published levels per airline.
- **Broken rows.** About 0.3% of the rows are duplicates, have a missing airport, a negative fare or a travel date
  before the booking date, so the cleaning step has real work to do.

**What Spark computes** (`src/trip_cost_model.py`):
1. **Cleaning.** Each rule's removed rows are counted, and the clean bookings are stored as Parquet by travel month.
2. **Fare index.** A window divides each fare by the average of its airport pair and airline. The index is then
   averaged by cabin × days booked ahead, and by weekday.
3. **Taxes.** A least-squares line per cabin (`taxes = fixed + rate × fare`) from covariance aggregates.
4. **Extras.** Baggage and seat fees per airline (the seat fee as a line on distance), and the share who pay them.
5. **Routes.** Cost per traveller and direction, joined with the late rate and typical delay of the same route in
   the flight records. Expected delay minutes × a value of time gives the delay cost.

**The calculation** (`quote()` in Python, `tripCost()` in `website/app.js`):

```
ticket = predicted fare × index(cabin, days booked ahead) × index(weekday)     per traveller, per direction
total  = (ticket + baggage fee + seat fee) × travellers × directions           taxes are a part of the ticket
delay  = chance of a late arrival × typical delay × value of an hour           time, not cash: shown apart
```

**Results of the last run** (from `trip_cost_summary.csv` and `trip_cost_fare_factors.csv`):

| | USA | India |
|---|---|---|
| Rows read / clean bookings | 3,002,250 / 2,993,250 | 1,501,125 / 1,496,625 |
| Removed in cleaning | 9,000 (2,250 per rule) | 4,500 (1,125 per rule) |
| Routes | 3,132 | 684 |
| Average cost per traveller and direction | $285.32 | ₹5,538 |
| Taxes as a share of the ticket | 12.4% | 18.6% |
| Extras as a share of the total | 8.5% | 8.0% |
| Economy index: booked 0–2 days ahead | × 1.34 | × 1.44 |
| Economy index: booked 3–8 weeks ahead | × 0.83 | × 0.90 |
| Business index: booked 3–8 weeks ahead | × 2.12 | × 2.55 |
| Weekday index: Tuesday / Sunday | × 0.94 / × 1.08 | × 0.94 / × 1.08 |

These factors recover the rules of the generator, so they show that the pipeline works, not how real travellers pay.

```powershell
python src\make_booking_data.py                    # USA bookings → data\raw\bookings.csv (~30 s)
python src\make_booking_data.py --dataset india    # India bookings → data\raw\india\bookings.csv (~30 s)
python src\trip_cost_model.py                      # USA: clean, aggregate, export (~80 s)
python src\trip_cost_model.py --dataset india      # India (~60 s)
```

| Output | Contents |
|---|---|
| `trip_cost_summary.csv` | rows read, rows removed per cleaning rule, travellers, average cost, tax and extras shares |
| `trip_cost_fare_factors.csv` | fare index and average fare per cabin × days booked ahead |
| `trip_cost_weekday.csv` | fare index per day of travel |
| `trip_cost_extras.csv` | per airline: share with bags, fee for one / two bags, share paying for seats, seat fee |
| `trip_cost_routes.csv` | per route: bookings, cost per traveller and direction, tax share, late rate, delay cost |
| `trip_cost_monthly.csv` | bookings and average cost per travel month |
| `data/processed/bookings_clean/` | the cleaned bookings as Parquet, partitioned by travel month |

On the website, **Check a flight** shows a **Trip cost calculator** under the predicted fare. The visitor picks the
travellers, one way or round trip, the cabin, how far ahead they book, the day of travel, bags and seat selection.
The page shows the total with a likely range, the breakdown, a **When to book** chart, the delay risk in minutes and
money, and an explanation of each step. The choices are remembered in the browser.

The offline dashboard (`dashboard/index.html`) does not show the trip cost yet; load the `trip_cost_*.csv` files in
Power BI for charts.

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
| `No module named 'distutils'` | `pip install setuptools` (pyspark.ml needs it on Python 3.12) |
| Website has no Predicted fare panel | run `python src/fare_price_model.py` (and `--dataset india`) |
| Website says "Trip cost factors are missing" | run `python src/make_booking_data.py` then `python src/trip_cost_model.py` (and `--dataset india`) |
| Dashboard says "No data yet" | run the pipeline; it writes `dashboard/data-usa.js` (`data-india.js` with `--dataset india`) |

## Limitations

USA: U.S. reporting carriers only. India: simulated records calibrated to DGCA levels, not real flights. Delay figures are historical and descriptive. Delay-cause fields
are blank for on-time flights. High delay metrics flag hotspots for investigation; they do
not prove that an airport or airline caused the delays. The synthetic sample is for setup
testing only and must not be used as evidence.

Fare prediction:
- **What it predicts.** Average fares by airport pair, airline and quarter, not the price of a ticket on a given day.
  The DOT data has no booking date, cabin or departure time, so the model cannot use them.
- **Direction.** Fares are not split by direction (A→B and B→A share a price).
- **Airlines.** Only airlines that appear as the largest or lowest-fare carrier in the DOT data can be priced
  individually. Regional carriers such as SkyWest sell under partner brands and get the all-airline forecast.
- **Inflation.** Forecasts beyond the training period assume the latest year's level (trees do not extrapolate trends).
- **India.** Fares are simulated, so the India model shows the method, not real prices.

Trip cost:
- **Simulated bookings.** The bookings are generated for both countries. The factors Spark finds are the ones the
  generator put in, so the calculator gives a reasoned estimate, not a market price.
- **Fees.** Baggage, seat and tax levels are approximations of published levels and change often. Check the airline.
- **Not covered.** Connections, carry-on fees, change fees, discounts, and travel to and from the airport.
- **Delay cost.** The value of an hour (default $30 / ₹400) is an assumption the visitor can change. It prices
  waiting time only, not missed connections or hotel nights.
