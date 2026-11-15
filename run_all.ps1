# Run the whole project for both data options - International (USA, BTS) and Domestic (India, simulated):
# (optional) download data, PySpark pipeline per dataset, fare prediction model, tests, open dashboard + website.
#   .\run_all.ps1                          # use existing data (downloads/generates what is missing)
#   .\run_all.ps1 -Start 2026-06 -End 2026-08   # download those BTS months first
#   .\run_all.ps1 -Sample                  # synthetic 20k-row U.S. sample (no internet)
#   .\run_all.ps1 -SkipFares               # skip the fare (journey price) model and the trip cost model
param([string]$Start, [string]$End, [switch]$Sample, [switch]$SkipTests, [switch]$SkipFares)
$ErrorActionPreference = "Stop"
Set-Location -LiteralPath $PSScriptRoot
$py = ".\.venv\Scripts\python.exe"
if (-not (Test-Path $py)) { throw "Virtual environment missing. Run setup_windows.ps1 first." }

# International (USA)
if ($Sample) {
    & $py src\make_sample_data.py --rows 20000
} elseif ($Start -and $End) {
    & $py src\download_bts_data.py --start $Start --end $End
} elseif (-not (Test-Path "data\raw\flights.csv")) {
    Write-Host "No data\raw\flights.csv - downloading June-August 2026 from BTS..."
    & $py src\download_bts_data.py --start 2026-06 --end 2026-08
}
if ($LASTEXITCODE) { throw "Data step failed" }

& $py src\flight_delay_pipeline.py --dataset usa
if ($LASTEXITCODE) { throw "Pipeline failed (USA)" }

# Domestic (India)
if (-not (Test-Path "data\raw\india\flights.csv")) {
    Write-Host "Generating simulated Indian domestic flights (calibrated to DGCA on-time data)..."
    & $py src\make_india_data.py
    if ($LASTEXITCODE) { throw "India data generation failed" }
}
& $py src\flight_delay_pipeline.py --dataset india
if ($LASTEXITCODE) { throw "Pipeline failed (India)" }

# Journey price prediction: Spark MLlib fare model per dataset (reads the Parquet written above)
if (-not $SkipFares) {
    if (-not (Test-Path "data\raw\fares.csv")) {
        Write-Host "No data\raw\fares.csv - downloading U.S. DOT airport-pair fares..."
        & $py src\download_fare_data.py
        if ($LASTEXITCODE) { throw "Fare download failed" }
    }
    if (-not (Test-Path "data\raw\india\fares.csv")) {
        & $py src\make_india_fares.py
        if ($LASTEXITCODE) { throw "India fare generation failed" }
    }
    foreach ($ds in "usa", "india") {
        & $py src\fare_price_model.py --dataset $ds
        if ($LASTEXITCODE) { throw "Fare model failed ($ds)" }
    }

    # Trip cost: simulated ticket bookings (millions of rows) -> Spark cost factors for the website calculator
    foreach ($ds in "usa", "india") {
        $bookings = if ($ds -eq "usa") { "data\raw\bookings.csv" } else { "data\raw\$ds\bookings.csv" }
        if (-not (Test-Path $bookings)) {
            & $py src\make_booking_data.py --dataset $ds
            if ($LASTEXITCODE) { throw "Booking generation failed ($ds)" }
        }
        & $py src\trip_cost_model.py --dataset $ds
        if ($LASTEXITCODE) { throw "Trip cost model failed ($ds)" }
    }
}

if (-not $SkipTests) {
    & $py -m pytest -q
    if ($LASTEXITCODE) { throw "Tests failed" }
}
Start-Process (Resolve-Path "dashboard\index.html")
Start-Process (Resolve-Path "website\index.html")
