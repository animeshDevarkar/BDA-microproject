# Run the whole project for both data options - International (USA, BTS) and Domestic (India, simulated):
# (optional) download data, PySpark pipeline per dataset, tests, open dashboard + website.
#   .\run_all.ps1                          # use existing data (downloads/generates what is missing)
#   .\run_all.ps1 -Start 2026-06 -End 2026-08   # download those BTS months first
#   .\run_all.ps1 -Sample                  # synthetic 20k-row U.S. sample (no internet)
param([string]$Start, [string]$End, [switch]$Sample, [switch]$SkipTests)
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

if (-not $SkipTests) {
    & $py -m pytest -q
    if ($LASTEXITCODE) { throw "Tests failed" }
}
Start-Process (Resolve-Path "dashboard\index.html")
Start-Process (Resolve-Path "website\index.html")
