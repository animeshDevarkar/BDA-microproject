# Run the whole project: (optional) download data, PySpark pipeline, tests, open dashboard + website.
#   .\run_all.ps1                          # use existing data\raw\flights.csv
#   .\run_all.ps1 -Start 2026-06 -End 2026-08   # download those BTS months first
#   .\run_all.ps1 -Sample                  # synthetic 20k-row sample (no internet)
param([string]$Start, [string]$End, [switch]$Sample, [switch]$SkipTests)
$ErrorActionPreference = "Stop"
Set-Location -LiteralPath $PSScriptRoot
$py = ".\.venv\Scripts\python.exe"
if (-not (Test-Path $py)) { throw "Virtual environment missing. Run setup_windows.ps1 first." }

if ($Sample) {
    & $py src\make_sample_data.py --rows 20000
} elseif ($Start -and $End) {
    & $py src\download_bts_data.py --start $Start --end $End
} elseif (-not (Test-Path "data\raw\flights.csv")) {
    Write-Host "No data\raw\flights.csv - downloading June-August 2026 from BTS..."
    & $py src\download_bts_data.py --start 2026-06 --end 2026-08
}
if ($LASTEXITCODE) { throw "Data step failed" }

& $py src\flight_delay_pipeline.py
if ($LASTEXITCODE) { throw "Pipeline failed" }

if (-not $SkipTests) {
    & $py -m pytest -q
    if ($LASTEXITCODE) { throw "Tests failed" }
}
Start-Process (Resolve-Path "dashboard\index.html")
Start-Process (Resolve-Path "website\index.html")
