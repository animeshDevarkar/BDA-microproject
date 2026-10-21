# One-time setup for Windows: Java check, virtual environment, packages, winutils.
# Run from the project folder:  powershell -ExecutionPolicy Bypass -File setup_windows.ps1
$ErrorActionPreference = "Stop"
Set-Location -LiteralPath $PSScriptRoot

Write-Host "== 1/4 Java ==" -ForegroundColor Cyan
$jdk = Get-ChildItem "C:\Program Files\Eclipse Adoptium", "C:\Program Files\Java", "C:\Program Files\Microsoft" -Directory -Filter "jdk-*" -ErrorAction SilentlyContinue | Select-Object -First 1
if (-not $env:JAVA_HOME -and -not $jdk) {
    Write-Host "No JDK found - installing Temurin JDK 17 with winget..."
    winget install --id EclipseAdoptium.Temurin.17.JDK -e --silent --accept-source-agreements --accept-package-agreements
} else {
    Write-Host "JDK found: $(if ($env:JAVA_HOME) { $env:JAVA_HOME } else { $jdk.FullName })"
}

Write-Host "== 2/4 Virtual environment ==" -ForegroundColor Cyan
if (-not (Test-Path ".venv\Scripts\python.exe")) { python -m venv .venv }
& .\.venv\Scripts\python.exe -m pip install --upgrade pip --quiet
& .\.venv\Scripts\python.exe -m pip install -r requirements.txt --quiet

Write-Host "== 3/4 Hadoop winutils (needed by Spark on Windows) ==" -ForegroundColor Cyan
New-Item -ItemType Directory -Force "hadoop\bin" | Out-Null
foreach ($f in "winutils.exe", "hadoop.dll") {
    if (-not (Test-Path "hadoop\bin\$f")) {
        Invoke-WebRequest "https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-3.3.6/bin/$f" -OutFile "hadoop\bin\$f"
    }
}

Write-Host "== 4/4 Verify ==" -ForegroundColor Cyan
& .\.venv\Scripts\python.exe -c "import sys; sys.path.insert(0, 'src'); import spark_env; spark_env.configure(); import pyspark, pandas, os; print('PySpark', pyspark.__version__, '| pandas', pandas.__version__, '| JAVA_HOME', os.environ['JAVA_HOME'])"
Write-Host "Setup complete. Next: .\run_all.ps1" -ForegroundColor Green
