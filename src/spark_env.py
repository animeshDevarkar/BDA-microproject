"""Prepare environment variables so PySpark runs reliably on this machine.

Import this module and call ``configure()`` *before* creating a SparkSession.
It is safe to call more than once and does nothing that is already set up.

What it does:
- Uses the current Python interpreter for Spark workers (avoids "python not found").
- Uses Windows short paths for Python/SPARK_HOME (Spark's .cmd scripts break on spaces).
- Finds a JDK if JAVA_HOME is not set (common right after installing Java).
- On Windows, points HADOOP_HOME at the project's ``hadoop`` folder, which holds
  ``winutils.exe`` / ``hadoop.dll`` needed for Spark to write Parquet/CSV files.
  Hadoop cannot handle spaces in paths, so the Windows 8.3 short path is used.
"""
from __future__ import annotations

import ctypes
import glob
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PROJECT_HADOOP = ROOT / "hadoop"

_JDK_SEARCH_PATTERNS = [
    r"C:\Program Files\Eclipse Adoptium\jdk-*",
    r"C:\Program Files\Java\jdk-*",
    r"C:\Program Files\Microsoft\jdk-*",
    r"C:\Program Files\Zulu\zulu-*",
    r"C:\Program Files\Amazon Corretto\jdk*",
]


def _short_path(path: Path) -> str:
    """Return the Windows 8.3 short form of ``path`` (no spaces), if available."""
    if os.name != "nt":
        return str(path)
    buffer = ctypes.create_unicode_buffer(32768)
    length = ctypes.windll.kernel32.GetShortPathNameW(str(path), buffer, len(buffer))
    return buffer.value if 0 < length < len(buffer) else str(path)


def _java_ok(java_home: str | None) -> bool:
    if not java_home:
        return False
    exe = "java.exe" if os.name == "nt" else "java"
    return (Path(java_home) / "bin" / exe).exists()


def _find_java_home() -> str | None:
    candidates: list[str] = []
    for pattern in _JDK_SEARCH_PATTERNS:
        candidates.extend(sorted(glob.glob(pattern), reverse=True))
    # Prefer JDK 17, then 21, then 11 (versions supported by Spark 3.5).
    for preferred in ("17", "21", "11"):
        for candidate in candidates:
            name = Path(candidate).name
            if f"-{preferred}" in name or name.startswith(f"jdk{preferred}") or f"zulu{preferred}" in name:
                if _java_ok(candidate):
                    return candidate
    return next((c for c in candidates if _java_ok(c)), None)


def _prepend_path(directory: str) -> None:
    current = os.environ.get("PATH", "")
    if directory.lower() not in current.lower():
        os.environ["PATH"] = directory + os.pathsep + current


def configure() -> None:
    # Spark's Windows launch scripts break on spaces in paths, so use short paths.
    python = _short_path(Path(sys.executable))
    os.environ.setdefault("PYSPARK_PYTHON", python)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", python)
    if not os.environ.get("SPARK_HOME"):
        import pyspark
        os.environ["SPARK_HOME"] = _short_path(Path(pyspark.__file__).resolve().parent)

    if not _java_ok(os.environ.get("JAVA_HOME")):
        java_home = _find_java_home()
        if java_home is None:
            raise RuntimeError(
                "Java was not found. Install a JDK 17 (e.g. `winget install "
                "EclipseAdoptium.Temurin.17.JDK`) or set JAVA_HOME to your JDK folder."
            )
        os.environ["JAVA_HOME"] = java_home
    _prepend_path(str(Path(os.environ["JAVA_HOME"]) / "bin"))

    if os.name == "nt":
        hadoop_home = os.environ.get("HADOOP_HOME")
        if not hadoop_home or not (Path(hadoop_home) / "bin" / "winutils.exe").exists():
            if not (PROJECT_HADOOP / "bin" / "winutils.exe").exists():
                raise RuntimeError(
                    "winutils.exe is missing. Run `powershell -ExecutionPolicy Bypass "
                    "-File setup_windows.ps1` from the project folder."
                )
            hadoop_home = str(PROJECT_HADOOP)
        hadoop_home = _short_path(Path(hadoop_home))
        os.environ["HADOOP_HOME"] = hadoop_home
        os.environ["hadoop.home.dir"] = hadoop_home
        _prepend_path(str(Path(hadoop_home) / "bin"))
