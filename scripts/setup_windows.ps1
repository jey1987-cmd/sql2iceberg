# Windows setup: run from the repo folder in PowerShell. Needs Java 17 (winget install EclipseAdoptium.Temurin.17.JDK).
$ErrorActionPreference = "Stop"
Remove-Item Env:SPARK_HOME -ErrorAction SilentlyContinue
Remove-Item Env:PYTHONPATH -ErrorAction SilentlyContinue
java -version
py -3 -m venv .venv
. .\.venv\Scripts\Activate.ps1
pip install "pyspark==4.0.1"
pip install -e ".[pytest,dev]"
python -c "from pyspark.sql import SparkSession; s=SparkSession.builder.master('local[2]').getOrCreate(); print('Spark', s.version); s.sql('select 1 as x').show()"
