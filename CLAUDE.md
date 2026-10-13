# CLAUDE.md

Learning-in-public capstone: a river-navigation lakehouse (Mississippi/Illinois Rivers,
St. Louis area) on Azure Databricks → Microsoft Fabric → Microsoft Foundry, with an
ASP.NET Core Razor front end. The owner is a senior .NET/SQL Server architect learning
PySpark and the lakehouse stack — explain Spark/Delta choices by contrast with T-SQL
when it helps, and prefer idiomatic Python over C#-shaped Python.

## Rules
- Public data only (USACE, USGS, NOAA). Never add client or proprietary data.
- Bronze keeps payloads as received; typing and cleaning happen in silver.
- Business rules live first as pure functions in `src/river_nav/transforms.py` with
  pytest coverage; pipeline code mirrors them in Spark.
- Use current Databricks names: Lakeflow Jobs, Lakeflow Spark Declarative Pipelines
  (`from pyspark import pipelines as dp`), Automation Bundles, Git folders.
- USGS: new `api.waterdata.usgs.gov` API only.
- Anything not yet confirmed against a live source is marked `VERIFY`.

## Commands
- `pytest` — unit tests (no Spark required)
- `ruff check src tests`
- `databricks bundle validate -t dev` / `deploy -t dev` / `run <resource> -t dev`
