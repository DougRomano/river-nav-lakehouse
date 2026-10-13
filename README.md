# River Navigation Lakehouse

A learning-in-public capstone: an end-to-end data and AI platform for the
Mississippi and Illinois Rivers around St. Louis, built on **Azure Databricks**,
**Microsoft Fabric** and **Microsoft Foundry**, with an **ASP.NET Core** front end.

It answers questions like:

- Which locks are backing up right now, and how long are tows waiting?
- How does river stage at St. Louis relate to delays at Mel Price or Chain of Rocks?
- Can we predict tomorrow's queue delay from river conditions and weather?

> Public data only. No client or proprietary data is used anywhere in this repo.

## Architecture

```
 Sources                  Databricks (Unity Catalog)                 Fabric            Apps
 ───────                  ──────────────────────────                 ──────            ────
 USACE Corps Locks ─┐
 USGS Water Data  ──┼─► collector job ─► Volume (raw files)
 NOAA weather     ──┘                        │
 USACE lock chars ───────────────────────────┤
                                             ▼
                                   bronze (Auto Loader, as-received)
                                             ▼
                                   silver (typed, deduped, expectations)
                                             ▼
                                   gold (star schema) ──► OneLake shortcut ─► Direct Lake report
                                             │
                                             ├──► Foundry agent (SQL tool + RAG) ─► ASP.NET Razor UI
                                             └──► MLflow lock-delay model
```

## Repo layout

| Path | What it is |
| --- | --- |
| `databricks.yml` | Automation Bundle root (dev / test / prod targets) |
| `resources/` | Bundle resources: the declarative pipeline and Lakeflow Jobs |
| `src/river_nav/` | Pure-Python library: config, API clients, transforms (unit tested) |
| `src/notebooks/` | Collector notebook that lands raw API responses in a Volume |
| `src/pipelines/` | Lakeflow Spark Declarative Pipeline: bronze, silver, gold |
| `sql/` | Gold star-schema reference DDL and sample queries |
| `tests/` | pytest for the pure transforms (no Spark needed) |
| `.azuredevops/` | Azure DevOps pipeline: test → validate → deploy per environment |
| `docs/` | Architecture notes, data sources, learning log |

## Getting started

1. **Local tooling**
   ```bash
   uv venv && source .venv/bin/activate      # or python -m venv .venv
   uv pip install -e ".[dev]"
   pytest
   ```
2. **Databricks CLI** — install, then `databricks auth login --host <workspace-url>`.
3. **Edit `databricks.yml`** — set your workspace host per target and the catalog names.
4. **Deploy to dev**
   ```bash
   databricks bundle validate -t dev
   databricks bundle deploy -t dev
   databricks bundle run collect_snapshots -t dev
   databricks bundle run river_nav_pipeline -t dev
   ```

## First things to verify (week 2 spike)

- [ ] **Corps Locks history depth.** Confirm what the public site exposes and in what
      format. If it is only a rolling window, enable the collector schedule
      immediately so history accumulates. Update `CORPS_LOCKS_SOURCES` in
      `src/river_nav/config.py`.
- [ ] **USGS gauge IDs and parameter codes.** Confirm the monitoring locations in
      `config.py` against <https://waterdata.usgs.gov>. Request a free API key at
      <https://api.waterdata.usgs.gov/signup/> if you hit rate limits.
- [ ] **Lock characteristics file.** Download the Waterway Locks CSV from Data.gov
      and drop it in the `reference` Volume folder.

## Learning path mapping

| Weeks | Focus | What gets built here |
| --- | --- | --- |
| 1–3 | Python, PySpark | `src/river_nav/` transforms + tests |
| 4–5 | Delta, modeling | silver dedupe, gold star schema, SCD2 lock dimension |
| 6–9 | Databricks | Auto Loader, declarative pipeline, Jobs, bundles, DevOps |
| 10–13 | Fabric | OneLake shortcut to gold, Eventstream for gauges, Direct Lake |
| 14–17 | Foundry | Agent with SQL tool + RAG over closure notices; Razor UI |
| 18–20 | MLOps | MLflow lock-delay model, batch scoring, monitoring |

## Blog series

Written alongside the build at [romano.io](https://romano.io):
*From SQL Server to the Lakehouse — a .NET architect's 20 weeks.*

## License

MIT for the code. Source data is U.S. government public domain; cite USACE,
USGS and NOAA when publishing results.
