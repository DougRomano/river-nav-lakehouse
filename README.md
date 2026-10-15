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

Until the Databricks job is scheduled, the `scripts/` folder lands the same raw
files under `data/raw/` in the same folder layout the Volume uses, so they can be
uploaded later with `databricks fs cp`.

## Repo layout

| Path | What it is |
| --- | --- |
| `databricks.yml` | Automation Bundle root (dev / test / prod targets) |
| `resources/` | Bundle resources: the declarative pipeline and Lakeflow Jobs |
| `src/river_nav/` | Pure-Python library: config, API clients, landing, transforms (unit tested) |
| `src/notebooks/` | Collector notebook that lands raw API responses in a Volume |
| `src/pipelines/` | Lakeflow Spark Declarative Pipeline: bronze, silver, gold |
| `scripts/` | Local stopgaps: Corps Locks collector, USGS backfill, lock reference CSV, gauge alignment check |
| `sql/` | Gold star-schema reference DDL and sample queries |
| `tests/` | pytest for the pure transforms and landing code (no Spark needed) |
| `Makefile` | `make check` runs pytest and ruff with the project venv |
| `.env.example` | Template for local secrets; copy to `.env` (gitignored) |
| `data/` | Local raw landing zone, gitignored |
| `.azuredevops/` | Azure DevOps pipeline: test → validate → deploy per environment |
| `docs/` | Architecture notes, data sources, learning log |

## Getting started

1. **Local tooling**
   ```bash
   uv venv && source .venv/bin/activate      # or python -m venv .venv
   uv pip install -e ".[dev]"
   make check                                 # pytest + ruff
   ```
2. **Secrets.** Copy `.env.example` to `.env` and set `USGS_API_KEY`. The key is
   optional but raises the USGS rate limit. `.env` is gitignored. The USGS client
   only reads the process environment, so either export the variable
   (`set -a; source .env; set +a`) or use `scripts/backfill_usgs.py`, which reads
   `.env` itself when the shell has not exported it. On Databricks, put the key in
   a secret scope instead.
3. **Land data locally** (optional, until the job is scheduled)
   ```bash
   python scripts/collect_local.py            # Corps Locks snapshots, ~4 min at 5 req/min
   python scripts/backfill_usgs.py --days 400 # discharge + gage height, 4 gauges
   python scripts/collect_locks.py            # Waterway Locks CSV (lock characteristics)
   python scripts/align_gauges.py --days 2    # 15-minute grid, checked against the pure functions
   ```
4. **Databricks CLI** — install, then `databricks auth login --host <workspace-url>`.
5. **Edit `databricks.yml`** — set your workspace host per target and the catalog names.
6. **Deploy to dev**
   ```bash
   databricks bundle validate -t dev
   databricks bundle deploy -t dev
   databricks bundle run collect_snapshots -t dev
   databricks bundle run river_nav_pipeline -t dev
   ```

## Transforms

Business rules live first as pure functions in `src/river_nav/transforms.py`, each
with a pytest, and the pipeline mirrors them in Spark. The gauge-alignment rules
exist in two spellings that must agree:

| Pure Python | pandas | What it does |
| --- | --- | --- |
| `interpolate_at` | `interpolate_frame` | Value of a gauge series at each grid time |
| `bisect_right - 1` | `merge_asof(direction="backward")` | Last reading at or before the grid time |
| `bisect_right` | `merge_asof(direction="forward", allow_exact_matches=False)` | First reading strictly after |
| per-gauge list | `by="gauge_id"` | Partition the match per gauge |

`tests/test_interpolate_asof.py` runs the same readings through both and asserts
every cell matches. `scripts/align_gauges.py` repeats that check against live
USGS data for the four gauges.

## Status

- **Done:** 15-minute gauge alignment (`interpolate_at`, `interpolate_frame`,
  `fifteen_minute_grid`, `interpolate_angle`), shared landing function used by the
  notebook and the local scripts, 400-day USGS backfill of discharge and gage
  height for Grafton, St. Louis, Chester and Valley City, Waterway Locks reference
  CSV landed. 42 tests, no Spark.
- **Next:** mirror the interpolation rules in the gold pipeline, which still only
  floors the observed minute into `time_key`.

## First things to verify

- [x] **Corps Locks history depth.** Traffic and queue are a rolling window of
      roughly 30 days, so `scripts/collect_local.py` must run at least daily
      until the Databricks job takes over.
- [x] **USGS gauge IDs and parameter codes.** The four monitoring locations in
      `config.py` return live data for `00060` and `00065` from the
      `api.waterdata.usgs.gov` OGC API.
- [x] **Lock characteristics file.** The BTS Hub CSV export of the USACE Waterway
      Locks layer (234 chamber rows, all six target locks present) lands under
      `reference/waterway_locks/` via `scripts/collect_locks.py`. Upload the file
      to the Volume with `databricks fs cp` until the job collects it. `STATUS`
      code meanings still need the data dictionary (`VERIFY`).

## Learning path mapping

| Weeks | Focus | What gets built here |
| --- | --- | --- |
| 1–3 | Python, PySpark | `src/river_nav/` transforms + tests, local landing scripts |
| 4–5 | Delta, modeling | silver dedupe, gold star schema, SCD2 lock dimension |
| 6–9 | Databricks | Auto Loader, declarative pipeline, Jobs, bundles, DevOps |
| 10–13 | Fabric | OneLake shortcut to gold, Eventstream for gauges, Direct Lake |
| 14–17 | Foundry | Agent with SQL tool + RAG over closure notices; Razor UI |
| 18–20 | MLOps | MLflow lock-delay model, batch scoring, monitoring |

The week-by-week record is in `docs/learning-log.md`.

## Blog series

Written alongside the build at [romano.io](https://romano.io):
*From SQL Server to the Lakehouse — a .NET architect's 20 weeks.*

## License

MIT for the code. Source data is U.S. government public domain; cite USACE,
USGS and NOAA when publishing results.
