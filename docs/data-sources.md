# Data sources

All sources are U.S. government public data. Nothing proprietary or client-owned is used.

| Source | Publisher | Use | Cadence | Notes |
| --- | --- | --- | --- | --- |
| Corps Locks (LPMS public snapshot) | USACE Navigation Data Center | Vessel transits, lock queues, closures → `fact_lockage` | ~30 min | Company/commodity detail is withheld. **Week-2 spike: confirm format and history depth.** <https://catalog.data.gov/dataset/corps-locks> |
| Waterway Locks | USACE NDC via BTS NTAD | Lock characteristics → `dim_lock` (SCD2) | Periodic | CSV / GeoJSON download. Data dictionary: <https://doi.org/10.21949/180Q-FC93>. <https://catalog.data.gov/dataset/waterway-locks1> |
| USGS Water Data API | USGS | Stage (00065), discharge (00060) → `fact_river_reading` | ~15 min | Use `api.waterdata.usgs.gov` only; legacy `waterservices.usgs.gov` is retired. Free key: <https://api.waterdata.usgs.gov/signup/>. Recent values are **provisional** and get revised. |
| NOAA Climate Data Online | NOAA NCEI | Daily precipitation and temperature (ML features) | Daily | TODO week 18 |
| Navigation notices, river forecasts | USACE districts, NWS | Unstructured text for Foundry RAG | As published | TODO week 16 |

## Known data quirks (keep adding as you find them)

- USGS no-data sentinel `-999999` → null in silver.
- USGS timestamps carry offsets; everything is stored in UTC, with Central-time date/time keys in gold.
- A provisional USGS value can be re-delivered as approved later; silver upserts, approved wins.
- Waterway Locks column names in `silver.py` are marked VERIFY until checked against the dictionary.
