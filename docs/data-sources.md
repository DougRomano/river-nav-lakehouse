# Data sources

All sources are U.S. government public data. Nothing proprietary or client-owned is used.

| Source | Publisher | Use | Cadence | Notes |
| --- | --- | --- | --- | --- |
| Corps Locks Data Web Services | USACE NDC (LPMS) | `traffic_report`: one row per lockage (arrival, start of lockage, end, barges, direction) → `fact_lockage`; also queue, lock status, delay averages, stall/stoppage | Rolling **~30 days**; collect at least daily | Documented public services: <https://ndc.ops.usace.army.mil/ords/r/lpms/corps-locks/data-web-services>. **5 requests/minute.** Full LPMS history is USACE-internal (FOIA). |
| Waterway Locks | USACE NDC via BTS NTAD | Lock characteristics → `dim_lock` (SCD2) | Periodic | ArcGIS FeatureServer: `services7.arcgis.com/n1YM8pTrFmm7L4hs/ArcGIS/rest/services/Locks/FeatureServer/0` (234 rows, one query; last edited 2025-12-11). Page: <https://geodata.bts.gov/datasets/usdot::waterway-locks/about>. CSV export (what the page's Download button fetches, verified 2026-10-15): `config.WATERWAY_LOCKS_CSV_URL`; the FeatureServer `query` endpoint does not serve CSV. Landed by `scripts/collect_locks.py` / `collect_waterway_locks` under `reference/waterway_locks/`. Data dictionary: <https://doi.org/10.21949/180Q-FC93>. |
| USGS Water Data API | USGS | Stage (00065), discharge (00060) → `fact_river_reading` | ~15 min | Use `api.waterdata.usgs.gov` only; legacy `waterservices.usgs.gov` is retired. Free key: <https://api.waterdata.usgs.gov/signup/>. Recent values are **provisional** and get revised. |
| NOAA Climate Data Online | NOAA NCEI | Daily precipitation and temperature (ML features) | Daily | TODO week 18 |
| Navigation notices, river forecasts | USACE districts, NWS | Unstructured text for Foundry RAG | As published | TODO week 16 |

## Where each source is collected

Databricks Free Edition serverless reaches only a list of trusted domains. `src/notebooks/00_check_egress.py`, run 2026-10-28:

| Source | From Free Edition | Collected by |
| --- | --- | --- |
| USGS Water Data API | OK | Databricks job (`collect_snapshots`) |
| USACE Corps Locks | BLOCKED (ConnectionError) | Mac `launchd` → `scripts/collect_local.py`, then `scripts/upload_raw.sh` |
| BTS Waterway Locks | BLOCKED (ConnectionError) | Mac, `scripts/collect_locks.py` when the layer changes, then `scripts/upload_raw.sh` |

## Known data quirks (keep adding as you find them)

- USGS no-data sentinel `-999999` → null in silver.
- USGS timestamps carry offsets; everything is stored in UTC, with Central-time date/time keys in gold.
- A provisional USGS value can be re-delivered as approved later; silver upserts, approved wins.
- USGS `continuous` features also carry `statistic_id` (`00011` = instantaneous), `unit_of_measure` and `last_modified`. `last_modified` is a better tiebreaker for revisions than arrival order.
- Values arrive as strings (`"146000"`, `"16.73"`); typing happens in silver.
- Observed 2026-10-13: St. Louis stage (07010000) returned 1,430 readings for 30 days, so roughly every 30 minutes rather than 15. A 400-day request returned 17,389 rows in 2 pages (~6 s): backfill is one call per gauge and parameter, no scheduler needed for history.
- Observed 2026-10-13: the newest reading was ~2.5 hours behind wall-clock time. Size the collector's lookback window for that lag.
- Corps Locks: a rate-limit breach returns **HTTP 200** with `{"error": "Rate limit exceeded..."}`. `corps_locks_client.polite_get` throttles to one call per 13 s and checks the body.
- Corps Locks: JSON is served as `text/html`, and `lock_delay_json` is not always valid JSON (`"N/A,"` is missing a quote). Land bodies as-is; parse in bronze/silver.
- Corps Locks: dates are `MM/DD/YY HH:MM` labeled `"timezone": "CST"` year-round, but the clock is **local Central time** (America/Chicago). Checked 2026-10-28 on 65,880 landed timestamps: read as fixed CST, 22 fall after the moment the file was pulled; read as America/Chicago, none do. `parse_lpms_time` converts to UTC.
- Corps Locks traffic: one vessel arrival can have **several rows** (a long tow locks through in cuts). Lock + vessel + arrival is not unique; adding start of lockage is, within one pull.
- Corps Locks traffic: the 30-day window is **revised** between pulls. On 2026-10-28, 48 of ~1,650 overlapping arrivals had a changed start, end or `numberProcessed`, and the number of cuts can change. Silver keeps the newest pull per vessel arrival (`latest_snapshot_lockages`).
- Corps Locks traffic: `numBarges`, `hazardCode` and `numberProcessed` are often null (237, 237 and 340 of 2,146 rows on 2026-10-28). A tow still waiting has `solDate` = `""`.
- Corps Locks: queue rows carry `MMSI` (often null) — the same vessel ID AIS uses.
- First pull 2026-10-14: 1,843 lockages at the 6 target locks, 2026-09-14 to 2026-10-13. Median wait (arrival → start of lockage): Peoria 0 min, LaGrange 7, L&D 24 86, L&D 25 137, Mel Price 25, Chain of Rocks 63.
- Waterway Locks: one row per lock **chamber** (`CHAMBN` = MAIN / AUXILLARY [sic] / MAIN1 / CNTRL), key `NDCCODE`. `RIVERCD`+`LOCKCD` identifies the lock.
- Waterway Locks: Mel Price and Chain of Rocks report `NOCHMB` = 2 but only their MAIN chamber row exists in the layer.
- Waterway Locks CSV is UTF-8 with a BOM on the first header (`OBJECTID`). Silver never reads that column, but VERIFY Auto Loader's CSV reader strips it before relying on `OBJECTID`.
- Waterway Locks: `STATUS` is a numeric code (1, 2, 5, 6, 7, 8); meanings still to VERIFY in the data dictionary.
- Waterway Locks is lock **characteristics only** (size, lift, river mile, district). It has no lockage, queue or delay data; that still has to come from Corps Locks / LPMS.
