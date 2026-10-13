-- Gold star schema: reference DDL and starter queries.
-- The declarative pipeline creates these tables; this file documents the model
-- and gives you T-SQL-style queries to run in Databricks SQL or the Fabric SQL endpoint.

-- ── Dimensions ────────────────────────────────────────────────────────────
-- dim_date   (date_key INT PK, date, year, quarter, month, month_name, day_of_week, day_name, nav_season)
-- dim_time   (time_key INT PK  -- HHMM in 15-minute slots, hour, minute)
-- dim_gauge  (gauge_id STRING PK, gauge_name, river)
-- dim_lock   (lock_sk BIGINT PK, lock_id, name, river, chamber_count, status,
--             valid_from, valid_to, is_current)                       -- SCD Type 2

-- ── Facts ─────────────────────────────────────────────────────────────────
-- fact_river_reading (gauge_id, date_key, time_key, observed_at_utc, stage_ft, discharge_cfs)
--   grain: one gauge × one 15-minute observation
-- fact_river_daily   (gauge_id, date_key, stage_min_ft, stage_max_ft, stage_avg_ft,
--                     discharge_avg_cfs, reading_count)
--   grain: one gauge × one day
-- fact_lockage       TODO week 7: one vessel × one lockage
--   (lock_sk, date_key, time_key, vessel_type, direction, arrival_utc, start_utc,
--    end_utc, delay_min, barge_count)

-- ── Starter queries ───────────────────────────────────────────────────────

-- Latest stage at each gauge (window function you already know from T-SQL)
SELECT gauge_name, observed_at_utc, stage_ft, discharge_cfs
FROM (
  SELECT g.gauge_name, f.observed_at_utc, f.stage_ft, f.discharge_cfs,
         ROW_NUMBER() OVER (PARTITION BY f.gauge_id ORDER BY f.observed_at_utc DESC) AS rn
  FROM nav.fact_river_reading f
  JOIN nav.dim_gauge g ON g.gauge_id = f.gauge_id
) x
WHERE rn = 1;

-- 7-day rise at St. Louis
SELECT d.date, r.stage_max_ft,
       r.stage_max_ft - LAG(r.stage_max_ft, 7) OVER (ORDER BY d.date) AS rise_7d_ft
FROM nav.fact_river_daily r
JOIN nav.dim_date d ON d.date_key = r.date_key
WHERE r.gauge_id = 'USGS-07010000'
ORDER BY d.date DESC
LIMIT 30;

-- Lock attributes as of a point in time (SCD2 lookup)
SELECT lock_id, name, status, chamber_count
FROM nav.dim_lock
WHERE DATE'2026-06-01' >= valid_from
  AND (valid_to IS NULL OR DATE'2026-06-01' < valid_to);
