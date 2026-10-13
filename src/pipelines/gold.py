"""Gold: business-ready star schema for Fabric (OneLake shortcut), Foundry and ML.

  dim_date, dim_time, dim_gauge, dim_lock (SCD2)
  fact_river_reading   — 15-minute stage and discharge, pivoted per gauge
  fact_river_daily     — daily aggregates used as ML features
  fact_lockage         — TODO once Corps Locks data lands (week 7)
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

from river_nav.config import GAUGES, PARAM_DISCHARGE_CFS, PARAM_GAGE_HEIGHT_FT

GOLD = {"quality": "gold"}


@dp.materialized_view(name="dim_date", comment="Calendar 2015–2035", table_properties=GOLD)
def dim_date():
    days = spark.sql(  # noqa: F821
        "SELECT explode(sequence(DATE'2015-01-01', DATE'2035-12-31', INTERVAL 1 DAY)) AS d"
    )
    return days.select(
        F.date_format("d", "yyyyMMdd").cast("int").alias("date_key"),
        F.col("d").alias("date"),
        F.year("d").alias("year"),
        F.quarter("d").alias("quarter"),
        F.month("d").alias("month"),
        F.date_format("d", "MMMM").alias("month_name"),
        F.dayofweek("d").alias("day_of_week"),
        F.date_format("d", "EEEE").alias("day_name"),
        # Navigation season: winter closures on the upper river are typical Dec–Mar
        F.when(F.month("d").isin(12, 1, 2, 3), "Winter").otherwise("Open").alias("nav_season"),
    )


@dp.materialized_view(name="dim_time", comment="15-minute slots", table_properties=GOLD)
def dim_time():
    slots = spark.range(0, 96).withColumnRenamed("id", "slot")  # noqa: F821
    return slots.select(
        (F.floor(F.col("slot") / 4) * 100 + (F.col("slot") % 4) * 15).cast("int").alias("time_key"),
        F.floor(F.col("slot") / 4).cast("int").alias("hour"),
        ((F.col("slot") % 4) * 15).cast("int").alias("minute"),
    )


@dp.materialized_view(name="dim_gauge", comment="Tracked USGS gauges", table_properties=GOLD)
def dim_gauge():
    rows = [(g.monitoring_location_id, g.name, g.river) for g in GAUGES]
    return spark.createDataFrame(rows, "gauge_id string, gauge_name string, river string")  # noqa: F821


@dp.materialized_view(name="dim_lock", comment="Lock dimension (SCD2)", table_properties=GOLD)
def dim_lock():
    h = spark.read.table("silver_lock_history")  # noqa: F821
    return h.select(
        F.xxhash64("lock_id", "__START_AT").alias("lock_sk"),
        "lock_id", "name", "river", "chamber_count", "status",
        F.col("__START_AT").alias("valid_from"),
        F.col("__END_AT").alias("valid_to"),
        F.col("__END_AT").isNull().alias("is_current"),
    )


@dp.materialized_view(
    name="fact_river_reading",
    comment="Stage (ft) and discharge (cfs) per gauge per 15-minute slot, Central time keys",
    table_properties=GOLD,
    cluster_by=["gauge_id", "date_key"],
)
def fact_river_reading():
    r = spark.read.table("silver_gauge_readings")  # noqa: F821
    local = F.from_utc_timestamp("observed_at_utc", "America/Chicago")
    pivoted = (
        r.groupBy("monitoring_location_id", "observed_at_utc")
        .pivot("parameter_code", [PARAM_GAGE_HEIGHT_FT, PARAM_DISCHARGE_CFS])
        .agg(F.first("value"))
    )
    return pivoted.select(
        F.col("monitoring_location_id").alias("gauge_id"),
        F.date_format(local, "yyyyMMdd").cast("int").alias("date_key"),
        (F.hour(local) * 100 + F.floor(F.minute(local) / 15) * 15).cast("int").alias("time_key"),
        "observed_at_utc",
        F.col(PARAM_GAGE_HEIGHT_FT).alias("stage_ft"),
        F.col(PARAM_DISCHARGE_CFS).alias("discharge_cfs"),
    )


@dp.materialized_view(
    name="fact_river_daily",
    comment="Daily river conditions per gauge; ML feature source",
    table_properties=GOLD,
)
def fact_river_daily():
    f = spark.read.table("fact_river_reading")  # noqa: F821
    return f.groupBy("gauge_id", "date_key").agg(
        F.min("stage_ft").alias("stage_min_ft"),
        F.max("stage_ft").alias("stage_max_ft"),
        F.avg("stage_ft").alias("stage_avg_ft"),
        F.avg("discharge_cfs").alias("discharge_avg_cfs"),
        F.count("*").alias("reading_count"),
    )
