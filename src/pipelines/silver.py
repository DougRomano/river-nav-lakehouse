"""Silver: typed, cleaned, deduplicated, with data-quality expectations.

Rules mirror river_nav.transforms (the unit-tested specification):
  - timestamps to UTC, no-data sentinels to null
  - instantaneous statistic only (00011, or statistic_id absent); see is_instantaneous
  - provisional readings upserted when USGS revises them (AUTO CDC, SCD1)
  - lock characteristics tracked as SCD Type 2
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

from river_nav.transforms import INSTANTANEOUS_STATISTIC_ID

NO_DATA = -999999.0


# ---------------------------------------------------------------------------
# Gauge readings
# ---------------------------------------------------------------------------

@dp.view(name="v_usgs_readings")
@dp.expect_or_drop("has_location", "monitoring_location_id IS NOT NULL")
@dp.expect_or_drop("has_time", "observed_at_utc IS NOT NULL")
@dp.expect("value_in_range", "value IS NULL OR value BETWEEN -50 AND 2000000")
def v_usgs_readings():
    features = (
        spark.readStream.table("bronze_usgs_continuous")  # noqa: F821
        .select(F.explode("features").alias("f"), "_ingested_at")
    )
    p = "f.properties"
    value = F.col(f"{p}.value").cast("double")
    # is_instantaneous: 00011, or the field omitted. A daily mean must not join the series.
    stat = F.col(f"{p}.statistic_id")
    instantaneous = stat.isNull() | (stat == F.lit(INSTANTANEOUS_STATISTIC_ID))
    return features.where(instantaneous).select(
        F.col(f"{p}.monitoring_location_id").alias("monitoring_location_id"),
        F.col(f"{p}.parameter_code").alias("parameter_code"),
        F.to_utc_timestamp(F.to_timestamp(F.col(f"{p}.time")), "UTC").alias("observed_at_utc"),
        F.when(value == NO_DATA, None).otherwise(value).alias("value"),
        F.col(f"{p}.unit_of_measure").alias("unit"),
        (F.lower(F.coalesce(F.col(f"{p}.approval_status"), F.lit(""))) != "approved")
        .alias("is_provisional"),
        "_ingested_at",
    )


dp.create_streaming_table(
    name="silver_gauge_readings",
    comment="One row per gauge, parameter and timestamp; revised values overwrite provisional",
    table_properties={"quality": "silver"},
    cluster_by=["monitoring_location_id", "observed_at_utc"],
)

dp.create_auto_cdc_flow(
    target="silver_gauge_readings",
    source="v_usgs_readings",
    keys=["monitoring_location_id", "parameter_code", "observed_at_utc"],
    # Approved beats provisional; otherwise latest ingestion wins
    sequence_by=F.struct(F.not_(F.col("is_provisional")), F.col("_ingested_at")),
    stored_as_scd_type=1,
)


# ---------------------------------------------------------------------------
# Lock characteristics (SCD Type 2)
# ---------------------------------------------------------------------------

@dp.view(name="v_lock_characteristics")
@dp.expect_or_drop("has_lock_id", "lock_id IS NOT NULL")
def v_lock_characteristics():
    # Columns verified 2026-10-13 against the BTS/USACE Waterway Locks FeatureServer
    # (see config.WATERWAY_LOCKS_URL). Grain is one row per lock CHAMBER, keyed by NDCCODE.
    # STATUS is a numeric code ('1','2','5'...): VERIFY its meanings in the data dictionary
    # (https://doi.org/10.21949/180Q-FC93) before decoding it in gold.
    src = spark.readStream.table("bronze_lock_characteristics")  # noqa: F821
    return src.select(
        F.trim(F.col("NDCCODE")).alias("lock_id"),
        F.concat_ws("-", F.trim("RIVERCD"), F.trim("LOCKCD")).alias("lock_code"),
        F.initcap(F.trim(F.col("PMSNAME"))).alias("name"),
        F.initcap(F.trim(F.col("CHAMBN"))).alias("chamber"),
        F.initcap(F.trim(F.col("RIVER"))).alias("river"),
        F.col("RIVERMI").cast("double").alias("river_mile"),
        F.col("NOCHMB").cast("int").alias("chamber_count"),
        F.col("LENGTH").cast("double").alias("length_ft"),
        F.col("WIDTH").cast("double").alias("width_ft"),
        F.col("LIFT").cast("double").alias("lift_ft"),
        F.trim(F.col("DISTRICT")).alias("district"),
        F.trim(F.col("STATUS")).alias("status"),
        F.col("_ingested_at"),
    )


dp.create_streaming_table(
    name="silver_lock_history",
    comment="SCD2 history of USACE lock characteristics",
    table_properties={"quality": "silver"},
)

dp.create_auto_cdc_flow(
    target="silver_lock_history",
    source="v_lock_characteristics",
    keys=["lock_id"],
    sequence_by=F.col("_ingested_at"),
    stored_as_scd_type=2,
    track_history_column_list=[
        "name", "chamber_count", "length_ft", "width_ft", "lift_ft", "status",
    ],
)
