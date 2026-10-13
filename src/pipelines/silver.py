"""Silver: typed, cleaned, deduplicated, with data-quality expectations.

Rules mirror river_nav.transforms (the unit-tested specification):
  - timestamps to UTC, no-data sentinels to null
  - provisional readings upserted when USGS revises them (AUTO CDC, SCD1)
  - lock characteristics tracked as SCD Type 2
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

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
    return features.select(
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
    # VERIFY column names against the Waterway Locks data dictionary
    # (https://doi.org/10.21949/180Q-FC93) and adjust the mapping below.
    src = spark.readStream.table("bronze_lock_characteristics")  # noqa: F821
    return src.select(
        F.trim(F.col("NDC_LOCK_ID")).alias("lock_id"),          # VERIFY
        F.initcap(F.trim(F.col("LOCK_NAME"))).alias("name"),     # VERIFY
        F.trim(F.col("RIVER_NAME")).alias("river"),              # VERIFY
        F.col("NUMBER_OF_CHAMBERS").cast("int").alias("chamber_count"),  # VERIFY
        F.upper(F.trim(F.col("STATUS"))).alias("status"),        # VERIFY
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
    track_history_column_list=["name", "river", "chamber_count", "status"],
)
