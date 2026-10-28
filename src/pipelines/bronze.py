"""Bronze: raw files → Delta, as received, with ingestion metadata.

Lakeflow Spark Declarative Pipelines Python API. On older runtimes, replace
`from pyspark import pipelines as dp` with `import dlt as dp` (same decorators).
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

RAW = spark.conf.get("river_nav.raw_volume")  # noqa: F821 (spark is injected)


@dp.table(
    name="bronze_usgs_continuous",
    comment="Raw USGS continuous-values pages, one row per GeoJSON page",
    table_properties={"quality": "bronze"},
)
def bronze_usgs_continuous():
    return (
        spark.readStream.format("cloudFiles")  # noqa: F821
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("multiLine", "true")
        .load(f"{RAW}/usgs_continuous/")
        .select(
            "*",
            F.col("_metadata.file_path").alias("_source_file"),
            F.col("_metadata.file_modification_time").alias("_file_modified_at"),
            F.current_timestamp().alias("_ingested_at"),
        )
    )


@dp.table(
    name="bronze_lock_characteristics",
    comment="USACE Waterway Locks reference file(s), as downloaded",
    table_properties={"quality": "bronze"},
)
def bronze_lock_characteristics():
    return (
        spark.readStream.format("cloudFiles")  # noqa: F821
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "false")  # keep strings in bronze
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .load(f"{RAW}/reference/waterway_locks/")
        .select(
            "*",
            F.col("_metadata.file_path").alias("_source_file"),
            F.current_timestamp().alias("_ingested_at"),
        )
    )

# Pull time from the landed file name, e.g. corps_locks_traffic_MI26_20261028T110705Z.json.
# This, not the upload or ingest time, orders revisions in silver.
_PULL_STAMP = r"_(\d{8}T\d{6}Z)\.json$"


@dp.table(
    name="bronze_corps_locks_traffic",
    comment="USACE Corps Locks traffic_report rows, one per array element, strings as received",
    table_properties={"quality": "bronze"},
)
def bronze_corps_locks_traffic():
    path = F.col("_metadata.file_path")
    return (
        spark.readStream.format("cloudFiles")  # noqa: F821
        .option("cloudFiles.format", "json")
        .option("multiLine", "true")  # each file is one JSON array
        .option("cloudFiles.inferColumnTypes", "false")  # keep strings in bronze
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        # One root plus a file-name filter. A glob over six lock folders that each hold
        # ingest_date=... folders makes Spark's partition discovery fail. VERIFY on the
        # first run that the empty partitionColumns turns partition inference off.
        .option("cloudFiles.partitionColumns", "")
        .option("pathGlobFilter", "corps_locks_traffic_*.json")
        .load(f"{RAW}/")
        .select(
            "*",
            path.alias("_source_file"),
            F.try_to_timestamp(
                F.regexp_extract(path, _PULL_STAMP, 1), F.lit("yyyyMMdd'T'HHmmss'Z'")
            ).alias("_pulled_at"),
            F.current_timestamp().alias("_ingested_at"),
        )
    )
