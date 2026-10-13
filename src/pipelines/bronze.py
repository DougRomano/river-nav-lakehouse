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

# TODO (week 7): add bronze_corps_locks_* tables once the source format is known.
