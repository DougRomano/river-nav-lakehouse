"""silver_lockages (Spark) must match latest_snapshot_lockages (pure Python) on the same files.

Needs pyspark and Java 17+: `uv pip install -e ".[dev,spark]"`. Skipped otherwise.
The declarative-pipelines decorators are stubbed, so this runs on plain Spark 4.0.
"""

import builtins
import importlib.util
import json
import re
import sys
import types
from datetime import UTC, datetime
from pathlib import Path

import pytest

pyspark = pytest.importorskip("pyspark")

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from river_nav.transforms import latest_snapshot_lockages, parse_lockage_record  # noqa: E402

SRC = Path(__file__).resolve().parents[1] / "src" / "pipelines"


def _rec(arr, sol, end, vessel="0608409", lock="26", **kw):
    r = {
        "eroc": "B3", "riverCode": "MI", "riverName": "MISSISSIPPI RIVER", "lockNo": lock,
        "lockName": "X", "direction": "D", "vesselNo": vessel, "vesselName": "BOAT ",
        "arrivalDate": arr, "solDate": sol, "endOfLockage": end, "timezone": "CST",
        "numBarges": 9, "numberProcessed": 9, "hazardCode": "N",
    }
    r.update(kw)
    return r


PULLS = {
    "20261027T110705Z": [
        _rec("10/24/26 12:00", "10/24/26 16:30", "10/24/26 17:30", numberProcessed=6),
        _rec("09/27/26 08:00", "09/27/26 09:00", "09/27/26 10:00", vessel="1111111"),
        _rec("10/25/26 01:00", "10/25/26 02:00", "10/25/26 03:00", vessel="2222222"),
        _rec("10/25/26 01:00", "10/25/26 03:10", "10/25/26 04:00", vessel="2222222"),
    ],
    "20261028T110705Z": [
        _rec("10/24/26 12:00", "10/24/26 13:58", "10/24/26 16:25", numberProcessed=9),
        _rec("10/25/26 01:00", "10/25/26 02:05", "10/25/26 03:30", vessel="2222222"),
        _rec("10/26/26 05:00", "10/26/26 07:00", "10/26/26 08:00", vessel="3333333", lock="27",
             numBarges=None, hazardCode=None),
        _rec("10/26/26 05:00", "10/26/26 05:40", "10/26/26 06:30", vessel="3333333", lock="27"),
        _rec("10/26/26 09:00", "", "", vessel="4444444"),
        _rec("12/05/26 12:06", "12/05/26 12:30", "12/05/26 13:00", vessel="5555555"),
    ],
}
RATE_LIMITED = '{"error": "Rate limit exceeded. Maximum 5 requests per minute allowed."}'


@pytest.fixture(scope="module")
def spark():
    dp = types.ModuleType("pyspark.pipelines")

    def passthrough(*args, **kwargs):
        return lambda f: f

    names = ("table", "materialized_view", "view", "expect", "expect_or_drop", "expect_or_fail")
    for name in names:
        setattr(dp, name, passthrough)
    dp.create_streaming_table = lambda *a, **k: None
    dp.create_auto_cdc_flow = lambda *a, **k: None
    sys.modules["pyspark.pipelines"] = dp
    pyspark.pipelines = dp

    session = (
        SparkSession.builder.master("local[1]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    builtins.spark = session
    yield session
    session.stop()
    del builtins.spark


def _land(root: Path) -> None:
    for stamp, rows in PULLS.items():
        day = f"{stamp[:4]}-{stamp[4:6]}-{stamp[6:8]}"
        folder = root / "corps_locks_traffic_MI26" / f"ingest_date={day}"
        folder.mkdir(parents=True, exist_ok=True)
        (folder / f"corps_locks_traffic_MI26_{stamp}.json").write_text(json.dumps(rows))
    folder = root / "corps_locks_traffic_MI24" / "ingest_date=2026-10-28"
    folder.mkdir(parents=True)
    (folder / "corps_locks_traffic_MI24_20261028T110705Z.json").write_text(RATE_LIMITED)


def test_silver_lockages_matches_pure_python(spark, tmp_path):
    _land(tmp_path)
    stamp_re = re.search(r'_PULL_STAMP = r"(.+)"', (SRC / "bronze.py").read_text()).group(1)

    path = F.col("_metadata.file_path")
    bronze = (
        spark.read.option("multiLine", "true").option("primitivesAsString", "true")
        .option("recursiveFileLookup", "true")
        .option("pathGlobFilter", "corps_locks_traffic_*.json")
        .json(str(tmp_path))
        .select(
            "*",
            F.try_to_timestamp(F.regexp_extract(path, stamp_re, 1), F.lit("yyyyMMdd'T'HHmmss'Z'"))
            .alias("_pulled_at"),
        )
    )
    bronze.createOrReplaceTempView("bronze_corps_locks_traffic")

    spec = importlib.util.spec_from_file_location("silver_under_test", SRC / "silver.py")
    silver = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(silver)
    out = (
        silver.silver_lockages()
        .where("arrival_utc IS NOT NULL")
        .withColumn("start_s", F.date_format("start_utc", "yyyy-MM-dd HH:mm"))
    )
    got = sorted(
        (r.lockage_id, r.start_s, r.number_processed, r.num_barges, r.is_hazardous, r.wait_min)
        for r in out.collect()
    )

    records = []
    for f in tmp_path.rglob("*.json"):
        body = json.loads(f.read_text())
        if not isinstance(body, list):
            continue
        pulled = datetime.strptime(
            re.search(r"_(\d{8}T\d{6}Z)\.json$", f.name).group(1), "%Y%m%dT%H%M%SZ"
        ).replace(tzinfo=UTC)
        records += [parse_lockage_record(r, pulled) for r in body]
    want = sorted(
        (
            p.lockage_id,
            f"{p.start_utc:%Y-%m-%d %H:%M}" if p.start_utc else None,
            p.number_processed,
            p.num_barges,
            p.is_hazardous,
            (p.start_utc - p.arrival_utc).total_seconds() / 60 if p.start_utc else None,
        )
        for p in latest_snapshot_lockages(records)
    )

    assert len(want) == 7
    assert got == want
