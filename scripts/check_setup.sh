#!/usr/bin/env bash
# Read-only check of the Databricks setup for river-nav-lakehouse. Changes nothing.
#   scripts/check_setup.sh                 # dev target, DEFAULT profile
#   PROFILE=other TARGET=dev scripts/check_setup.sh
set -uo pipefail
cd "$(dirname "$0")/.."
PROFILE="${PROFILE:-DEFAULT}"
TARGET="${TARGET:-dev}"
CATALOG="${CATALOG:-river_nav_dev}"
VOLUME="${VOLUME:-/Volumes/river_nav_dev/nav_raw/landing}"
DB="databricks -p $PROFILE"

pass() { printf '  PASS  %s\n' "$1"; }
fail() { printf '  FAIL  %s\n        %s\n' "$1" "$2"; }

echo "== 1. CLI"
if v=$(databricks -v 2>&1); then pass "$v"; else fail "databricks CLI" "not on PATH: brew install databricks"; exit 1; fi

echo "== 2. Sign-in (profile $PROFILE)"
if me=$($DB current-user me -o json 2>&1); then
  pass "signed in as $(echo "$me" | sed -n 's/.*"userName": *"\([^"]*\)".*/\1/p' | head -1)"
else fail "current-user me" "$(echo "$me" | tail -1)"; exit 1; fi
host=$(awk -v p="[$PROFILE]" '$0==p{f=1;next} /^\[/{f=0} f&&$1=="host"{print $3}' ~/.databrickscfg 2>/dev/null)
bundle_host=$(sed -n '/^  dev:/,/^  [a-z]*:$/p' databricks.yml | sed -n 's/.*host: *\([^ ]*\).*/\1/p' | head -1)
[ "${host%/}" = "${bundle_host%/}" ] && pass "profile host matches databricks.yml dev host" \
  || fail "host mismatch" "profile: $host  databricks.yml: $bundle_host"

echo "== 3. Unity Catalog"
$DB catalogs get "$CATALOG" >/dev/null 2>&1 && pass "catalog $CATALOG" \
  || fail "catalog $CATALOG" "CREATE CATALOG IF NOT EXISTS $CATALOG;"
$DB schemas get "$CATALOG.nav_raw" >/dev/null 2>&1 && pass "schema $CATALOG.nav_raw" \
  || fail "schema $CATALOG.nav_raw" "CREATE SCHEMA IF NOT EXISTS $CATALOG.nav_raw;"
$DB schemas get "$CATALOG.nav" >/dev/null 2>&1 && pass "schema $CATALOG.nav (pipeline tables)" \
  || fail "schema $CATALOG.nav" "CREATE SCHEMA IF NOT EXISTS $CATALOG.nav;  (pipeline target)"
$DB volumes read "$CATALOG.nav_raw.landing" >/dev/null 2>&1 && pass "volume $CATALOG.nav_raw.landing" \
  || fail "volume $CATALOG.nav_raw.landing" "CREATE VOLUME IF NOT EXISTS $CATALOG.nav_raw.landing;"

echo "== 4. Files in the landing volume"
if ls_out=$($DB fs ls "dbfs:$VOLUME" 2>&1); then
  n=$(echo "$ls_out" | grep -c . || true)
  pass "$n top-level folders"
  for src in usgs_continuous corps_locks_traffic_MI26 reference; do
    echo "$ls_out" | grep -q "^$src" && pass "  $src present" || fail "  $src missing" "upload it (scripts/upload_raw.sh or databricks fs cp)"
  done
else fail "fs ls $VOLUME" "$(echo "$ls_out" | tail -1)"; fi

echo "== 5. Bundle"
if out=$($DB bundle validate -t "$TARGET" 2>&1); then pass "bundle validate -t $TARGET"
else fail "bundle validate -t $TARGET" "$(echo "$out" | grep -iE 'error|warn' | head -3)"; fi
summary=$($DB bundle summary -t "$TARGET" 2>&1 || true)
for r in collect_snapshots refresh_lakehouse river_nav_pipeline; do
  echo "$summary" | grep -A3 "$r" | grep -q "URL:" && pass "deployed: $r" \
    || fail "not deployed: $r" "databricks bundle deploy -t $TARGET"
done

echo "== 6. Tables (needs a SQL warehouse)"
wh=$($DB warehouses list -o json 2>/dev/null | sed -n 's/.*"id": *"\([^"]*\)".*/\1/p' | head -1)
if [ -z "$wh" ]; then fail "SQL warehouse" "none found: create a 2X-Small serverless warehouse, auto-stop 10 min"
else
  pass "warehouse $wh"
  # Statement Execution API; the CLI pretty-prints JSON, so parse it with python3.
  q() {
    $DB api post /api/2.0/sql/statements \
      --json "{\"warehouse_id\":\"$wh\",\"wait_timeout\":\"50s\",\"statement\":\"$1\"}" 2>&1 |
    python3 -c '
import json, sys
raw = sys.stdin.read()
try:
    d = json.loads(raw)
except ValueError:
    print("ERROR " + raw.strip().splitlines()[-1][:200]); sys.exit()
state = d.get("status", {}).get("state")
if state != "SUCCEEDED":
    err = d.get("status", {}).get("error", {}).get("message", "")
    print(f"ERROR {state} {err[:200]}"); sys.exit()
print(d.get("result", {}).get("data_array", []))'
  }
  for t in bronze_corps_locks_traffic silver_lockages silver_gauge_readings; do
    r=$(q "SELECT COUNT(*) FROM $CATALOG.nav.$t")
    case "$r" in
      ERROR*) fail "$t" "$r" ;;
      "[['0']]") fail "$t rows: 0" "table exists but is empty: check the bronze source path and filter" ;;
      *) pass "$t rows: $r" ;;
    esac
  done
  echo "        lockages by lock: $(q "SELECT lock_code, COUNT(*), MIN(arrival_utc)::date, MAX(arrival_utc)::date FROM $CATALOG.nav.silver_lockages GROUP BY lock_code ORDER BY lock_code")"
  echo "        traffic files in bronze: $(q "SELECT COUNT(DISTINCT _source_file), COUNT(*) FROM $CATALOG.nav.bronze_corps_locks_traffic")"
  echo "        gauge readings: $(q "SELECT monitoring_location_id, COUNT(*) FROM $CATALOG.nav.silver_gauge_readings GROUP BY 1 ORDER BY 1")"
fi
echo "== done"
