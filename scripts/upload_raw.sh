#!/usr/bin/env bash
# Upload USACE files the Mac collected to the Databricks landing Volume.
#
# Free Edition serverless cannot reach the USACE or BTS hosts, so collect_local.py
# runs on the Mac and this script copies the recent ingest_date folders up.
# Auto Loader only reads files it has not seen, so re-copying a folder is harmless.
#
#   scripts/upload_raw.sh                 # last 2 ingest days, dev volume
#   DAYS=45 scripts/upload_raw.sh         # first upload: everything collected so far
#   VOLUME=/Volumes/river_nav_dev/nav_raw/landing PROFILE=DEFAULT scripts/upload_raw.sh
set -euo pipefail

cd "$(dirname "$0")/.."
VOLUME="${VOLUME:-/Volumes/river_nav_dev/nav_raw/landing}"
PROFILE="${PROFILE:-DEFAULT}"
DAYS="${DAYS:-2}"
DATABRICKS="${DATABRICKS:-$(command -v databricks || echo /opt/homebrew/bin/databricks)}"

cutoff=$(date -u -v-"${DAYS}"d +%Y-%m-%d 2>/dev/null || date -u -d "-${DAYS} days" +%Y-%m-%d)

copied=0
for src in data/raw/corps_locks_* data/raw/reference/waterway_locks; do
  [ -d "$src" ] || continue
  rel="${src#data/raw/}"
  for day in "$src"/ingest_date=*; do
    d="${day##*=}"
    [[ "$d" < "$cutoff" ]] && continue
    "$DATABRICKS" fs cp -r --overwrite -p "$PROFILE" \
      "$day" "dbfs:${VOLUME}/${rel}/ingest_date=${d}"
    copied=$((copied + 1))
  done
done
echo "uploaded ${copied} ingest_date folders to ${VOLUME}"
