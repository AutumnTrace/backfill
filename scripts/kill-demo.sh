#!/usr/bin/env bash
# The uncontrolled version: start an import of a larger seeded file, kill -9 the worker after
# $AFTER seconds wherever it happens to be, resume, compare with an uninterrupted run.
#
#   AFTER=1.5 LINES=200000 scripts/kill-demo.sh
set -euo pipefail
cd "$(dirname "$0")/.."

LINES=${LINES:-200000}
SEED=${SEED:-7}
AFTER=${AFTER:-1.5}
CHUNK=${CHUNK:-1000}
JAR=${BACKFILL_JAR:-target/backfill.jar}
FILE=data/bench-$LINES-$SEED.ndjson
bf() { java -jar "$JAR" "$@"; }

[ -f "$FILE" ] || bf generate --out "$FILE" --lines "$LINES" --seed "$SEED"
for schema in kill_reference killed; do bf --schema "$schema" reset > /dev/null; done
bf --schema kill_reference import "$FILE" --chunk-size "$CHUNK" --quiet | head -1

echo "== import, then kill -9 after ${AFTER}s"
java -jar "$JAR" --schema killed import "$FILE" --chunk-size "$CHUNK" --quiet &
pid=$!
sleep "$AFTER"
kill -9 "$pid" 2> /dev/null || echo "(the import finished before the kill; lower AFTER)"
wait "$pid" 2> /dev/null || true
bf --schema killed status
echo "-- restart"
bf --schema killed resume 1 --quiet | head -1 || true
bf compare kill_reference killed
