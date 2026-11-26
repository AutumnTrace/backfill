#!/usr/bin/env bash
# Kill the worker halfway through a warehouse import. Restart it. Compare the resulting stock
# counts with an uninterrupted run. Deterministic: the JVM halts (as kill -9 would) just before or
# just after the COMMIT of the chunk that holds line $LINE.
#
#   scripts/crash-demo.sh                    LINE=6000 CHUNK=500 FILE=data/scans-demo.ndjson
#
# Needs a built jar (./mvnw -DskipTests package) and PostgreSQL at $BACKFILL_DB_URL.
set -euo pipefail
cd "$(dirname "$0")/.."

FILE=${FILE:-data/scans-demo.ndjson}
LINE=${LINE:-6000}
CHUNK=${CHUNK:-500}
JAR=${BACKFILL_JAR:-target/backfill.jar}
bf() { java -jar "$JAR" "$@"; }

for schema in reference crash_before crash_after; do bf --schema "$schema" reset > /dev/null; done

echo "== reference: one uninterrupted import of $FILE in chunks of $CHUNK"
bf --schema reference import "$FILE" --chunk-size "$CHUNK" --quiet

for point in before-commit after-commit; do
  schema=crash_${point%-commit}
  echo
  echo "== $schema: halt the JVM $point in the chunk holding line $LINE"
  set +e
  bf --schema "$schema" import "$FILE" --chunk-size "$CHUNK" --quiet --crash-at-line "$LINE" --crash-point "$point"
  code=$?
  set -e
  echo "(worker exited with $code)"
  bf --schema "$schema" status
  echo "-- restart"
  bf --schema "$schema" resume 1 --quiet | head -1
done

echo
echo "== compare with the uninterrupted run"
bf compare reference crash_before
bf compare reference crash_after
