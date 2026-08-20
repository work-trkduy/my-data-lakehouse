#!/usr/bin/env bash
#
# Run a SQL statement against the local Trino (Iceberg catalog) and print the
# result rows as tab-separated text. Polls the /v1/statement API until FINISHED
# or FAILED, accumulating all result pages (results are paged for large sets).
#
# Usage (from the Docker host):
#   scripts/trino-run.sh "SELECT count(*) FROM polaris.transactions_cdc_log"
#   echo "SELECT 1" | scripts/trino-run.sh
#
# Env overrides:
#   TRINO_HOST    (default localhost:8080)
#   TRINO_CATALOG (default iceberg)
#   TRINO_USER    (default admin)

set -euo pipefail

TRINO_HOST="${TRINO_HOST:-localhost:8080}"
TRINO_CATALOG="${TRINO_CATALOG:-iceberg}"
TRINO_USER="${TRINO_USER:-admin}"

if [ "$#" -ge 1 ]; then
  SQL="$1"
else
  SQL="$(cat)"
fi
if [ -z "${SQL}" ]; then
  echo "no SQL given" >&2
  exit 2
fi
# /v1/statement takes ONE statement; drop trailing semicolons (helpers may pass
# SQL that ends with ';', which Trino rejects as a second (empty) statement).
SQL="$(printf '%s' "${SQL}" | sed -e 's/;[[:space:]]*$//')"

resp="$(curl -s -X POST "http://${TRINO_HOST}/v1/statement" \
  -H "X-Trino-User: ${TRINO_USER}" \
  -H "X-Trino-Catalog: ${TRINO_CATALOG}" \
  -d "${SQL}")"

next="$(printf '%s' "${resp}" | python3 -c 'import sys,json; print(json.load(sys.stdin).get("nextUri",""))' 2>/dev/null || true)"
state="$(printf '%s' "${resp}" | python3 -c 'import sys,json; print(json.load(sys.stdin).get("stats",{}).get("state",""))' 2>/dev/null || true)"

allrows=""

for _ in $(seq 1 120); do
  err="$(printf '%s' "${resp}" | python3 -c 'import sys,json; d=json.load(sys.stdin); e=d.get("error"); print(e.get("message","")[:800] if e else "")' 2>/dev/null || true)"
  if [ -n "${err}" ]; then
    echo "ERROR: ${err}" >&2
    exit 1
  fi
  chunk="$(printf '%s' "${resp}" | python3 -c 'import sys,json; d=json.load(sys.stdin); print(json.dumps({"cols":[c["name"] for c in d.get("columns",[])], "rows": d.get("data",[])}))' 2>/dev/null || true)"
  allrows="${allrows}
${chunk}"
  state="$(printf '%s' "${resp}" | python3 -c 'import sys,json; print(json.load(sys.stdin).get("stats",{}).get("state",""))' 2>/dev/null || true)"
  if [ "${state}" = "FINISHED" ]; then
    break
  fi
  if [ -z "${next}" ]; then
    echo "statement ended without FINISHED (state=${state})" >&2
    exit 1
  fi
  resp="$(curl -s "${next}")"
  next="$(printf '%s' "${resp}" | python3 -c 'import sys,json; print(json.load(sys.stdin).get("nextUri",""))' 2>/dev/null || true)"
done

[ "${state}" = "FINISHED" ] || { echo "timed out polling (state=${state})" >&2; exit 1; }

printf '%s' "${allrows}" | python3 -c '
import sys, json
cols = None
rows = []
for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    d = json.loads(line)
    if d.get("cols"):
        cols = d["cols"]
    rows += d.get("rows", [])
if cols:
    print("\t".join(cols))
for r in rows:
    print("\t".join("" if c is None else str(c) for c in r))
'
