#!/bin/sh
# One-shot: wait for MinIO, configure mc alias, create the warehouse bucket.
# Runs in a `minio/mc` sidecar container (not part of the minio image itself).
set -e

echo "[minio-init] Waiting for MinIO at ${MINIO_ENDPOINT} ..."
until mc alias set minio "${MINIO_ENDPOINT}" "${MINIO_ROOT_USER}" "${MINIO_ROOT_PASSWORD}" >/dev/null 2>&1; do
  sleep 2
done
echo "[minio-init] MinIO reachable."

mc mb --ignore-existing "minio/${MINIO_BUCKET}"
echo "[minio-init] Bucket '${MINIO_BUCKET}' ready."