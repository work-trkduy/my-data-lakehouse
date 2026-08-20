#!/bin/bash
#
# Creates the warehouse catalog in Apache Polaris pointing at MinIO.
# Mirrors the official Polaris quickstart/bootstrap flow, adapted for MinIO.
# Runs in the `polaris-setup` one-shot container before the Polaris server serves requests.
set -e

REALM="${POLARIS_REALM:-POLARIS}"
ROOT_USER="${POLARIS_ROOT_USER:-root}"
ROOT_PASSWORD="${POLARIS_ROOT_PASSWORD:-s3cr3t}"
CATALOG="${POLARIS_CATALOG:-quickstart_catalog}"

MINIO_INTERNAL="${MINIO_ENDPOINT}"
MINIO_EXTERNAL="${MINIO_EXTERNAL_ENDPOINT}"
S3_KEY="${MINIO_ROOT_USER:-minioadmin}"
S3_SECRET="${MINIO_ROOT_PASSWORD:-minioadmin}"
S3_REGION="${MINIO_REGION:-us-west-2}"
BUCKET="${MINIO_BUCKET:-warehouse}"

POLARIS_URL="http://polaris:8181"

echo "[setup] Waiting for Polaris server..."
for i in $(seq 1 60); do
  if curl -sf http://polaris:8182/q/health >/dev/null 2>&1; then
    break
  fi
  sleep 2
  if [ "$i" = "60" ]; then
    echo "[setup] Polaris did not become healthy in time" >&2
    exit 1
  fi
done
echo "[setup] Polaris server is up."

echo "[setup] Obtaining root access token (realm=$REALM)..."
TOKEN=$(curl -sf \
  -X POST "${POLARIS_URL}/api/catalog/v1/oauth/tokens" \
  --user "${ROOT_USER}:${ROOT_PASSWORD}" \
  -H "Polaris-Realm: ${REALM}" \
  -d 'grant_type=client_credentials' \
  -d 'scope=PRINCIPAL_ROLE:ALL' | sed -n 's/.*"access_token"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p')

if [ -z "${TOKEN}" ]; then
  echo "[setup] Failed to obtain root token" >&2
  exit 1
fi
echo "[setup] Token obtained."

if curl -sf -o /dev/null \
    -H "Authorization: Bearer ${TOKEN}" \
    -H "Polaris-Realm: ${REALM}" \
    "${POLARIS_URL}/api/management/v1/catalogs/${CATALOG}"; then
  echo "[setup] Catalog '${CATALOG}' already exists; skipping creation."
else
  echo "[setup] Creating catalog '${CATALOG}' pointing at s3://${BUCKET} ..."

  PAYLOAD=$(cat <<ENDJSON
{
  "catalog": {
    "name": "${CATALOG}",
    "type": "INTERNAL",
    "readOnly": false,
    "properties": {
      "default-base-location": "s3://${BUCKET}"
    },
    "storageConfigInfo": {
      "storageType": "S3",
      "allowedLocations": ["s3://${BUCKET}"],
      "endpoint": "${MINIO_EXTERNAL}",
      "endpointInternal": "${MINIO_INTERNAL}",
      "pathStyleAccess": true,
      "region": "${S3_REGION}"
    }
  }
}
ENDJSON
  )

  curl -sfX POST \
    -o /dev/null \
    -H "Authorization: Bearer ${TOKEN}" \
    -H "Accept: application/json" \
    -H "Content-Type: application/json" \
    -H "Polaris-Realm: ${REALM}" \
    --data "${PAYLOAD}" \
    "${POLARIS_URL}/api/management/v1/catalogs"

  echo "[setup] Catalog '${CATALOG}' created successfully."
fi

# Grant CATALOG_MANAGE_CONTENT on the catalog to the catalog_admin role so the
# root principal can create namespaces/tables via the contract (CREATE DATABASE /
# CREATE TABLE in Flink). Without this the root role gets NotAuthorizedException.
echo "[setup] Granting CATALOG_MANAGE_CONTENT on catalog '${CATALOG}'..."
curl -sf -o /dev/null \
  -X PUT \
  -H "Authorization: Bearer ${TOKEN}" \
  -H "Accept: application/json" \
  -H "Content-Type: application/json" \
  -H "Polaris-Realm: ${REALM}" \
  --data '{"privilege":"CATALOG_MANAGE_CONTENT","type":"catalog"}' \
  "${POLARIS_URL}/api/management/v1/catalogs/${CATALOG}/catalog-roles/catalog_admin/grants"

echo "[setup] Grant applied."
exit 0