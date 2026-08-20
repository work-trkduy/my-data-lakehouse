# Debezium Connect with the Postgres CDC connector, plus the connector-registration
# script. The register script runs as a sidecar container (debezium-register) that
# waits for the Connect REST API and POSTs the connector config.
FROM debezium/connect:3.0.0.Final

COPY register-debezium.sh /scripts/register-debezium.sh
COPY debezium-register.json /scripts/debezium-register.json