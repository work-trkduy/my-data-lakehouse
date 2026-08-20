# Apache Flink 2.1 jobmanager/taskmanager image with Iceberg + Kafka connectors baked in.
# Iceberg bill of materials:
#   - iceberg-flink-runtime-2.1      (Iceberg connector compiled against Flink 2.1)
#   - iceberg-aws-bundle             (S3FileIO + aws hadoop support, MinIO via path-style)
#   - flink-sql-connector-kafka      (Kafka SQL source for Flink 2.1; reads raw JSON envelope)
#   - hadoop-client-*                (hadoop compatible filesystem layer for Iceberg)
FROM flink:2.1.2-scala_2.12

# Bake connector jars straight into the Flink lib dir (replaces the old
# runtime "cp" loop -- no post-start jar installation needed).
COPY flink-jars/iceberg-flink-runtime-2.1-1.11.0.jar        /opt/flink/lib/
COPY flink-jars/iceberg-aws-bundle-1.11.0.jar               /opt/flink/lib/
COPY flink-jars/flink-sql-connector-kafka-5.0.0-2.1.jar     /opt/flink/lib/
COPY flink-jars/hadoop-client-api-3.3.6.jar                 /opt/flink/lib/
COPY flink-jars/hadoop-client-runtime-3.3.6.jar             /opt/flink/lib/