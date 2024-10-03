echo "NESSIE_URI: ${NESSIE_URI}"
echo "BRANCH_MAIN: ${BRANCH_MAIN}"
echo "MINIO_ACCESS_KEY: ${MINIO_ACCESS_KEY}"
echo "MINIO_SECRET_KEY: ${MINIO_SECRET_KEY}"
echo "MINIO_ENDPOINT: ${MINIO_ENDPOINT}"
echo "MINIO_ICEBERG_S3_BUCKET: ${MINIO_ICEBERG_S3_BUCKET}"
echo "SPARK_HOME: ${SPARK_HOME}"

spark-submit \
    --class org.apache.spark.sql.hive.thriftserver.HiveThriftServer2 \
    --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.92.1 \
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions \
    --conf spark.sql.catalog.nessie=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.nessie.uri=$NESSIE_URI \
    --conf spark.sql.catalog.nessie.ref=$BRANCH_MAIN \
    --conf spark.sql.catalog.nessie.authentication.type=NONE \
    --conf spark.sql.catalog.nessie.catalog-impl=org.apache.iceberg.nessie.NessieCatalog \
    --conf spark.sql.catalog.nessie.warehouse=$MINIO_ICEBERG_S3_BUCKET \
    --conf spark.sql.catalog.nessie.s3.endpoint=$MINIO_ENDPOINT \
    --conf spark.sql.catalog.nessie.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
    --conf spark.hadoop.fs.s3a.access.key=$MINIO_ACCESS_KEY \
    --conf spark.hadoop.fs.s3a.secret.key=$MINIO_SECRET_KEY \
    --conf spark.hadoop.fs.s3a.path.style.access=true \
    --executor-memory 1g \
    $SPARK_HOME/jars/spark-thrift-server_2.12-3.5.0.jar
    # --conf nessie.server.default-branch=$BRANCH_MAIN \
    # --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
    # --conf spark.hadoop.fs.s3a.endpoint=$MINIO_ENDPOINT \