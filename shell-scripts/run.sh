# run it manually without airflow

# docker compose up -d

# docker compose restart spark

# # creates landing file bucket
# chmod +x minio-setup.sh
# sh minio-setup.sh
# initializes Iceberg tables


# echo -e "AIRFLOW_UID=$(id -u)" > ./airflow/.env
set -e

. ./shell-scripts/exporter.sh ./config # exporting environment variables in config directory


cd ./airflow
mkdir -p ./dags ./logs ./plugins ./config
cd .. 

docker compose up minio mc nessie -d
docker compose -f ./airflow/airflow-compose.yaml up airflow-init -d
docker compose -f ./airflow/airflow-compose.yaml up -d  

sudo chmod 777 airflow/logs/

sh ./shell-scripts/setup/minio-setup.sh

docker compose up spark -d

docker exec spark spark-submit /spark-container/spark/jobs/init_project.py \
    > bash-logs/0-init_job.log

docker exec spark sh /spark-container/ThriftServer-Iceberg-Nessie.sh \
    > bash-logs/1-thrift-server.log 2>&1 &

sh ./shell-scripts/setup/airflow-setup.sh
# docker exec spark spark-submit /spark-container/spark/jobs/ingest.py -o queued/sampled_data_2.csv -t 2024-09-03 16:35:47.837268 \\
#     > bash-logs/1-ingest.log

# # docker exec spark spark-submit /soda/checks/bronz_amazon_ORDERS.py \
# #     > bash-logs/2-bronze_validation.log

# docker exec spark spark-submit /spark-container/spark/jobs/cleansing.py \
#     > bash-logs/3-cleansing.log

# docker exec spark spark-submit /soda/checks/silver_amazon_ORDERS.py \
#     > bash-logs/5-silver_validation.log

# docker exec spark spark-submit /spark/jobs/load.py \
#     > bash-logs/6-load.log