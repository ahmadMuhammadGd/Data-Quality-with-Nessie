# run it manually without airflow
set -e 

# docker compose up -d

# docker compose restart spark

# # creates landing file bucket
# chmod +x minio-setup.sh
# sh minio-setup.sh
# initializes Iceberg tables
docker exec spark spark-submit /spark/jobs/init_project.py\
    > bash-logs/0-init_job.log

docker exec spark spark-submit /spark/jobs/ingest.py -o queued/sampled_data_1.csv\
    > bash-logs/1-ingest.log

docker exec spark spark-submit /soda/checks/bronz_amazon_sales.py \
    > bash-logs/2-bronze_validation.log

docker exec spark spark-submit /spark/jobs/cleansing.py \
    > bash-logs/3-cleansing.log

docker exec spark spark-submit /soda/checks/silver_amazon_sales.py \
    > bash-logs/5-silver_validation.log

docker exec spark spark-submit /spark/jobs/load.py \
    > bash-logs/6-load.log