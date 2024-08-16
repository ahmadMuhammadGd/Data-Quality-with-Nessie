# run it manually without airflow
set -e 

docker compose up -d

# write on env file to upate minio end point
# Making sure that the minio ip has been updated in spark container
minio_container_name='minio'
container_id=$(docker container ls -q --filter "name=${minio_container_name}")
if [ -z "$container_id" ]; then
  echo "container ${minio_container_name} is not found"
  exit 1
fi

container_ip=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' $container_id)
echo "Minio IP: ${container_ip}"
sed -i "s|^MINIO_END_POINT.*|MINIO_END_POINT=http://${container_ip}:9000|" config.env

docker compose restart spark

docker exec spark spark-submit /spark-jobs/jobs/init_project.py \
    > init_job.log
docker exec spark spark-submit /spark-jobs/jobs/clean_stage_1.py \
    > stage_1.log
docker exec spark spark-submit /spark-jobs/jobs/clean_stage_2.py \
    > stage_2.log
docker exec spark spark-submit /spark-jobs/jobs/insert_into_main.py \
    > insert_job.log