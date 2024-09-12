
# #################################################################################################
# #                                                                                               #  
# #  This script configures and manages MinIO client (mc) interactions within a Docker container. #
# #                                                                                               #                                                              
# #################################################################################################

# # Configure MinIO client and create the warehouse bucket
docker exec mc \
    bin/sh -c 'until (/usr/bin/mc config host add minio http://minio:9000 admin password) do echo "...waiting..." && sleep 1; done'

# Remove and recreate the warehouse bucket
docker exec mc bin/sh -c '\
    /usr/bin/mc rm -r --force minio/warehouse ; \
    /usr/bin/mc mb minio/warehouse ; \
    /usr/bin/mc policy set public minio/warehouse'

# Remove and recreate the queued bucket
docker exec mc bin/sh -c '\
    /usr/bin/mc rm -r --force minio/queued ; \
    /usr/bin/mc mb minio/queued ; \
    /usr/bin/mc policy set public minio/queued'

# # Remove and recreate the processed bucket
# docker exec mc bin/sh -c '\
#     /usr/bin/mc rm -r --force minio/processed ; \
#     /usr/bin/mc mb minio/processed ; \
#     /usr/bin/mc policy set public minio/processed'

# Copy batchs directory content from host machine to MinIO bucket
# Ensure the path to the directory is correct
docker exec mc bin/sh -c '\
    /usr/bin/mc cp -r /data/original_dataset/batchs/ minio/queued/landing'

# Work around to avoid DNS errors, replacing http://minio:9000 with http://<minio_ip>:9000
minio_container_name='minio'
container_id=$(docker container ls -q --filter "name=${minio_container_name}")
if [ -z "$container_id" ]; then
  echo "container ${minio_container_name} is not found"
  exit 1
fi

container_ip=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' $container_id)
echo "Minio IP: ${container_ip}"
sed -i "s|^MINIO_ENDPOINT.*|MINIO_ENDPOINT=http://${container_ip}:9000|" ./config/minio.env
