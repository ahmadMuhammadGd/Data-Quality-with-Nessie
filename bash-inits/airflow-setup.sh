export $(grep -v '^\s*#.*' ./config.env | xargs)

docker exec webserver /home/airflow/.local/bin/airflow \
    connections add 'sparkSSH' \
    --conn-type 'ssh' \
    --conn-login 'me' \
    --conn-password 'changeme' \
    --conn-host 'spark' \
    --conn-port '22'


docker exec webserver /home/airflow/.local/bin/airflow \
    connections add 'sodaSSH' \
    --conn-type 'ssh' \
    --conn-login 'me' \
    --conn-password 'changeme' \
    --conn-host 'spark' \
    --conn-port '22'

docker exec webserver /home/airflow/.local/bin/airflow variables set MINIO_ACCESS_KEY ${MINIO_ACCESS_KEY} && \
docker exec webserver /home/airflow/.local/bin/airflow variables set MINIO_SECRET_KEY ${MINIO_SECRET_KEY} && \
docker exec webserver /home/airflow/.local/bin/airflow variables set MINIO_END_POINT ${MINIO_END_POINT}
docker exec webserver /home/airflow/.local/bin/airflow variables set QUEUED_BUCKET ${QUEUED_BUCKET}
docker exec webserver /home/airflow/.local/bin/airflow variables set PROCESSED_BUCKET ${PROCESSED_BUCKET}


docker network connect airflow-network spark
docker network connect airflow-network minio