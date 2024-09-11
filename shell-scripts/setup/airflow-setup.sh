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


airflow connections add 'minio_connection' \
    --conn-type 'aws' \
    --conn-extra "{
        "aws_access_key"        : "${AWS_ACCESS_KEY}",
        "aws_secret_key"        : "${AWS_SECRET_KEY}",
        "aws_region"            : "${AWS_REGION}",
        "aws_default_region"    : "${AWS_SECRET_KEY}"
    }"


# docker exec webserver /home/airflow/.local/bin/airflow variables set MINIO_ACCESS_KEY ${MINIO_ACCESS_KEY} && \
# docker exec webserver /home/airflow/.local/bin/airflow variables set MINIO_SECRET_KEY ${MINIO_SECRET_KEY} && \
# docker exec webserver /home/airflow/.local/bin/airflow variables set MINIO_ENDPOINT ${MINIO_ENDPOINT}
# docker exec webserver /home/airflow/.local/bin/airflow variables set QUEUED_BUCKET ${QUEUED_BUCKET}
# docker exec webserver /home/airflow/.local/bin/airflow variables set PROCESSED_BUCKET ${PROCESSED_BUCKET}


docker network connect airflow-network spark
docker network connect airflow-network minio