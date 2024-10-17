
# docker network connect airflow-network spark
# docker network connect airflow-network minio





spark_ssh_extra=$(cat <<EOF
{
    "AWS_ACCESS_KEY_ID": "${AWS_ACCESS_KEY}",
    "AWS_SECRET_ACCESS_KEY": "${AWS_SECRET_KEY}",
    "AWS_REGION": "${AWS_REGION}",
    "AWS_DEFAULT_REGION": "${AWS_DEFAULT_REGION}"
}
EOF
)

minio_extra=$(cat <<EOF
{
    "aws_access_key_id": "${AWS_ACCESS_KEY}",
    "aws_secret_access_key": "${AWS_SECRET_KEY}",
    "endpoint_url": "http://minio:9000"
}
EOF
)

echo "$spark_ssh_extra"
echo "$minio_extra"
# Add sparkSSH connection
docker exec airflow-scheduler /home/airflow/.local/bin/airflow \
    connections add 'sparkSSH' \
    --conn-type 'ssh' \
    --conn-login 'me' \
    --conn-password 'changeme' \
    --conn-host 'spark' \
    --conn-port '22' \
    --conn-extra "$spark_ssh_extra"

# Add minio_connection
docker exec airflow-scheduler /home/airflow/.local/bin/airflow \
    connections add 'minio_connection' \
    --conn-type 'aws' \
    --conn-extra "$minio_extra"



# docker exec webserver /home/airflow/.local/bin/airflow variables set MINIO_ACCESS_KEY ${MINIO_ACCESS_KEY} && \
# docker exec webserver /home/airflow/.local/bin/airflow variables set MINIO_SECRET_KEY ${MINIO_SECRET_KEY} && \
# docker exec webserver /home/airflow/.local/bin/airflow variables set MINIO_ENDPOINT ${MINIO_ENDPOINT}
# docker exec webserver /home/airflow/.local/bin/airflow variables set QUEUED_BUCKET ${QUEUED_BUCKET}
# docker exec webserver /home/airflow/.local/bin/airflow variables set PROCESSED_BUCKET ${PROCESSED_BUCKET}

