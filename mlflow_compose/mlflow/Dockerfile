FROM python:3.11-slim

# Directorio de trabajo
WORKDIR /app

# Instalar dependencias
RUN pip install --no-cache-dir \
    mlflow \
    boto3 \
    pymysql \
    cryptography \
    minio

# curl para healthcheck/debug
RUN apt-get update && apt-get install -y curl

# Exponer puerto MLflow
EXPOSE 5000

# Levantar MLflow usando variables de entorno
CMD sh -c 'mlflow server \
  --host 0.0.0.0 \
  --port 5000 \
  --backend-store-uri mysql+pymysql://${MYSQL_USER}:${MYSQL_PASSWORD}@${MYSQL_HOST}:${MYSQL_PORT}/${MLFLOW_DB} \
  --default-artifact-root s3://${MINIO_BUCKET} \
  --serve-artifacts \
  --allowed-hosts "*"'