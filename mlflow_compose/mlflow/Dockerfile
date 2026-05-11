FROM python:3.11-slim

# Variables de entorno para MinIO
ENV MLFLOW_S3_ENDPOINT_URL=http://minio:9000 \
    AWS_ACCESS_KEY_ID=admin \
    AWS_SECRET_ACCESS_KEY=supersecret

# Directorio de trabajo
WORKDIR /app

# Instalar dependencias
RUN pip install --no-cache-dir \
    mlflow \
    boto3 \
    pymysql \
    cryptography \
    minio

# Exponer puerto MLflow
EXPOSE 5000

# Levantar servidor MLflow usando MySQL
CMD ["mlflow", "server", \
     "--host", "0.0.0.0", \
     "--port", "5000", \
     "--backend-store-uri", "mysql+pymysql://mlops_user:mlops_pass@mysql_db:3306/mlflow_db", \
     "--default-artifact-root", "s3://mlflow-artifacts", \
     "--serve-artifacts"]