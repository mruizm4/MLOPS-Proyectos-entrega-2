FROM apache/airflow:2.8.1-python3.11

USER airflow
# Actualizar pip para evitar warnings
RUN pip install --upgrade pip

# Copiar requirements
COPY requirements-airflow.txt /requirements.txt

# Instalar dependencias (sin --no-cache para debug si falla)
RUN pip install --user -r /requirements.txt