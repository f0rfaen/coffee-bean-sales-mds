FROM apache/airflow:3.0.4
USER root
RUN apt-get update && apt-get install -y libpq-dev gcc
USER airflow
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt