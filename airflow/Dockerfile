FROM apache/airflow:3.3.2

USER root

# Clean up apt caches in the same layer to keep the image slim
RUN apt-get update && apt-get install -y gcc && apt-get clean

USER airflow 

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt
