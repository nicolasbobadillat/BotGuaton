FROM apache/airflow:2.7.1-python3.11

USER airflow
COPY infra/requirements-airflow.txt /requirements-airflow.txt
RUN pip install --no-cache-dir -r /requirements-airflow.txt

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

RUN export PYTHONPATH=$(find /home/airflow/.local/lib -name "site-packages" | head -n 1) && \
    /home/airflow/.local/bin/playwright install-deps chromium

USER airflow
RUN playwright install chromium
