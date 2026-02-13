FROM apache/airflow:3.1.7-python3.12

USER root
USER airflow

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt \
    && pip install --no-cache-dir apache-airflow-providers-fab
