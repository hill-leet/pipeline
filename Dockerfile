FROM apache/airflow:2.2.3

ENV AIRFLOW_HOME=/opt/airflow

RUN pip install --no-cache-dir -r requirements.txt

WORKDIR ${AIRFLOW_HOME}

COPY scripts scripts
