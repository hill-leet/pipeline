FROM apache/airflow:2.5.3

ENV AIRFLOW_HOME=/opt/airflow

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

WORKDIR ${AIRFLOW_HOME}

COPY scripts scripts
