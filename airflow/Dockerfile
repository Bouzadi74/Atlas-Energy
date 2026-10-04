FROM apache/airflow:slim-3.3.1-python3.12

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV ATLAS_PROJECT_ROOT=/opt/atlas
ENV ATLAS_DATA_ROOT=/opt/atlas/data
WORKDIR /opt/atlas
COPY pyproject.toml uv.lock README.md ./
COPY src ./src
COPY configs ./configs
COPY kafka/schemas ./kafka/schemas
COPY airflow/dags /opt/airflow/dags
RUN mkdir -p /opt/atlas/data /opt/atlas/.spark \
    && chown -R airflow:0 /opt/atlas /opt/airflow/dags

USER airflow
RUN pip install --no-cache-dir "apache-airflow[postgres]==${AIRFLOW_VERSION}" "uv==0.12.11" \
    && uv sync --locked --no-dev --extra data
