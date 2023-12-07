FROM apache/airflow:2.7.3

USER root

RUN apt-get update && \
        apt-get install -y --no-install-recommends  build-essential && \
        apt-get autoremove -yqq --purge && \
        apt-get clean

USER airflow

ADD requirements.txt .
RUN python -m pip install -r requirements.txt