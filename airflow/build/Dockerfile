FROM apache/airflow:2.6.3-python3.11

USER root

RUN apt-get update && \
        apt-get install -y --no-install-recommends  build-essential && \
        apt-get autoremove -yqq --purge && \
        apt-get clean

USER airflow

ADD requirements.txt .
RUN python -m pip --no-cache-dir install --upgrade pip && python -m pip --no-cache-dir install -r requirements.txt