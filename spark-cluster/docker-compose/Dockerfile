FROM ubuntu

USER root

VOLUME ./pynode/data:/pynode/data
LABEL NodeDesc="Spark Image"

RUN apt-get update && \
    apt-get install -y supervisor && \
    apt-get install -y wget && \
    apt-get install -y curl && \
    apt-get clean

RUN wget https://downloads.apache.org/spark/spark-3.0.1/pyspark-3.0.1.tar.gz -P /tmp && \
    tar -xvf /tmp/pyspark-3.0.1.tar.gz && \
    sudo ln -s /usr/local/spark3.0/ /usr/local/spark && \
    sudo sh -c 'chmod +x /usr/local/spark/bin/*.sh'

ENV export SPARK_HOME=/usr/local/spark
ENV export PYSPARK_PYTHON=python3