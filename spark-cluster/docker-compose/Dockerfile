FROM ubuntu

USER root

VOLUME /spark
LABEL NodeDesc="Spark Image"

ENV SPARK_HOME=/usr/local/spark
ENV PYSPARK_PYTHON=python3
ENV PYTHONPATH=$(ZIPS=("$SPARK_HOME"/python/lib/*.zip)

RUN apt-get update && \
    apt-get install -y supervisor && \
    apt-get install -y wget && \
    apt-get install -y curl && \
    apt-get clean && \
    apt install default-jdk scala git -ys

RUN mkdir spark

COPY ./install_spark.sh spark

RUN ./spark/install_spark.sh "spark-3.0.1-bin-hadoop3.2.tgz" 

CMD bash -C '/bin/';'bash'