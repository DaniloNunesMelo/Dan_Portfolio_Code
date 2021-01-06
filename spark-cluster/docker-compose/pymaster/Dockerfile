# PyMaster
FROM jupyter/pyspark-notebook

USER root
RUN mkdir -p /pymaster/data
VOLUME ./pymaster/data:/pymaster/data
LABEL NodeDesc="Spark Master Node Dan Cluster"

# Update
RUN apt-get update && \
    apt-get install -y supervisor && \
    apt-get install -y curl && \
    apt-get clean

## Installing spark dependencies
RUN pip install pyspark[sql] && \
    pip install pyarrow && \
    pip install geopandas && \
    pip install descartes && \
    pip install mapclassify

## Setting master node
RUN cp $SPARK_HOME/conf/spark-env.sh.template $SPARK_HOME/conf/spark-env.sh && \
            echo -e SPARK_MASTER_HOST=\'172.25.0.101\'>> $SPARK_HOME/conf/spark-env.sh

## Setting supervisor
RUN mkdir -p /var/log/supervisor
COPY supervisord.conf /etc/supervisor/conf.d/supervisord.conf

EXPOSE 8080
EXPOSE 7077
EXPOSE 8089

# Stating up the Master Services using supervisord
CMD /usr/bin/supervisord -c /etc/supervisor/conf.d/supervisord.conf


## Checking Master node admin 
HEALTHCHECK CMD curl -f http://172.25.0.101:8080/ || exit 1