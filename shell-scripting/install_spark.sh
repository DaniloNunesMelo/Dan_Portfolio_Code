#!/bin/bash
#https://downloads.apache.org/spark/
pwd
#wget https://downloads.apache.org/spark/spark-3.0.0-preview2/spark-3.0.0-preview2-bin-hadoop3.2.tgz -P /tmp
if [[ $? -eq 0 ]]
     then
       echo "Spark Downloaded successfully"
     else
       echo "Download not completed"
       exit
fi

echo "Starting  unpacking"

cd /usr/local/
pwd
tar -xvf /tmp/spark-3.0.0-preview2-bin-hadoop3.2.tgz

if [[ $? -eq 0 ]]
     then
       echo "Files unzipped successfully"
     else
       echo "Error while unzipping"
       exit
fi
echo "Starting configuration"

sudo ln -s /usr/local/spark3.0/ /usr/local/spark

#sudo chown -RH spark: /usr/local/spark

sudo sh -c 'chmod +x /usr/local/spark/bin/*.sh'

export SPARK_HOME=/usr/local/spark
export PYSPARK_DRIVER_PYTHON=jupyter
export PYSPARK_DRIVER_PYTHON_OPTS='notebook'
export PYSPARK_PYTHON=python3