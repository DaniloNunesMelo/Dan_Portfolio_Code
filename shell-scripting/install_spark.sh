#!/bin/bash
# Danilo Nunes Melo
# 01/09/2021
# Spark Installation Automated
# Version 1.0

url_spark='https://downloads.apache.org/spark/spark-3.0.1/'

spark_version=$(curl ${url_spark} | grep tar.gz | awk -F '"' '{ print $6 }' | grep gz$)

# Set PS3 prompt
PS3="Enter the Spark Version to install : "

# set shuttle list
select spk_vrn in spark_version
do
    echo "${spk_vrn} selected"
done

echo "Downloading the packages ${spk_vrn}"

wget ${url_spark}${spk_vrn} -P /tmp

if [[ $? -eq 0 ]]
     then
       echo "Spark Downloaded successfully"
     else
       echo "Download not completed"
       exit
fi

echo "Starting unpacking"

tar -xvf /tmp/${spk_vrn}

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