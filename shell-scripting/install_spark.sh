#!/bin/bash
# Danilo Nunes Melo
# Creation date 01/08/2021
# Update date 02/22/2021
# Spark Installation Automated
# Version 1.0

# TODO Verify java version

if [[ ${EUID} -eq 0 ]]
     then
       echo "=== You have root access ==="
     else
       echo "=== Root permission required to install ==="
       exit
fi

if [[ $# -gt 1 ]]
     then
       echo "=== Just one argument allowed: Spark Version ==="
       exit
     else
      spk_vrn=$1
fi


url_spark='https://downloads.apache.org/spark/spark-3.0.1/'
spark_version=$(curl --silent ${url_spark} | grep gz | awk -F '"' '{ print $6 }' | grep gz$) 1>/dev/null 2>&1

# TODO *Verify previous Spark Installation

# Check if the spark version was passed as argument, if not choose the version
if [[ -z ${spk_vrn} ]]
     then
        # Set PS3 prompt
        PS3="Enter the Spark Version to install : "
    
        # set shuttle list
        select spk_vrn in ${spark_version}
        do
            echo "${spk_vrn} selected"
        break;
    
        done
    else
      continue
fi

echo "=== Downloading the package ${spk_vrn} ==="

wget ${url_spark}${spk_vrn} -P /tmp

if [[ $? -eq 0 ]]
     then
       echo "=== Spark Downloaded successfully ==="
     else
       echo "=== Download not completed ==="
       exit
fi

## sha512sum

wget ${url_spark}${spk_vrn}'.sha512' -P /tmp

echo "=== sha512sum ==="
cat ${spk_vrn}'.sha512' | tr '\n' ' '| tr -d ' ' | awk -F ':' '{ print $2 "\t" $1 }'| sha512sum -c -


echo "=== Starting unpacking ==="

# Removing .tgz from spark version
spk_fld=$(echo ${spk_vrn} | awk '{ print substr( $0, 0, length($0)-4 ) }')

if [ -d "/usr/local/spark" ] 
then
    echo "===Directory /usr/local/spark exists.===" 
else
    echo "===Creating Spark directory==="
    mkdir /usr/local/spark
    echo "===Extracting files==="
    tar -xf /tmp/${spk_vrn} -C /tmp
    echo "===Moving files from Temp to Spark Directory==="
    mv /tmp/${spk_fld}/* /usr/local/spark

fi

if [[ $? -eq 0 ]]
     then
       echo "=== Files unzipped successfully ==="
     else
       echo "=== Error while unzipping ==="
       exit
fi

echo "=== Starting configuration ==="

echo "export SPARK_HOME=/usr/local/spark" >> ~/.profile
echo "export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin" >> ~/.profile
echo "export PYSPARK_PYTHON=/usr/bin/python3" >> ~/.profile

if [[ ${spk_vrn} == *pyspark* ]]
     then
       echo "=== Starting configuration Pyspark ==="
       sh -c 'chmod +x /usr/local/spark/bin/*.sh'
       export SPARK_HOME=/usr/local/spark
#       export PYSPARK_DRIVER_PYTHON=jupyter
#       export PYSPARK_DRIVER_PYTHON_OPTS='notebook'
       export PYSPARK_PYTHON=python3
       export PYTHONPATH=$(ZIPS=("$SPARK_HOME"/python/lib/*.zip); IFS=:; echo "${ZIPS[*]}"):$PYTHONPATH
     else
       echo "Error while configuring"
       exit
fi
## TODO *Verify env varialbles for R Spark
if [[ ${spk_vrn} == *SparkR* ]]
     then
       echo "=== Starting configuration SparkR ==="
       sh -c 'chmod +x /usr/local/${spk_vrn}/bin/*.sh'
       export SPARK_HOME=/usr/local/${spk_vrn}
#       export SPARKR_DRIVER_R 'R binary executable to use for SparkR shell (default is R).'
#       export PYSPARK_DRIVER_PYTHON=jupyter
#       export PYSPARK_DRIVER_PYTHON=jupyter

     else
       echo "=== Error while configuring ==="
       exit
fi
