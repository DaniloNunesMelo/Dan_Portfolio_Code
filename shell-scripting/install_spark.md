# Installing Spark

```ascii
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version 3.0.0-preview2
      /_/
```

```bash
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
```

## Checking integrity

```bash
#wget https://downloads.apache.org/spark/spark-3.0.0-preview2/spark-3.0.0-preview2-bin-hadoop3.2.tgz.asc -P /tmp
gpg --verify /tmp/spark-3.0.0-preview2-bin-hadoop3.2.tgz.asc /tmp/spark-3.0.0-preview2-bin-hadoop3.2.tgz
```

## Unpacking

```bash
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
```

## Creation Link

```bash
sudo ln -s /usr/local/spark3.0/ /usr/local/spark

#sudo chown -RH spark: /usr/local/spark

sudo sh -c 'chmod +x /usr/local/spark/bin/*.sh'

```

[spark-local](http://localhost:8080/)

export SPARK_HOME=/usr/local/spark

* Linking Spark and Jupyter

```bash
export PYSPARK_DRIVER_PYTHON=jupyter
export PYSPARK_DRIVER_PYTHON_OPTS='notebook'
export PYSPARK_PYTHON=python3


```

## Start a standalone master server

* At this point you can browse to <http://localhost:8080/> to view the status screen.
sudo $SPARK_HOME/sbin/start-master.sh

### Start a worker process

```bash
sudo $SPARK_HOME/sbin/start-slave.sh spark://ethane:7077
```

### Test out the Spark shell. You’ll note that this exposes the native Scala interface to Spark

* Scala

```bash
$SPARK_HOME/bin/spark-shell
```

### Maybe Scala is not your cup of tea and you’d prefer to use Python. No problem

* Python

```bash
$SPARK_HOME/bin/pyspark
```

[spark-jupyter] https://medium.com/@am.benatmane/setting-up-a-spark-environment-with-jupyter-notebook-and-apache-zeppelin-on-ubuntu-e12116d6539e

```bash

pip install spylon-kernel
# or
conda install -c conda-forge spylon-kernel

You can use spylon-kernel as Scala kernel for Jupyter Notebook. Do this when you want to work with Spark in Scala with a bit of Python code mixed in.
Create a kernel spec for Jupyter notebook by running the following command:

python -m spylon_kernel install --user

```

### Finally, if you prefer to work with R, that’s also catered for

* R

```bash
$SPARK_HOME/bin/sparkR
```

[spark-on-ubuntu](https://datawookie.netlify.app/blog/2017/07/installing-spark-on-ubuntu/)

[scala-commans](https://data-flair.training/blogs/scala-spark-shell-commands/)




[spark-cluster] https://www.tutorialkart.com/apache-spark/how-to-setup-an-apache-spark-cluster/



