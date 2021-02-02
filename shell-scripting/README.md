# Installing Spark

```ascii
██████╗  █████╗ ███████╗██╗  ██╗
██╔══██╗██╔══██╗██╔════╝██║  ██║
██████╔╝███████║███████╗███████║
██╔══██╗██╔══██║╚════██║██╔══██║
██████╔╝██║  ██║███████║██║  ██║
╚═════╝ ╚═╝  ╚═╝╚══════╝╚═╝  ╚═╝
```

```ascii
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\
      /_/
```

1. Root access validation

2. Argument validation (The version can be passed as argument)

3. Prepare URL

4. Check parameter or Web Scraping to choose the version

5. Download the version

6. Checking integrity

7. Unpacking

8. Env Variables Configuration

> Selecting Spark Version

```bash
root@4be53fe57784:/spark# ./install_spark.sh
=== You have root access ===
1) SparkR_3.0.1.tar.gz                    4) spark-3.0.1-bin-hadoop2.7.tgz          7) spark-3.0.1.tgz
2) pyspark-3.0.1.tar.gz                   5) spark-3.0.1-bin-hadoop3.2.tgz
3) spark-3.0.1-bin-hadoop2.7-hive1.2.tgz  6) spark-3.0.1-bin-without-hadoop.tgz
Enter the Spark Version to install : 5
```

> using Dockerfile and passing as argument

```bash
-> docker build --tag dan-spark-base:v1.0 .
[+] Building 79.2s (10/10) FINISHED
 => [internal] load build definition from Dockerfile                                                                                                   0.0s
 => => transferring dockerfile: 38B                                                                                                                    0.0s
 => [internal] load .dockerignore                                                                                                                      0.0s
 => => transferring context: 2B                                                                                                                        0.0s
 => [internal] load metadata for docker.io/library/ubuntu:latest                                                                                       2.4s
 => [1/5] FROM docker.io/library/ubuntu@sha256:703218c0465075f4425e58fac086e09e1de5c340b12976ab9eb8ad26615c3715                                        6.4s
 => => resolve docker.io/library/ubuntu@sha256:703218c0465075f4425e58fac086e09e1de5c340b12976ab9eb8ad26615c3715                                        0.0s
 => => sha256:703218c0465075f4425e58fac086e09e1de5c340b12976ab9eb8ad26615c3715 1.20kB / 1.20kB                                                         0.0s
 => => sha256:3093096ee188f8ff4531949b8f6115af4747ec1c58858c091c8cb4579c39cc4e 943B / 943B                                                             0.0s
 => => sha256:f63181f19b2fe819156dcb068b3b5bc036820bec7014c5f77277cfa341d4cb5e 3.31kB / 3.31kB                                                         0.0s
 => => sha256:83ee3a23efb7c75849515a6d46551c608b255d8402a4d3753752b88e0dc188fa 28.57MB / 28.57MB                                                       4.7s
 => => sha256:db98fc6f11f08950985a203e07755c3262c680d00084f601e7304b768c83b3b1 843B / 843B                                                             0.5s
 => => sha256:f611acd52c6cad803b06b5ba932e4aabd0f2d0d5a4d050c81de2832fcb781274 162B / 162B                                                             0.6s
 => => extracting sha256:83ee3a23efb7c75849515a6d46551c608b255d8402a4d3753752b88e0dc188fa                                                              1.3s
 => => extracting sha256:db98fc6f11f08950985a203e07755c3262c680d00084f601e7304b768c83b3b1                                                              0.0s
 => => extracting sha256:f611acd52c6cad803b06b5ba932e4aabd0f2d0d5a4d050c81de2832fcb781274                                                              0.0s
 => [internal] load build context                                                                                                                      0.0s
 => => transferring context: 3.12kB                                                                                                                    0.0s
 => [2/5] RUN apt-get update &&     apt-get install -y supervisor &&     apt-get install -y wget &&     apt-get install -y curl &&     apt-get clean  30.4s
 => [3/5] RUN mkdir spark                                                                                                                              0.5s
 => [4/5] COPY ./install_spark.sh spark                                                                                                                0.1s
 => [5/5] RUN ./spark/install_spark.sh "spark-3.0.1-bin-hadoop3.2.tgz"                                                                                37.3s
 => exporting to image                                                                                                                                 2.0s
 => => exporting layers                                                                                                                                2.0s
 => => writing image sha256:85a62a8e25b2e0dec87abb5024359ee0cfe9f76ee7acbf9c5c957aa33f9ac209                                                           0.0s
 => => naming to docker.io/library/dan-spark-base:v1.0                                                                                                 0.0s
```

## Adtional configuration

### Start a standalone master server

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

[spark-jupyter](https://medium.com/@am.benatmane/setting-up-a-spark-environment-with-jupyter-notebook-and-apache-zeppelin-on-ubuntu-e12116d6539e)

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

*Links

[spark-on-ubuntu](https://datawookie.netlify.app/blog/2017/07/installing-spark-on-ubuntu/)

[scala-commans](https://data-flair.training/blogs/scala-spark-shell-commands/)

[spark-cluster](https://www.tutorialkart.com/apache-spark/how-to-setup-an-apache-spark-cluster/)



