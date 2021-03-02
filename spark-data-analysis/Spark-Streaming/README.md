# Pyspark Structured Streaming

```bash
   ____              __     ______                     _          
  / __/__  ___ _____/ /__  / __/ /________ ___ ___ _  (_)__  ___ _
 _\ \/ _ \/ _ `/ __/  '_/ _\ \/ __/ __/ -_) _ `/  ' \/ / _ \/ _ `/
/___/ .__/\_,_/_/ /_/\_\ /___/\__/_/  \__/\_,_/_/_/_/_/_//_/\_, / 
   /_/                                                     /___/  
```

The purpose of this is trying to simulate a real-world data streaming process.

1. Using Cluster created in docker
2. Explore data using Jupyter Notebooks
3. Creating a script for data pipeline using spark-submit
4. Using a shell script the simulate an ongoing data flow for streaming.

## Exploring Data Streaming

`Spark-Streaming.ipynb`

## Spark Submit

```bash
$SPARK_HOME/bin/spark-submit \
    --master spark://172.25.0.101:7077 \
    --executor-memory 10G \
    /home/jovyan/spark/Italy-Migr-Streaming.py \
    1000

```

## Generating Fake Data

`InsertStreamingFakeData.sh`
