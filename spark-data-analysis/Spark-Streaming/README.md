# Pyspark Data Streaming

```bash
   ____              __     ______                     _          
  / __/__  ___ _____/ /__  / __/ /________ ___ ___ _  (_)__  ___ _
 _\ \/ _ \/ _ `/ __/  '_/ _\ \/ __/ __/ -_) _ `/  ' \/ / _ \/ _ `/
/___/ .__/\_,_/_/ /_/\_\ /___/\__/_/  \__/\_,_/_/_/_/_/_//_/\_, / 
   /_/                                                     /___/  
```

## Exploring Data Streaming

`Spark-Streaming.ipynb`

## Spark Submit

```bash
$SPARK_HOME/bin/spark-submit \
    --master spark://172.25.0.101:7077 \
    /home/jovyan/spark/Italy-Migr-Streaming.py \
    1000

```

## Generate Fake Data

`InsertStreamingFakeData.sh`