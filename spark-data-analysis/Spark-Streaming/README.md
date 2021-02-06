# Pyspark Data Streaming

```bash
#                                       __                    __                                       _                  
#     _____    ____   ____ _   _____   / /__          _____  / /_   _____  ___   ____ _   ____ ___    (_)   ____    ____ _
#    / ___/   / __ \ / __ `/  / ___/  / //_/         / ___/ / __/  / ___/ / _ \ / __ `/  / __ `__ \  / /   / __ \  / __ `/
#   (__  )   / /_/ // /_/ /  / /     / ,<           (__  ) / /_   / /    /  __// /_/ /  / / / / / / / /   / / / / / /_/ / 
#  /____/   / .___/ \__,_/  /_/     /_/|_|         /____/  \__/  /_/     \___/ \__,_/  /_/ /_/ /_/ /_/   /_/ /_/  \__, /  
#          /_/                                                                                                   /____/   
```

## Exploring Data Streaming

`Spark-Streaming.ipynb`

## Spark Submit

```bash
./bin/spark-submit \
    --master spark://172.25.0.101:7077 \
    /home/jovyan/spark/Italy-Migr-Streaming.py \
    1000

```

## Generate Fake Data

`InsertStreamingFakeData.sh`