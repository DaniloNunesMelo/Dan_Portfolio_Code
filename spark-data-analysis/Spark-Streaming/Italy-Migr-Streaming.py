import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .master("spark://172.25.0.101:7077") \
    .appName("Dan.Submit.Streaming") \
    .getOrCreate()

spark.conf.set("spark.sql.shuffle.partitions", "16")

staticMigItaly = spark.read.format("csv")\
.option("header", "true")\
.option("inferSchema", "true")\
.load("/home/jovyan/spark/Datasets/Mig_Italy_Streaming_data.csv")

staticMigItaly.createOrReplaceTempView("Mig_Italy_Streaming")
staticItalySchema = staticMigItaly.schema

print(staticItalySchema.names)

streamingMigItaly = spark.readStream\
.schema(staticItalySchema)\
.option("maxFilesPerTrigger", 1)\
.format("csv")\
.option("header", "true")\
.load("/home/jovyan/spark/Datasets/Streaming/*.csv")

sumMigraPerCountry = streamingMigItaly\
.groupBy("Country")\
.sum("Value")\
.select(F.col("Country").alias("Country"),F.col("sum(Value)").alias("QtdPeople"))

activityQuery = (
    sumMigraPerCountry.writeStream\
    .format("console")\
    .queryName("countries_people")\
    .outputMode("complete")\
    .start()
)

activityQuery.awaitTermination()