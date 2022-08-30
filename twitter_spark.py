import findspark
findspark.init('E:\spark-3.0.2-bin-hadoop2.7')
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType
from dotenv import load_dotenv
from pyspark.sql import functions as F
import os
load_dotenv()


KAFKA_TOPIC_NAME= 'twitter_streaming'
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'


spark = SparkSession.\
        builder.\
        master("local")\
        .appName("TwitterStreaming")\
        .getOrCreate()

schema = StructType() \
        .add("data", StructType() \
            .add("created_at", StringType())
            .add("id", StringType())
            .add("text", StringType()))
df = spark \
  .readStream \
  .format("kafka") \
  .option("startingOffsets", "earliest") \
  .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
  .option("subscribe", KAFKA_TOPIC_NAME) \
  .load() \
  .select(F.from_json(F.col("value").cast("string"), 
                schema)['data']['created_at'].alias('created_at'),
            F.from_json(F.col("value").cast("string"), 
                schema)['data']['text'].alias('text')) 

spark.sparkContext.setLogLevel("INFO")

query = df.select(F.to_json(F.struct(df.schema.names)).alias('value')) \
.writeStream \
        .outputMode("append").format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("topic", "twitter_consumer") \
        .option("checkpointLocation", "/tmp/vaquarkhan/checkpoint") \
        .start().awaitTermination()
print("PySpark Structured Streaming with Kafka Application Completed")

