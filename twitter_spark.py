import findspark
findspark.init()
import pyspark
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql.types import StructType,TimestampType, DoubleType, StringType, StructField
from dotenv import load_dotenv
import os
load_dotenv()

mongo_db = os.getenv("mongoDB")
KAFKA_TOPIC_NAME= 'twitter_streaming'
KAFKA_BOOTSTRAP_SERVERS = '127.0.0.1:9092'

packages = ','.join(
        [
            'org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.3',
            'org.mongodb.spark:mongo-spark-connector_2.12:3.1.3'
        ]
    )
spark = SparkSession.\
        builder.\
        appName("TwitterStreaming").\
        config('spark.jars.packages', packages) \
         .config("spark.driver.memory","8G")\
        .config("spark.driver.maxResultSize", "0") \
        .config("spark.mongodb.input.uri", mongo_db) \
        .config("spark.mongodb.output.uri", mongo_db) \
        .getOrCreate()

schema = StructType() \
        .add("data", StructType() \
            .add("created_at", StringType())
            .add("id", StringType())
            .add("text", StringType()))
df = spark \
  .readStream \
  .format("kafka") \
  .option("startingOffsets", "latest") \
  .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
  .option("subscribe", KAFKA_TOPIC_NAME) \
  .load() \
  .select(F.col('key').cast('string'),
            F.from_json(F.col("value").cast("string"), 
                schema)['data']['created_at'].alias('created_at'),
            F.from_json(F.col("value").cast("string"), 
                schema)['data']['text'].alias('text')) 





# schemaKafka = StructType([ \
#     StructField("data",StringType(),True),\
#     StructField("matching_rules", StringType(), True)])



# stockDF=stockDF.select(from_json(col('value'),schemaKafka).alias("json_data")).selectExpr('json_data.*')


# query= df.writeStream \
#     .format("mongodb") \
#     .option('spark.mongodb.connection.uri', mongo_db) \
#     .option('spark.mongodb.database', 'twitter') \
#     .option('spark.mongodb.collection', 'twitter_streaming') \
#     .option("checkpointLocation", "/text") \
#     .trigger(processingTime="1 seconds") \
#     .outputMode("append") \
#     .start()
query= df.writeStream \
    .format("console") \
    .option("checkpointLocation", "/text") \
    .outputMode("append") \
    .trigger(processingTime="10 seconds") \
    .start()
# query.awaitTermination()

# query.start()