import findspark
findspark.init('E:\spark-3.0.2-bin-hadoop2.7')
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType
from dotenv import load_dotenv
import os
load_dotenv()
# os.environ['PYSPARK_SUBMIT_ARGS'] = "org.apache.spark:spark-streaming-kafka-0-10_2.12:3.0.2"

mongo_db = os.getenv("mongoDB")
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
# df = spark \
#   .readStream \
#   .format("kafka") \
#   .option("startingOffsets", "latest") \
#   .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
#   .option("subscribe", KAFKA_TOPIC_NAME) \
#   .load() \
#   .select(F.col('key').cast('string'),
#             F.from_json(F.col("value").cast("string"), 
#                 schema)['data']['created_at'].alias('created_at'),
#             F.from_json(F.col("value").cast("string"), 
#                 schema)['data']['text'].alias('text')) 
# df = spark \
#   .readStream \
#   .format("kafka") \
#   .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
#   .option("subscribe", KAFKA_TOPIC_NAME) \
#   .load()


# df.printSchema()


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
#     .outputMode("append") \
#     .start()
if "__name__" == "__main__":

    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "twitter_streaming") \
        .option("") \
        .load("startingOffsets", "earliest")
    # spark.sparkContext.setLogLevel("INFO")
    query = df.write \
        .queryName("all_tweets") \
        .outputMode("complete").format("console") \
        .start().awaitTermination()
    print("PySpark Structured Streaming with Kafka Application Completed")

