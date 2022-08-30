import findspark
findspark.init('E:\spark-3.0.2-bin-hadoop2.7')
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType
from dotenv import load_dotenv
from pyspark.sql import functions as F
import os
load_dotenv()
# os.environ['PYSPARK_SUBMIT_ARGS'] = "org.apache.spark:spark-streaming-kafka-0-10_2.12:3.0.2"

mongo_db = os.getenv("mongoDB")
# print(mongo_db)
KAFKA_TOPIC_NAME= 'twitter_streaming'
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'


spark = SparkSession.\
        builder.\
        master("local")\
        .config("spark.mongodb.read.uri","mongodb://localhost:27017/twitter.twitter_streaming") \
        .config("spark.mongodb.write.uri","mongodb://localhost:27017/twitter.twitter_streaming") \
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








# df = spark \
#         .readStream \
#         .format("kafka") \
#         .option("kafka.bootstrap.servers", "localhost:9092") \
#         .option("subscribe", "twitter_streaming") \
#         .option("startingOffsets", "earliest") \
#         .load()
# df.show()
spark.sparkContext.setLogLevel("INFO")
query = df.writeStream \
        .queryName("all_tweets") \
        .outputMode("append").format("console") \
        .start().awaitTermination()
# query= df.writeStream \
#     .format('mongodb') \
#     .option('spark.mongodb.connection.uri', mongo_db) \
#     .option('spark.mongodb.database', 'twitter') \
#     .option('spark.mongodb.collection', 'twitter_streaming') \
#     .outputMode("append") \
#     .start()
print("PySpark Structured Streaming with Kafka Application Completed")

