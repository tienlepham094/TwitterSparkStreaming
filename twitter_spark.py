import findspark
findspark.init("D:\Spark\spark-3.1.3-bin-hadoop3.2")
import pyspark
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql.types import StringType, StructType, StructField, ArrayType
from pyspark.sql.functions import udf, from_json, col

KAFKA_TOPIC_NAME = "twitter_streaming"
KAFKA_BOOTSTRAP_SERVERS = '127.0.0.1:9092'
spark = SparkSession.builder\
        .appName("Kafka Pyspark Streamin Learning")\
            .master("local[*]").getOrCreate()

# Construct a streaming DataFrame that reads from testtopic
# Subscribe to 1 topic
df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
  .option("subscribe", KAFKA_TOPIC_NAME) \
  .option("startingOffsets", "latest") \
  .option("startingOffsets", "latest") \
  .option("header", "true") \
  .load() 

def aggregate_df(processed_df:DataFrame, config:dict) -> None:
    '''Aggregates data from the last hour and send it to mongoDB'''   
    
    agg_sentiment = processed_df.groupBy("topic") \
      .agg(F.avg(F.when(F.col("sentiment").eqNullSafe("positive"), 1) \
          .otherwise(0)).alias('positivity'), 
          F.count(F.col('topic')).alias('counts')) \
      .withColumn('created_at', F.current_timestamp()) \
      .select(F.col('topic').alias('topic_agg'), 
              F.round('positivity', 2).alias('positivity_rate'), 
              'counts', 'created_at')
    
    agg_emotion = processed_df.groupby('topic', 'emotion') \
        .agg(F.count(F.col('topic')).alias('counts')) \
        .groupby('topic').pivot('emotion').sum('counts').na.fill(0)
    
    inner_join = agg_sentiment.join(agg_emotion, 
        agg_sentiment.topic_agg == agg_emotion.topic) \
        .select('*')
    
    inner_join.write.format("mongo").mode("append").option("uri", config.get("mongoDB")).save()


