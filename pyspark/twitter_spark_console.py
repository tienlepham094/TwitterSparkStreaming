import findspark
findspark.init('E:\spark-3.0.2-bin-hadoop2.7')
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, StructField, ArrayType
from dotenv import load_dotenv
from pyspark.sql import functions as F
import os


load_dotenv()

# get env variables
KAFKA_TOPIC_NAME = os.getenv("KAFKA_TOPIC_NAME")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
mongodb = os.getenv("mongoDB")

spark = SparkSession. \
    builder. \
    master("local") \
    .appName("TwitterStreaming") \
    .config("spark.mongodb.input.uri", mongodb) \
    .config("spark.mongodb.output.uri", mongodb) \
    .getOrCreate()

# schema = StructType() \
#     .add("data", StructType() \
#          .add("created_at", StringType())
#          .add("id", StringType())
#          .add("text", StringType()))
schema = StructType([
    StructField('data', StructType([
       StructField('created_at', StringType(), True),
       StructField('id', StringType(), True),
       StructField('text', StringType(), True)
       ])),
       StructField('matching_rules', ArrayType(StructType(
        [StructField('id', StringType(), True),
       StructField('tag', StringType(), True),
        ]
       )), True),
    ])
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
                        schema)['data']['id'].alias('id'),
            F.from_json(F.col("value").cast("string"),
                        schema)['data']['text'].alias('text'),
            F.from_json(F.col("value").cast("string"),
                        schema)['matching_rules'][0]['tag'].alias('tag'))

spark.sparkContext.setLogLevel("INFO")
query = df.writeStream \
        .queryName("all_tweets") \
        .outputMode("append").format("console") \
        .start().awaitTermination()