import os
from dotenv import load_dotenv
load_dotenv()
mongodb = os.getenv("mongoDB")

def write_to_mongo(df, epoch_id):
     df.write.format("mongo").mode("append").option("uri",mongodb).save()
     pass