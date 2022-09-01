from flask import Flask, jsonify
import os
import json
import pymongo
from dotenv import load_dotenv
from flask import render_template
from pymongo import MongoClient 
import pandas as pd

load_dotenv()
mongodb = os.getenv("mongoDB")
client = MongoClient(mongodb)

app = Flask(__name__)



@app.route("/")
def home():
    return render_template('index.html')

@app.errorhandler(404)
def page_not_found(error):
    return render_template('page_not_found.html'), 404


def get_df(database, collection):
    db = client[database]
    collection = db[collection].find()
    data = list(collection)
    return pd.DataFrame(data)

def process_data(df):
    meta_count = df.tag.value_counts()['Meta'] 
    aws_count = df.tag.value_counts()['Amazon']
    gg_count = df.tag.value_counts()['Google']
    apple_count = df.tag.value_counts()['Apple']
    netflix_count = df.tag.value_counts()['Netflix']
    return meta_count, aws_count, gg_count, apple_count, netflix_count



@app.route('/refreshData', methods=['GET'])
def refresh_graph_data():
    df = get_df("twitter", "twitter_streaming")
    meta_count, aws_count, gg_count, apple_count, netflix_count = process_data(df)
    return jsonify({"meta":str(meta_count), "amz": str(aws_count), "gg": str(gg_count),"apple": str(apple_count), "netflix": str(netflix_count)})
if __name__ == '__main__':
   app.run(host='localhost', port=5000, debug=True)