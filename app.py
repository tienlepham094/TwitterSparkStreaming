from flask import Flask 
import os
from dotenv import load_dotenv
from flask import render_template
from pymongo import MongoClient 
import pandas as pd

load_dotenv()
app = Flask(__name__)



@app.route("/")
def home():
    return render_template('index.html')

@app.errorhandler(404)
def page_not_found(error):
    return render_template('page_not_found.html'), 404
if __name__ == '__main__':
   app.run(host='localhost', port=5000, debug=True)

def get_df(database, collection):
    db = client[database]
    collection = df[collection]
    items = list(collection)
    return pd.DataFrame(items)
def process_data(df):
    columns = df['tag'].unique()
    