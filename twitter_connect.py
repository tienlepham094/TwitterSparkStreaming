import tweepy
from tweepy import StreamingClient
import json
import os
from dotenv import load_dotenv
load_dotenv()
from twitter_kafka_producer import send_message

# get key from .env file
api_key = os.getenv("API_KEY")
api_secret = os.getenv("API_KEY_SECRET")
bearer_token = os.getenv("BEARER_TOKEN")
access_token = os.getenv("ACCESS_TOKEN")
access_token_secret = os.getenv("ACCESS_TOKEN_SECRET")

# TWEEPY
# Defining functions to attach the necessary rules for the Twitter API streaming filter:

def check_rules(bearer_token:str, rules:list, tags:list) -> None:
    '''Checks whether there are rules already attached to the
    bearer token or not, if there are rules attached it will 
    delete all the rules, then it will add all the necessary 
    rules in both cases'''
    def add_rules(client:tweepy.StreamingClient, rules:list, tags:list) -> None:
        '''Adds rules to the streamer filter'''
        for rule, tag in zip(rules, tags):
            client.add_rules(tweepy.StreamRule(value=rule, tag=tag))

    client = tweepy.StreamingClient(bearer_token, wait_on_rate_limit=True)
    # print(client.get_rules())
    if client.get_rules()[3]['result_count'] != 0:
        n_rules = client.get_rules()[0]
        ids = [n_rules[i_tuple[0]][2] for i_tuple in enumerate(n_rules)]
        client.delete_rules(ids)
        add_rules(client, rules, tags)
    else:
        add_rules(client, rules, tags)


# Creating a Twitter stream listener class:

class Listener(tweepy.StreamingClient):
    def on_data(self, data):
        message = json.loads(data)
        send_message(message)

    def on_connect(self):
        print("Connecting...")
    
    def on_error(self, status):
        print(status)
    
    def on_connection_error(self):
        self.disconnect()
# Getting necessary variables from config.json:

with open('config.json', 'r') as config_file:
    config = json.load(config_file)

# Defining topics of our interest:

tags = ['Meta', 'Apple', 'Amazon', 'Netflix', 'Google']
query = config.get('query')
# -has:multimedia -is:retweet -has:link -is:quote -is:reply

rules = [f"{config.get('Meta')} {query}", 
    f"{config.get('Apple')} {query}", 
    f"{config.get('Amazon')} {query}", 
    f"{config.get('Netflix')} {query}", 
    f"{config.get('Google')} {query}"]

def main():
    # Setting up Tweepy filter configuration:
    check_rules(bearer_token, rules, tags)
    # Start and configure Kafka producer and topics:
    # configure_create_topics(topics=tags)
    # Start streaming:
    Listener(bearer_token).filter(tweet_fields=['created_at'])

if __name__ == '__main__':
     main()