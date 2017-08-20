import config
import time
import json

import tweepy
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream

from pymongo import MongoClient

MONGO_HOST = 'mongodb://localhost/twitterdb'
FILTERS = ['news']

class StreamListener(StreamListener):
    
    def on_connect(self):
        print('You are now connected to the streaming API')

    def on_error(self, status_code):
        print('An error has occured: {}'.format(repr(status_code)))
        return False

    def on_data(self, data):
        try:
            client = MongoClient(MONGO_HOST)
            db = client.twitterdb

            datajson = json.loads(data)
            createdat = datajson['created_at']
            
            print('Tweet collected at: {}'.format(createdat))
            
            db.twitter_search.insert(datajson)
        except Exception, e:
            print e


auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_secret)

listener = StreamListener(api=tweepy.API(wait_on_rate_limit=True))
streamer = Stream(auth=auth, listener=listener)

print('Tracking: {}'.format(str(WORDS)))
streamer.filter(track=WORDS)


