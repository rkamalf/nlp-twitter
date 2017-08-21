import config
import time
import json

import tweepy
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream

from pymongo import MongoClient

FILTERS = ['news', 'trump']

class StreamListener(StreamListener):
    
    def on_connect(self):
        print('You are now connected to the streaming API')

    def on_error(self, status_code):
        print('An error has occured: {}'.format(repr(status_code)))
        return False

    def on_data(self, data):
        try:
            client = MongoClient('localhost', 27017)
            db = client.twitterdb

            datajson = json.loads(data)
            createdat = datajson['created_at']
            
            print('Tweet collected at: {}'.format(createdat))
            
            db.twitter_search.insert(datajson)
        except Exception as e:
            print(e)


auth = OAuthHandler(config.CONSUMER_KEY, config.CONSUMER_SECRET)
auth.set_access_token(config.ACCESS_TOKEN, config.ACCESS_TOKEN_SECRET)

listener = StreamListener(api=tweepy.API(wait_on_rate_limit=True))
streamer = Stream(auth=auth, listener=listener)

print('Tracking: {}'.format(str(FILTERS)))
streamer.filter(track=FILTERS)


