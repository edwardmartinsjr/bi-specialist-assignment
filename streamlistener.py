import tweepy
from dateutil import parser
import time
import uuid
import json
import util

# Tweepy class to access Twitter API
class Streamlistener(tweepy.StreamListener):
    # Set dataset
    dataset = []
    
    def on_connect(self):
        # Get streamming for 60 seconds and exit
        self.start_time = time.time()
        self.limit = 60

        util.logger.info('You are connected to the Twitter API')

    def on_error(self, status):
        if status != 200:
            util.logger.error('error found: {}'.format(status) )
            # Returning false disconnects the stream
            return False

    # Reads in tweet data as Json and extracts the data we want.
    def on_data(self,data):	        
        if (time.time() - self.start_time) < self.limit:
            try:
                raw_data = json.loads(data)

                if 'text' in raw_data:
                    username = raw_data['user']['screen_name']
                    created_at = parser.parse(raw_data['created_at'])
                    tweet = raw_data['text']
                    retweets = raw_data['retweet_count']
                    location = raw_data['user']['location']
                    
                    if raw_data['place'] is not None:
                        place = raw_data['place']['country']
                        util.logger.info(place)
                    else:
                        place = None
                        
                    util.logger.info('USERNAME: %s \n TWEET: %s \n RETWEET: %d \n LOCATION: %s',username, tweet, retweets, location)
                    util.logger.info(' Tweet colleted at: {} \n'.format(str(created_at)))
                    
                    # appends data in a structured format
                    self.dataset.append({
                        'key':uuid.uuid4(),
                        'username':username,
                        'tweet':tweet,
                        'retweets':retweets,
                        'location':location,
                        'created_at':str(created_at)})
            
            except BaseException as e:
                util.logger.error(e)
        else:
            return False
