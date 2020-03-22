import tweepy
import json
from dateutil import parser
import time
import os
import subprocess
import logging
from sqlalchemy import create_engine
import pandas as pd

# Twitter 
consumer_key = os.environ['CONSUMER_KEY']
consumer_secret = os.environ['CONSUMER_SECRET']
access_token = os.environ['ACCESS_TOKEN']
access_token_secret = os.environ['ACCESS_TOKEN_SECRET']

# Postgres
db_user = os.environ['POSTGRES_USER']
db_password = os.environ['POSTGRES_PASSWORD']
db_host = os.environ['POSTGRES_HOST']
db_port = os.environ['POSTGRES_PORT']
db_name = os.environ['POSTGRES_NAME']

# Set logger
LOG = logging.getLogger(__name__)

# Set location
# Bounding boxes for geolocations
# Online-Tool to create boxes (c+p as raw CSV): http://boundingbox.klokantech.com/
GEOBOX_NETHERLANDS = [3.0761845666, 51.0227615064, 7.288878522, 53.9033167283]

def get_dbh():
    # Connect to DB
    conn = 'postgresql://%s:%s@%s:%d/%s' % (db_user, db_password, db_host,
                                            db_port, db_name)
    return create_engine(conn, encoding='utf-8')

def set_logger(debug):
    fh = logging.StreamHandler()
    formatter = logging.Formatter('%(levelname)s %(message)s')
    fh.setFormatter(formatter)
    LOG.addHandler(fh)

    if debug is True:
        LOG.setLevel(logging.DEBUG)
    else:
        LOG.setLevel(logging.INFO)

# Tweepy class to access Twitter API
class Streamlistener(tweepy.StreamListener):
	def on_connect(self):
		self.start_time = time.time()
		self.limit = 60
		LOG.info('You are connected to the Twitter API')


	def on_error(self, status):
		if status != 200:
			LOG.error('error found: {}'.format(status) )
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
					retweet_count = raw_data['retweet_count']

					if raw_data['place'] is not None:
						place = raw_data['place']['country']
						LOG.info(place)
					else:
						place = None
					

					location = raw_data['user']['location']

					
					LOG.info('\n USERNAME: %s \n TWEET: %s \n RETWEET: %d \n PLACE: %s \n LOCATION: %s',username, tweet, retweet_count, place, location)
					LOG.info(' Tweet colleted at: {} \n'.format(str(created_at)))

			except BaseException as e:
				LOG.error(e)
		else:
			return False

if __name__== '__main__':
	set_logger(True)
	
	LOG.info('Start Twitter Stream Listener')

	# Connects to
	# Authentification so we can access twitter
	auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
	auth.set_access_token(access_token, access_token_secret)
	api =tweepy.API(auth, wait_on_rate_limit=True)
	LOG.info('OAuth authentication')

	LOG.info('Listening twitter streaming')
	# Create instance of Streamlistener
	listener = Streamlistener(api = api)
	stream = tweepy.Stream(auth, listener = listener)

	# Choose what we want to filter by
	track = ['all-inclusive']
	stream.filter(track = track, languages = ['en', 'nl'], locations = GEOBOX_NETHERLANDS)
