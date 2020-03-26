import tweepy
import json
from dateutil import parser
import time
import os
import subprocess
import logging
from sqlalchemy import create_engine
import pandas as pd
import uuid
import argparse
from argparse import RawTextHelpFormatter

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

# Set Postgres
engine = create_engine('postgresql://%s:%s@%s:%s/%s' % (db_user, db_password, db_host, db_port, db_name))

def parse_args():
	parser = argparse.ArgumentParser(description='BI Specialist Assignment',formatter_class=RawTextHelpFormatter)	
	parser.add_argument('-t', '--track', type=str, help='python main.py --track word-to-track')

	return parser.parse_args()

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
		
		# Get streamming for 60 seconds and exit
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
					retweets = raw_data['retweet_count']
					location = raw_data['user']['location']

					if raw_data['place'] is not None:
						place = raw_data['place']['country']
						LOG.info(place)
					else:
						place = None
					
					LOG.info('\n USERNAME: %s \n TWEET: %s \n RETWEET: %d \n LOCATION: %s',username, tweet, retweets, location)
					LOG.info(' Tweet colleted at: {} \n'.format(str(created_at)))
					
					# Prepares the data in a structured format
					data = []
					data.append({
						'key':uuid.uuid4(),
						'username':username,
						'tweet':tweet,
						'retweets':retweets,
						'location':location,
						'created_at':str(created_at)})
					
					# Transformation
					# converting created_at to datetime 
					columns=['key', 'username', 'tweet', 'retweets', 'location', 'created_at']
					df = pd.DataFrame(data, columns = columns)
					df['created_at'] = pd.to_datetime(df['created_at'])
					df['created_at'] = df['created_at'].dt.strftime('%Y-%m-%d %H:%M:S')
					
					# converting number of retweets to numeric
					df['retweets'] = pd.to_numeric(df['retweets'])

					# Stores the data in a Database (append mode)
					df.to_sql(con=engine, name='twitter_stream', if_exists='append', index=False)

			except BaseException as e:
				LOG.error(e)
		else:
			return False

if __name__== '__main__':
	set_logger(True)

	args = parse_args()
	if args.track is None:
		LOG.error(argparse.ArgumentTypeError('--track is required, eg: python main.py --track word-to-track'))
		os._exit(1)
	
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
	track = [args.track]
	LOG.info('Tracking: %s', track)
	stream.filter(track = track, languages = ['en', 'nl'], locations = GEOBOX_NETHERLANDS)
