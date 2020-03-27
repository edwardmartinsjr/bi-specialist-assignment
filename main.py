import os
import argparse
from argparse import RawTextHelpFormatter
import tweepy
from streamlistener import Streamlistener
import util

# Twitter 
consumer_key = os.environ['CONSUMER_KEY']
consumer_secret = os.environ['CONSUMER_SECRET']
access_token = os.environ['ACCESS_TOKEN']
access_token_secret = os.environ['ACCESS_TOKEN_SECRET']

# Set location
# Bounding boxes for geolocations
# Online-Tool to create boxes (c+p as raw CSV): http://boundingbox.klokantech.com/
GEOBOX_NETHERLANDS = [3.0761845666, 51.0227615064, 7.288878522, 53.9033167283]

# Set log
util.set_logger(True)

def parse_args():
	parser = argparse.ArgumentParser(description='BI Specialist Assignment',formatter_class=RawTextHelpFormatter)	
	parser.add_argument('-t', '--track', type=str, help='python main.py --track word-to-track')

	return parser.parse_args()

if __name__== '__main__':
	args = parse_args()
	if args.track is None:
		util.logger.error(argparse.ArgumentTypeError('--track is required, eg: python main.py --track word-to-track'))
		os._exit(1)
	
	util.logger.info('Start Twitter Stream Listener')

	# Connects to
	# Authentification so we can access twitter
	auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
	auth.set_access_token(access_token, access_token_secret)
	api =tweepy.API(auth, wait_on_rate_limit=True)
	util.logger.info('OAuth authentication')

	util.logger.info('Listening twitter streaming')
	# Create instance of Streamlistener
	listener = Streamlistener(api = api)
	stream = tweepy.Stream(auth, listener = listener)

	# Choose what we want to filter by
	track = [args.track]
	util.logger.info('Tracking: %s', track)
	stream.filter(track = track, languages = ['en', 'nl'], locations = GEOBOX_NETHERLANDS)
