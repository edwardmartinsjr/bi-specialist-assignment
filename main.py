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


if __name__== '__main__':
	set_logger(True)
	
	LOG.info('Start Twitter Stream Listener')
