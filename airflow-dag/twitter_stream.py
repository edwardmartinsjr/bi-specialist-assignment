from __future__ import print_function

import airflow
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator

from pprint import pprint
from datetime import datetime, timedelta
from sqlalchemy import create_engine
import pandas as pd
import tweepy
from dateutil import parser
import time
import uuid
import json

# Twitter 
consumer_key = 'CONSUMER_KEY'
consumer_secret = 'CONSUMER_SECRET'
access_token = 'ACCESS_TOKEN'
access_token_secret = 'ACCESS_TOKEN_SECRET'

# Set location
# Bounding boxes for geolocations
# Online-Tool to create boxes (c+p as raw CSV): http://boundingbox.klokantech.com/
GEOBOX_NETHERLANDS = [3.0761845666, 51.0227615064, 7.288878522, 53.9033167283]

# Postgres
db_user = 'POSTGRES_USER'
db_password = 'POSTGRES_PASSWORD'
db_host = 'POSTGRES_HOST'
db_port = 'POSTGRES_PORT'
db_name = 'POSTGRES_NAME'

# Set Postgres
engine = create_engine('postgresql://%s:%s@%s:%s/%s' % (db_user, db_password, db_host, db_port, db_name))

args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(1),
}

dag = DAG(
    dag_id='twitter_stream',
    default_args=args,
    schedule_interval=timedelta(minutes=2),
)

# Tweepy class to access Twitter API
class Streamlistener(tweepy.StreamListener):
    # Set dataset
    dataset = []
    
    def on_connect(self):
        # Get streamming for 60 seconds and exit
        self.start_time = time.time()
        self.limit = 60

        print('You are connected to the Twitter API')

    def on_error(self, status):
        if status != 200:
            print('error found: {}'.format(status) )
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
                                                                
                    # appends data in a structured format
                    self.dataset.append({
                        'key':uuid.uuid4(),
                        'username':username,
                        'tweet':tweet,
                        'retweets':retweets,
                        'location':location,
                        'created_at':str(created_at)})
            
            except BaseException as e:
                print(e)
        else:
            return False

def read_twitter_streaming(ds, **kwargs):    
    # Connects to
    # Authentification so we can access twitter
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = tweepy.API(auth, wait_on_rate_limit=True)
    print('OAuth authentication')
    
    print('Listening twitter streaming')
    # Create instance of Streamlistener
    listener = Streamlistener(api = api)
    stream = tweepy.Stream(auth, listener = listener)
    
    # Choose what we want to filter by
    track = ['all-inclusive']
    print('Tracking: %s', track)
    stream.filter(track = track, languages = ['en', 'nl'], locations = GEOBOX_NETHERLANDS)
    
    print('Finish Twitter Stream Listener')
    
    return listener.dataset	

def transform(ds, **kwargs):
    columns=['key', 'username', 'tweet', 'retweets', 'location', 'created_at']
    dataset = kwargs['task_instance'].xcom_pull(task_ids='read_twitter_streaming')
    
    df = pd.DataFrame(dataset, columns = columns)    

    print('Transform Twitter Stream')
    try:    
        # converting created_at to datetime
        df['created_at'] = pd.to_datetime(df['created_at'])
        df['created_at'] = df['created_at'].dt.strftime('%Y-%m-%d %H:%M:S')
        
        # converting number of retweets to numeric
        df['retweets'] = pd.to_numeric(df['retweets'])
    except BaseException as e:
        print(e)
    return df

def load_db(ds, **kwargs):
    df = kwargs['task_instance'].xcom_pull(task_ids='transform')

    print('Store Twitter Stream')
    try:
        # Stores the data in a Database (append mode)
        df.to_sql(con=engine, name='twitter_stream', if_exists='append', index=False)
    except BaseException as e:
        print(e)       

read_twitter_streaming = PythonOperator(
    task_id='read_twitter_streaming',
    provide_context=True,
    python_callable=read_twitter_streaming,
    dag=dag,
)

transform = PythonOperator(
    task_id='transform',
    provide_context=True,
    python_callable=transform,
    dag=dag,
)

load_db = PythonOperator(
    task_id='load_db',
    provide_context=True,
    python_callable=load_db,
    dag=dag,
)

bashtask = BashOperator(
    task_id='print_date',
    bash_command='date',
    dag=dag,
)

read_twitter_streaming.set_downstream(transform)
transform.set_downstream(load_db)
load_db.set_downstream(bashtask)
