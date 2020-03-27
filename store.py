from sqlalchemy import create_engine
import pandas as pd
import os
import util

# Postgres
db_user = os.environ['POSTGRES_USER']
db_password = os.environ['POSTGRES_PASSWORD']
db_host = os.environ['POSTGRES_HOST']
db_port = os.environ['POSTGRES_PORT']
db_name = os.environ['POSTGRES_NAME']

# Set Postgres
engine = create_engine('postgresql://%s:%s@%s:%s/%s' % (db_user, db_password, db_host, db_port, db_name))

def save_data(df):
    try:
        # Stores the data in a Database (append mode)
        df.to_sql(con=engine, name='twitter_stream', if_exists='append', index=False)
    except BaseException as e:
        util.logger.error(e)
