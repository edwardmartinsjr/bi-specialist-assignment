## Zoover - BI Specialist Assignment - Case 2: ETL Process

This assignment consists of three parts:
- Connects to Twitter API
- Prepares the data in a structured format
- Stores the data in a Postgres DB


# Architecture

From Twitter API to Pub/Sub to Dataflow to BigQuery


## Configuration

There are some env variables you must set to do the Twitter streaming configs:

NAME                      | DESCRIPTION
--------------------------|------------
CONSUMER_KEY            | Twitter consumer key
CONSUMER_SECRET         | Twitter consumer secret
ACCESS_TOKEN            | Twitter access Token
ACCESS_TOKEN_SECRET     | Twitter access Token Secret
POSTGRES_USER           | Postgres user
POSTGRES_PASSWORD       | Postgres password
POSTGRES_HOST           | Postgres host
POSTGRES_PORT           | Postgres port
POSTGRES_NAME           | Postgres database name

Set hourly job in crontab, eg:
`sudo crontab -e -u username`
```
CONSUMER_KEY='XXX'
CONSUMER_SECRET='XXX'
ACCESS_TOKEN='XXX'
ACCESS_TOKEN_SECRET='XXX'
POSTGRES_USER='XXX'
POSTGRES_PASSWORD='XXX'
POSTGRES_HOST='XXX'
POSTGRES_PORT='XXX'
POSTGRES_NAME='XXX'
 30 * * * * /usr/bin/python /Users/username/Projects/Zoover/main.py >> ~/cron.log 2>&1
 ```

 ## Python version:
 Python 3.6.8 :: Anaconda, Inc.


