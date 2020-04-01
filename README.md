## BI Specialist Assignment - Streaming ETL Process:

This assignment consists of three parts:
- Connects to Twitter API
- Prepares the data in a structured format
- Stores the data in a Postgres DB


# Architecture

From Twitter API to Python to Postgres:
![Architecture](https://github.com/edwardmartinsjr/bi-specialist-assignment/blob/master/architecture.png)


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

## Run
Create a twitter app:
`https://iag.me/socialmedia/how-to-create-a-twitter-app-in-8-easy-steps/`

Docking postgres:
`https://hub.docker.com/_/postgres/`

Install application dependencies:
`pip install -r requirements.txt`

Ask for help:
`python main.py --help`

Running in a crontab, set time job, eg:
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
 30 * * * * /usr/bin/python /Users/username/Projects/bi-specialist-assignment/main.py --track word-to-track >> ~/cron.log 2>&1
```

Running at once:
```
export CONSUMER_KEY=XXX
export CONSUMER_SECRET=XXX
export ACCESS_TOKEN=XXX
export ACCESS_TOKEN_SECRET=XXX
export POSTGRES_USER=XXX
export POSTGRES_PASSWORD=XXX
export POSTGRES_HOST=XXX
export POSTGRES_PORT=XXX
export POSTGRES_NAME=XXX
```
`python main.py --track word-to-track`

Running at Apache Airflow:
![Architecture](https://github.com/edwardmartinsjr/bi-specialist-assignment/blob/master/architecture-airflow.png)

Add DAG `airflow-dag\twitter_stream.py` to DAGs folder `/Users/username/airflow/dags` 

![](graph-view.png?raw=true)
![](airflow-dag.png?raw=true)

## Python version:
Python 3.6.8 :: Anaconda, Inc.
- Problems with dyld: Library not loaded? Go to: https://github.com/kelaberetiv/TagUI/issues/86


