import pandas as pd
import log

def transform(data):
    columns=['key', 'username', 'tweet', 'retweets', 'location', 'created_at']
    df = pd.DataFrame(data, columns = columns)    
    try:    
        # converting created_at to datetime
        df['created_at'] = pd.to_datetime(df['created_at'])
        df['created_at'] = df['created_at'].dt.strftime('%Y-%m-%d %H:%M:S')
        
        # converting number of retweets to numeric
        df['retweets'] = pd.to_numeric(df['retweets'])
    except BaseException as e:
        log.logger.error(e)

    return df
