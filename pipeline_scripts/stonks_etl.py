import pandas as pd
import time
from git import Repo

from alpha_vantage.timeseries import TimeSeries
# from apscheduler.schedulers.blocking import BlockingScheduler

# sched = BlockingScheduler()

# @sched.scheduled_job('interval', minutes=60, timezone='America/Chicago')
# def scheduled_job():
av_api_key = 'JFWC5K8EAVN6SWO6'

def av_intraday(api_key: str
                , ticker: str
                , interval: str = '15min'
                , return_metadata = False
                ):

    """
    Pull intraday time series data by stock ticker name.
    Args:
        api_key: Str. Alpha Vantage API key.
        ticker: Str. Ticker name that we want to pull.
        interval: String. Desired data interval for the data. Can be '1min', '5min', '15min', '30min', '60min'.
        return_metadata: Boolean. If True return metadata along with DataFrame.
    Outputs:
        data: Dataframe. Time series data, including open, high, low, close, and datetime values.
        metadata: Dataframe. Metadata associated with the time series.   
    """

    #Generate Alpha Vantage time series object
    ts = TimeSeries(key = api_key, output_format = 'pandas')

    #Retrieve the data for the past sixty days (outputsize = full)
    data, meta_data = ts.get_intraday(ticker, outputsize = 'full', interval=interval)
    

    if(return_metadata==True):
        data['date_time'] = data.index
        return data, meta_data
    else:
        df = pd.DataFrame(data.reset_index())
        df.columns = ['datetime', 'Open', 'High', 'Low', 'Close', 'Volume']
        df['date'] = df['datetime'].astype('str').str.split(' ').str[0]
        return df

ticker_list = ['DIS', 'MSFT', 'WFC', 'V', 'AAPL', 'SQ', 'XLY', 'XLK', 'INTC', 'MRNA']

df_list = ticker_list

for a, b in enumerate(ticker_list):
    df_tmp = av_intraday(api_key=av_api_key, ticker = b, interval='1min')
    df_tmp['ticker'] = b

    df_list[a] = df_tmp
    time.sleep(60)

df = pd.concat(df_list)

df.to_parquet('stonks.parquet')

# repo = Repo('stonks')
# print(repo)