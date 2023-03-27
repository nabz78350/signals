import pandas as pd
import numpy as np
import datetime
import tqdm
from data_master import DataMaster
import os
from tqdm import tqdm
from scipy.stats import norm

master = DataMaster()



def presence_matrix(df):
    # create a list of dates starting from the earliest start date until today
    start_date = min(df['StartDate'])
    end_date = pd.Timestamp.today().date()
    date_range = pd.date_range(start=start_date, end=end_date, freq='D')

    # create a dictionary to hold the values for each column

    # create a matrix of False values with the same shape as the date range
    matrix = np.zeros((len(date_range), len(df)), dtype=bool)

    # iterate through the rows in the dataframe and populate the matrix
    for i, (_, row) in enumerate(df.iterrows()):
        # get the start and end indices for the row's values in the matrix
        if row['IsActiveNow'] == 1:
            start_index = date_range.get_loc(row['StartDate'])
            end_index = len(date_range) - 1
        else:
            start_index = date_range.get_loc(row['StartDate'])
            end_index = date_range.get_loc(row['EndDate'])
        # set the corresponding values in the matrix to True
        matrix[start_index:end_index+1, i] = True

    # create a dataframe using the matrix and the date range as the index
    new_df = pd.DataFrame(matrix, index=date_range, columns=df['Code'])
    return new_df

def data_from_dict(dico:dict):

    data = pd.DataFrame()
    for key, value in tqdm(dico.items()):
        try :
            df = value
            df['Ticker'] = key
            data = pd.concat([df, data], 0)
        except :
            ''
    return data.set_index(['datetime','Ticker'])


    
def aggregate_tickers_classifs(tickers):
    classif = {}
    for ticker in tqdm(tickers) :
        try :
            classif_ticker = master.equities.get_ticker_classification(ticker,'US')
            classif[ticker] = classif_ticker
        except :
            classif = classif
    return pd.DataFrame(classif).T


def aggregate_tickers_balance_sheet(tickers):
    balance_sheet = {}
    for ticker in tqdm(tickers) :
        try :
            balance_sheet_ticker = master.equities.get_ticker_balance_sheet(ticker,'US','q')
            balance_sheet[ticker] = balance_sheet_ticker
        except :
            balance_sheet = balance_sheet
    balance_sheet = pd.concat(balance_sheet)
    balance_sheet.index.names =['Ticker','Date']
    balance_sheet = balance_sheet .reorder_levels(['Date','Ticker'])

    return balance_sheet


def aggregate_market_data(tickers:list,period_start :datetime.date):
    mkt_data = {}
    for ticker in tqdm(tickers) :
        try :
            mkt_data_ticker = master.equities.get_ohlcv(ticker,'US',period_start = period_start)
            mkt_cap =master.equities.get_ticker_historical_mcap('AAPL','US')
            mkt_data_ticker.index = mkt_data_ticker['datetime']
            mkt_data_ticker.index.names =['Date']
            mkt_cap.index.names =['Date']
            mkt_cap.columns =['MktCap']
            mkt_cap.index = pd.to_datetime(mkt_cap.index)
            mkt_data_ticker = mkt_data_ticker.join(mkt_cap,how ='left')
            mkt_data[ticker]= mkt_data_ticker

        except :
            mkt_data = mkt_data
            
    mkt_data = pd.concat(mkt_data)
    mkt_data.index.names =['Ticker','Date']
    mkt_data = mkt_data .reorder_levels(['Date','Ticker'])

    return mkt_data

    


def create_rank_column(df:pd.DataFrame,column :str,pct=True, ascending=True,level=0,normalize = False):
    column_rank = column+'_rank'
    if normalize:
        df[column_rank] = df.groupby(level=level)[column].rank(pct=pct,ascending=ascending).clip(0.01,0.99).apply(norm.ppf)
    else :
        df[column_rank] = df.groupby(level=level)[column].rank(pct=pct,ascending=ascending).clip(0.01,0.99)

    return df



def write_to_parquet(df:pd.DataFrame,directory:str,name :str):
    path_directory = 'data/'+directory
    path = path_directory +'/'+name +'.pq'
    # Check whether the specified path exists or not
    isExist = os.path.exists(path_directory)
    if not isExist:
        # Create a new directory because it does not exist
        os.makedirs(path_directory)

    if os.path.exists(path):
        os.remove(path)
    
    df.to_parquet(path)


    

def average_directional_index(df:pd.DataFrame,lookback,smooth =True):
    plus_dm = df['high'].groupby(level=1).diff()
    minus_dm = df['high'].groupby(level=1).diff()
    plus_dm[plus_dm < 0] = 0
    minus_dm[minus_dm > 0] = 0

    tr1 = pd.DataFrame(df['high'] - df['low'])
    tr2 = pd.DataFrame(abs(df['high'] - df['close'].groupby(level=1).shift(1)))
    tr3 = pd.DataFrame(abs(df['low'] - df['close'].groupby(level=1).shift(1)))
    frames = [tr1, tr2, tr3]
    tr = pd.concat(frames, axis = 1, join = 'inner').max(axis = 1)
    atr = tr.groupby(level=1).rolling(lookback).mean().droplevel(0)

    plus_di = 100 * (plus_dm.groupby(level=1).ewm(alpha = 1/lookback).mean().droplevel(0) / atr)
    minus_di = abs(100 * (minus_dm.groupby(level=1).ewm(alpha = 1/lookback).mean().droplevel(0) / atr))
    dx = (abs(plus_di - minus_di) / abs(plus_di + minus_di)) * 100
    adx = ((dx.groupby(level=1).shift(1) * (lookback - 1)) + dx) / lookback
    adx_smooth = adx.groupby(level=1).ewm(alpha = 1/lookback).mean().droplevel(0)
    
    if smooth:
        return pd.DataFrame(adx_smooth,columns =['ADX'])
    else :
        return pd.DataFrame(adx,columns =['ADX'])
    

def mvwap(df:pd.DataFrame,lookback:int):
    tp = df[['close','high','low']].mean(1)
    tpv = tp * df['volume']
    tpv_cum = tpv.groupby(level=1).rolling(lookback).sum().droplevel(0)
    mvwap = tpv_cum / df['volume'].groupby(level=1).rolling(lookback).sum().droplevel(0)
    return pd.DataFrame(mvwap,columns =['MVWAP'])

def rsi(df:pd.DataFrame,lookback:int):
    ret = df['close'].groupby(level=1).pct_change()
    up = ret
    up[up<0]= 0

    lookback=5
    up = up.groupby(level=1).ewm(alpha = 1/lookback).mean().droplevel(0).reindex(ret.index).ffill()

    down = ret
    down[down>0]=0
    lookback=5
    down = down.abs().groupby(level=1).ewm(alpha = 1/lookback).mean().droplevel(0).reindex(ret.index).ffill()
    rs = up/ down
    rsi = 100 - (100/(1+rs))
    rsi = pd.DataFrame(rsi)
    rsi.columns =['RSI']
    return rsi

def rank_ts(df:pd.DataFrame,col:str,lookback:int,pct=True,ascending=True,normalize=False):
    column = df[col].unstack()
    if normalize :
        column_rank =column.rolling(lookback).rank(axis=0,pct=pct,ascending=ascending).clip(0.01,0.99).apply(norm.ppf)
    else :
        column_rank = column.rolling(lookback).rank(axis=0,pct=pct,ascending=ascending).clip(0.01,0.99)
    return pd.DataFrame(column_rank.stack(),columns =[col+'_rank'])

def extract_gics(universe:str):
    path = 'data/'+universe+'/GICS.pq'
    gics = pd.read_parquet(path)
    gics = gics.fillna('other')
    return gics


def extract_mkt_data(universe:str):
    path = 'data/'+universe+'/mkt_data.pq'
    mkt_data = pd.read_parquet(path)
    mkt_data['ret'] = mkt_data['close'].groupby(level=0).pct_change()
    return mkt_data

def center(x):
    mean = x.mean(1)
    x = x.sub(mean,0)
    return x