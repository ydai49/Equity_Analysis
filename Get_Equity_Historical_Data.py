#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jul 15 18:23:05 2019

@author: Yu Dai
"""

import pandas as pd
import datetime 
import pandas_datareader.data as pdr
from yahoofinancials import YahooFinancials
from alpha_vantage.alpha_vantage.timeseries import TimeSeries

# Set default date value
end_date_local = datetime.date.today()
beg_date_local = datetime.date.today()-datetime.timedelta(1825)

# Web Scraping Method
def get_data_Web_Scrap(all_tickers,method="daily",beg_date=beg_date_local,end_date=end_date_local):
    """
    Data type:
    all_tickers :list
    method : string. 
    beg_date : datetime.date() object
    end_date : datetime.date() object
    """
    
    beg_date = beg_date.strftime('%Y-%m-%d')
    end_date = end_date.strftime('%Y-%m-%d')
    
    close_prices = pd.DataFrame()
    cp_tickers = all_tickers
    attempt = 0
    drop = []
    
    while len(cp_tickers) != 0 and attempt <=5:
        
        print("-----------------")
        print("attempt number ",attempt)
        print("-----------------")
        
        for i in range(len(cp_tickers)):
            
            try:
                
                yahoo_financials = YahooFinancials(cp_tickers[i])
                json_obj = yahoo_financials.get_historical_price_data(beg_date,end_date,method)
                ohlv = json_obj[cp_tickers[i]]['prices']
                temp = pd.DataFrame(ohlv)[["formatted_date","adjclose"]]
                temp.set_index("formatted_date",inplace=True)
                temp2 = temp[~temp.index.duplicated(keep='first')]
                close_prices[cp_tickers[i]] = temp2["adjclose"]
                drop.append(cp_tickers[i])       
            
            except:
                
                print(cp_tickers[i]," :failed to fetch data...retrying")
                continue
                
        cp_tickers = [j for j in cp_tickers if j not in drop]
        attempt+=1
        
    failed_tickers = cp_tickers
    
    return [close_prices,failed_tickers]

# API Method
def get_data_API(all_tickers,beg_date=beg_date_local,end_date=end_date_local):
    """
    Data type:
    all_tickers:list
    beg_date : datetime.date() object
    end_date : datetime.date() object
    """
    
    close_prices = pd.DataFrame() 
    cp_tickers = all_tickers
    # initializing passthrough variable
    attempt = 0 
    # initializing list to store tickers whose close price was successfully extracted
    drop = []
    while len(cp_tickers) != 0 and attempt <= 5:
        print("-----------------")
        print("attempt number ",attempt)
        print("-----------------")
        
        for i in range(len(cp_tickers)):
            
            try:
                
                temp = pdr.get_data_yahoo(cp_tickers[i],beg_date,end_date)
                close_prices[cp_tickers[i]] = temp["Adj Close"]
                drop.append(cp_tickers[i])  
                
            except:
                
                print(cp_tickers[i]," :failed to fetch data...retrying")
                continue
        # removing stocks whose data has been extracted from the ticker list
        cp_tickers = [j for j in cp_tickers if j not in drop] 
        attempt+=1
        
    failed_tickers = cp_tickers
    
    return [close_prices,failed_tickers]

# Get intraday historical equtiy data
API_KEY= "W8EWFQEAKAKALOBL"
def get_data_intraday(all_tickers,API_key=API_KEY,time_interval='1min'):
    """
    This function can retrieve at most 5 ticker at a time and should be called every 1 minutes.
    Data type:
    all_tickers:list
    time_interval: string. Accepted variable: 1min, 5min, 15min
    """
    
    #Standard API call frequency is 5 calls per minute and 500 calls per day
    close_prices = pd.DataFrame()
    cp_tickers = all_tickers
        
    attempt = 0
    drop = []
    time_count = 0
    
    while len(cp_tickers) != 0 and attempt <=5:
        print("-----------------")
        print("attempt number ",attempt)
        print("-----------------")
        
        ts = TimeSeries(key=API_key, output_format='pandas')
        
        for i in range(len(cp_tickers)):
            
            try:    
                data = ts.get_intraday(symbol=cp_tickers[i],interval=time_interval, outputsize='full')[0]
                data.columns = ["open","high","low","close","volume"]
                close_prices[cp_tickers[i]] = data["close"]
                drop.append(cp_tickers[i])  
                
            except:
                
                print(cp_tickers[i]," :failed to fetch data...retrying")
                continue
                
        cp_tickers = [j for j in cp_tickers if j not in drop]
        attempt+=1
        
    failed_tickers = cp_tickers
    
    return [close_prices,failed_tickers]

