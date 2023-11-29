import yfinance as yf
from apify_client import ApifyClient
import pandas as pd

def get_stocksData(tickers_name):
    tickers_stock_merged= pd.DataFrame()

    for index,stock in enumerate(tickers_name):
        ticker= yf.Ticker(stock)
        ticker_stock = ticker.history(period='1mo')
        ticker_stock['day']= ticker_stock.index.day
        ticker_stock['month']= ticker_stock.index.month
        ticker_stock['year']= ticker_stock.index.year
        ticker_stock['dateTime']= pd.to_datetime(ticker_stock[['year', 'month', 'day']])
        ticker_stock= ticker_stock.set_index(keys='dateTime')
        ticker_stock= ticker_stock[['Close']]
        ticker_stock.columns= [stock]
        ticker_stock= pd.DataFrame(ticker_stock)    

        if index == 0:
            tickers_stock_merged= pd.DataFrame(ticker_stock)
        else:
            tickers_stock_merged= pd.merge(tickers_stock_merged, ticker_stock, left_index= True, right_index= True)
                    
    
    return tickers_stock_merged