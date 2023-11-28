import yfinance as yf
from apify_client import ApifyClient
import pandas as pd

stock_string = 'sbux mcd msft'
stock_list = ['SBUX', 'MCD', 'MSFT']
API_TOKEN = "apify_api_EvEL4Oawoze0LxkvZ831rW4SQYunIv1Iku30"
keywords = ['palestina', 'israel', 'boikot']

tickers = yf.Tickers(stock_string)
df_stocks = pd.DataFrame()

for i, stock in enumerate(stock_list):
  df_stock = tickers.tickers[stock].history(period='1mo')
  df_stock['day'] = df_stock.index.day
  df_stock['month'] = df_stock.index.month
  df_stock['year'] = df_stock.index.year
  df_stock['datetime'] = pd.to_datetime(df_stock[['year', 'month', 'day']])
  df_stock = df_stock.set_index('datetime')
  df_stock = df_stock[['Close']]
  df_stock.columns = [stock]
  df_stock = pd.DataFrame(df_stock)

  if i == 0:
    df_stocks = df_stock
  else:
    df_stocks = pd.merge(df_stocks, df_stock, left_index=True, right_index=True)

client = ApifyClient(API_TOKEN)
df_trends = pd.DataFrame()

for j, trend in enumerate(keywords):
    run_input = {
        "searchTerms": [trend],
        "isMultiple": False,
        "timeRange": "today 1-m",
        "geo": "",
        "viewedFrom": "id",
        "skipDebugScreen": False,
        "isPublic": False,
        "category": "",
        "maxItems": 0,
        "extendOutputFunction": """($) => {
        const result = {};

        return result;
    }""",
        "outputAsISODate": True,
        "csvOutput": False,
        "maxConcurrency": 10,
        "maxRequestRetries": 15,
        "pageLoadTimeoutSecs": 180,
    }

    run = client.actor("DyNQEYDj9awfGQf9A").call(run_input=run_input)

    items_list = []

    for item in client.dataset(run["defaultDatasetId"]).iterate_items():
        row = {}
        for key, value in item.items():
            row['Key'] = key
            row['Value'] = value
            items_list.append(row.copy())

    df_trend = pd.DataFrame(items_list)
    df_trend.columns = df_trend.iloc[0]
    df_trend = df_trend[1:]
    df_trend = df_trend.reset_index(drop=True)

    df_trend['Term / Date'] = pd.to_datetime(df_trend['Term / Date'])
    df_trend['day'] = df_trend['Term / Date'].dt.day
    df_trend['month'] = df_trend['Term / Date'].dt.month
    df_trend['year'] = df_trend['Term / Date'].dt.year
    df_trend['datetime'] = pd.to_datetime(df_trend[['year', 'month', 'day']])
    df_trend = df_trend.set_index('datetime')
    df_trend = df_trend[['palestina']]

    if i == 0:
        df_trends = df_trend
    else:
        df_trends = pd.merge(df_trends, df_trend, left_index=True, right_index=True)

df_final = pd.merge(df_stocks, df_trends, left_index=True, right_index=True)