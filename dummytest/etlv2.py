import yfinance as yf
from apify_client import ApifyClient
import pandas as pd
from sklearn.preprocessing import MinMaxScaler


# Fetch Stock Data
def get_stocksData(tickers_name, start_date= None):
    global df_finalStock
    df_finalStock= pd.DataFrame()

    for index,stock in enumerate(tickers_name):
        ticker= yf.Ticker(stock)
        ticker_stock = ticker.history(period='1mo', start= pd.to_datetime(start_date))
        ticker_stock['day']= ticker_stock.index.day
        ticker_stock['month']= ticker_stock.index.month
        ticker_stock['year']= ticker_stock.index.year
        ticker_stock['dateTime']= pd.to_datetime(ticker_stock[['year', 'month', 'day']])
        ticker_stock= ticker_stock.set_index(keys='dateTime')
        ticker_stock= ticker_stock[['Close']]
        ticker_stock.columns= [stock]
        ticker_stock= pd.DataFrame(ticker_stock)    

        if index == 0:
            df_finalStock= pd.DataFrame(ticker_stock)
        else:
            df_finalStock= pd.merge(df_finalStock, ticker_stock, left_index= True, right_index= True)
                    
    
    
# Fetch Trends Data
def get_trendsData(keywords ,API_TOKEN= 'apify_api_EvEL4Oawoze0LxkvZ831rW4SQYunIv1Iku30'):    
    global df_finalTrends
    client = ApifyClient(API_TOKEN)
    df_finalTrends = pd.DataFrame()

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
        df_trend = df_trend[[trend]]

        if j == 0:
            df_finalTrends = df_trend
        else:
            df_finalTrends = pd.merge(df_finalTrends, df_trend, left_index=True, right_index=True)

# Transformation
def merging_Datasets(dataFrame1, dataFrame2):
    global merged_dataFrame
    merged_dataFrame= pd.merge(dataFrame1, dataFrame2, left_index=True, right_index= True)

    for col in merged_dataFrame.columns:
        merged_dataFrame[col]= merged_dataFrame[col].astype('float64')

    scaler = MinMaxScaler(feature_range= (0,100))
    merged_dataFrame[merged_dataFrame.columns]= scaler.fit_transform(merged_dataFrame[merged_dataFrame.columns])
    