import os
from apify_client import ApifyClient
import pandas as pd

def get_trendsData(API_TOKEN, keywords):    
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
        df_trend = df_trend[[trend]]

        if j == 0:
            df_trends = df_trend
        else:
            df_trends = pd.merge(df_trends, df_trend, left_index=True, right_index=True)

    for col in df_trend.columns:
        df_trend[col] = df_trend[col].astype(int)

    save_path= os.path.dirname(os.path.abspath(__file__))
    csv_filename = os.path.join(save_path,'data', 'trends.csv')
    os.makedirs(os.path.join(save_path,'data'), exist_ok=True)
    
    if os.path.exists(csv_filename):
        df_trends.to_csv(csv_filename, mode='w')
    else:
        df_trends.to_csv(csv_filename)
