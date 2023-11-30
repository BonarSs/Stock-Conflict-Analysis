from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from ETL_process.extract_stocks import get_stocksData
from ETL_process.extract_trends import get_trendsData
from ETL_process.load_trends import load_trends_to_database
from ETL_process.load_stocks import load_stocks_to_database
from ETL_process.transform_trends import merge_and_normalize

with DAG("stocks_trends_dag",
         start_date=datetime(2023, 1, 1),
         schedule_interval='@daily',
         catchup=False) as dag:

    get_stocks_task = PythonOperator(
        task_id="get_stocks_data",
        python_callable=get_stocksData,
        op_kwargs={"tickers_name": ['SBUX','MAPI.JK', 'MAPB.JK','FAST.JK','MCD']}
    )

    get_trends_task = PythonOperator(
        task_id="get_trends_data",
        python_callable=get_trendsData,
        op_kwargs={
            "API_TOKEN": "apify_api_EvEL4Oawoze0LxkvZ831rW4SQYunIv1Iku30",
            "keywords": ['palestina', 'israel', 'boikot', 'palestine', 'hamas', 'war', 'boycott', 'idf', 'genocide', 'genosida']
        }
    )

    merge_normalize_task = PythonOperator(
        task_id="merge_and_normalize",
        python_callable=merge_and_normalize,
    )

    load_trends = PythonOperator(
        task_id="trends_load",
        python_callable=load_trends_to_database,
    )
    
    load_stock = PythonOperator(
        task_id="stock_load",
        python_callable=load_stocks_to_database,
    )

    get_stocks_task >> load_stock
    get_trends_task >> merge_normalize_task >> load_trends

